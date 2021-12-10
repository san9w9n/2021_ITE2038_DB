#include "log.h"

int logFD;
int buff_pos;
FILE* logmsgFP = nullptr;
header_log_t* header_log;
char log_buffer[LOGBUFFSIZE];
pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
activetrans_t activetrans;
std::set<int> loser;

void open_recovery_table(int64_t table_id) {
  char path[10];
  sprintf(path, "DATA%ld", table_id);
  file_open_table_file(path);
}

int init_log(int flag, int log_num, char* log_path, char* logmsg_path) {
  if ((logFD = open(log_path, O_RDWR | O_CREAT, 0777)) < 0) return 1;
  if (!(logmsgFP = fopen(logmsg_path, "w+"))) return 1;

  buff_pos = 0;
  header_log = new header_log_t();
  if (!pread(logFD, header_log, HEADERLOG, 0)) {
    header_log->flushed_LSN = HEADERLOG;
    header_log->last_trx_id = 0;
    pwrite(logFD, header_log, HEADERLOG, 0);
    fsync(logFD);
  } else {
    analysis();
    if (flag == REDO_CRASH) {
      redo(log_num);
      return 0;
    }
    redo(-1);
    if (flag == UNDO_CRASH)
      undo(log_num);
    else {
      undo(-1);
      log_flush();
    }
  }
  return 0;
}

int last_trx_id() { return header_log->last_trx_id; }

void push_log_to_buffer(main_log_t* main_log, update_log_t* update_log,
                        char* old_img, char* new_img, LSN_t next_undo_LSN) {
  uint16_t valsize;

  if (buff_pos >= LOGTHRESHOLD) {
    pwrite(logFD, log_buffer, buff_pos, header_log->flushed_LSN);
    header_log->flushed_LSN += buff_pos;
    pwrite(logFD, header_log, HEADERLOG, 0);
    fsync(logFD);

    buff_pos = 0;
    memset(log_buffer, 0, LOGBUFFSIZE);
  }

  memcpy(log_buffer + buff_pos, main_log, MAINLOG);
  buff_pos += MAINLOG;
  if (main_log->type == UPDATE || main_log->type == COMPENSATE) {
    valsize = update_log->valsize;
    memcpy(log_buffer + buff_pos, update_log, UPDATELOG);
    buff_pos += UPDATELOG;
    memcpy(log_buffer + buff_pos, old_img, valsize);
    memcpy(log_buffer + buff_pos + valsize, old_img, valsize);
    buff_pos += 2 * valsize;
    if (main_log->type == COMPENSATE) {
      memcpy(log_buffer + buff_pos, &next_undo_LSN, 8);
      buff_pos += 8;
    }
  }

  delete[] old_img;
  delete[] new_img;
  delete update_log;
  delete main_log;

  UNLOCK(log_mutex);
}

main_log_t* make_main_log(int trx_id, int type, int log_size, LSN_t prev_LSN) {
  main_log_t* main_log;

  LOCK(log_mutex);

  main_log = new main_log_t();
  main_log->LSN = header_log->flushed_LSN + buff_pos;
  main_log->log_size = log_size;
  main_log->prev_LSN = prev_LSN;
  main_log->trx_id = trx_id;
  main_log->type = type;

  return main_log;
}

update_log_t* make_update_log(int64_t table_id, pagenum_t page_id,
                              uint16_t valsize, uint16_t offset) {
  update_log_t* update_log;

  update_log = new update_log_t();
  update_log->table_id = table_id;
  update_log->page_id = page_id;
  update_log->valsize = valsize;
  update_log->offset = offset;

  return update_log;
}

void log_flush() {
  LOCK(log_mutex);
  if (!buff_pos) {
    UNLOCK(log_mutex);
    return;
  }
  pwrite(logFD, log_buffer, buff_pos, header_log->flushed_LSN);
  header_log->flushed_LSN += buff_pos;
  pwrite(logFD, header_log, HEADERLOG, 0);
  fsync(logFD);

  buff_pos = 0;
  memset(log_buffer, 0, LOGBUFFSIZE);
  UNLOCK(log_mutex);
}

void make_active_trx(int trx_id, LSN_t first_LSN) {
  active_trx_t* active_trx;
  if (activetrans.find(trx_id) != activetrans.end()) return;
  active_trx = new active_trx_t();
  active_trx->begin_LSN = first_LSN;
  activetrans[trx_id] = active_trx;
}

void erase_active_trx(int trx_id) {
  active_trx_t* active_trx;

  if (activetrans.find(trx_id) == activetrans.end()) return;
  active_trx = activetrans[trx_id];
  activetrans.erase(trx_id);
  delete active_trx;
}

void push_update_stack(int trx_id, LSN_t LSN) {
  active_trx_t* active_trx;
  if (activetrans.find(trx_id) == activetrans.end()) return;
  active_trx = activetrans[trx_id];
  active_trx->update_LSN_stack.push(LSN);
}

void pop_update_stack(int trx_id) {
  active_trx_t* active_trx;
  if (activetrans.find(trx_id) == activetrans.end()) return;
  active_trx = activetrans[trx_id];
  if (!active_trx->update_LSN_stack.empty()) active_trx->update_LSN_stack.pop();
}

LSN_t top_update_stack(int trx_id) {
  active_trx_t* active_trx;
  LSN_t ret;
  if (activetrans.find(trx_id) == activetrans.end()) return 0;
  ret = 0;
  active_trx = activetrans[trx_id];
  if (!active_trx->update_LSN_stack.empty())
    ret = active_trx->update_LSN_stack.top();

  return ret;
}

void analysis() {
  LSN_t LSN;
  main_log_t* main_log;
  int max_winner_id = 0;
  int max_loser_id = 0;
  active_trx_t* active_trx;
  std::set<int> winner;

  fprintf(logmsgFP, "[ANALYSIS] Analysis pass start\n");

  main_log = new main_log_t();
  LSN = HEADERLOG;
  while (LSN < header_log->flushed_LSN) {
    if (pread(logFD, main_log, MAINLOG, LSN) != MAINLOG) break;
    switch (main_log->type) {
      case BEGIN:
        make_active_trx(main_log->trx_id, LSN);
        loser.insert(main_log->trx_id);
        activetrans[main_log->trx_id]->last_LSN = main_log->LSN;
        break;
      case COMMIT:
      case ROLLBACK:
        erase_active_trx(main_log->trx_id);
        loser.erase(main_log->trx_id);
        winner.insert(main_log->trx_id);
        break;
      default:
        activetrans[main_log->trx_id]->last_LSN = main_log->LSN;
        break;
    }
    LSN += main_log->log_size;
  }
  delete main_log;

  fprintf(logmsgFP, "[ANALYSIS] Analysis success. Winner:");
  for (auto it = winner.begin(); it != winner.end(); it++) {
    max_winner_id = (*it);
    fprintf(logmsgFP, " %d", (*it));
  }
  fprintf(logmsgFP, ", Loser:");
  for (auto it = loser.begin(); it != loser.end(); it++) {
    max_loser_id = (*it);
    fprintf(logmsgFP, " %d", (*it));
  }
  fprintf(logmsgFP, "\n");
  header_log->last_trx_id =
      ((max_winner_id > max_loser_id) ? max_winner_id : max_loser_id);
}

void redo(int log_num) {
  main_log_t* main_log;
  update_log_t* update_log;
  char* new_img;
  uint16_t valsize;
  uint16_t offset;
  int64_t table_id;
  pagenum_t page_id;
  int page_idx;
  int loop;
  LSN_t LSN;
  LSN_t next_undo_LSN;
  page_t* page;

  LSN = HEADERLOG;
  main_log = new main_log_t();
  update_log = new update_log_t();
  fprintf(logmsgFP, "[REDO] Redo pass start\n");
  loop = 0;
  while ((LSN < header_log->flushed_LSN) && (log_num == -1 || ++loop <= log_num)) {
    if (pread(logFD, main_log, MAINLOG, LSN) != MAINLOG) break;
    if (main_log->type == UPDATE || main_log->type == COMPENSATE) 
    {
      pread(logFD, update_log, UPDATELOG, LSN + MAINLOG);
      if (main_log->type == UPDATE)
        push_update_stack(main_log->trx_id, LSN);
      else {
        pread(logFD, &next_undo_LSN, 8, LSN + MAINLOG + UPDATELOG + 2 * update_log->valsize);
        while (top_update_stack(main_log->trx_id) > next_undo_LSN)
          pop_update_stack(main_log->trx_id);
      }

      valsize = update_log->valsize;
      offset = update_log->offset - 128;
      table_id = update_log->table_id;
      page_id = update_log->page_id;
      open_recovery_table(table_id);

      page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
      if (page->LSN < LSN) {
        new_img = new char[valsize + 2];
        pread(logFD, new_img, valsize, LSN + MAINLOG + UPDATELOG + valsize);

        for (int i = 0; i < valsize; i++)
          page->leafbody.value[i + offset] = new_img[i];
        page->LSN = LSN;

        if (main_log->type == UPDATE)
          fprintf(logmsgFP, "LSN %lu [UPDATE] Transaction id %d redo apply\n", LSN, main_log->trx_id);
        else
          fprintf(logmsgFP, "LSN %lu [CLR] next undo lsn %lu\n", LSN, next_undo_LSN);
        buffer_write_page(table_id, page_id, page_idx, 1);

        delete[] new_img;
        new_img = nullptr;
      } else {
        fprintf(logmsgFP, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", LSN, main_log->trx_id);
        buffer_write_page(table_id, page_id, page_idx, 0);
      }
    }

    else 
    {
      if (main_log->type == BEGIN)
        fprintf(logmsgFP, "LSN %lu [BEGIN] Transaction id %d\n", LSN,
                main_log->trx_id);
      else if (main_log->type == COMMIT)
        fprintf(logmsgFP, "LSN %lu [COMMIT] Transaction id %d\n", LSN,
                main_log->trx_id);
      else
        fprintf(logmsgFP, "LSN %lu [ROLLBACK] Transaction id %d\n", LSN,
                main_log->trx_id);
    }
    LSN += main_log->log_size;
  }
  if (log_num == -1) fprintf(logmsgFP, "[REDO] Redo pass end\n");
}

void undo(int log_num) {
  main_log_t* main_log;
  update_log_t* update_log;
  char* old_img;
  char* new_img;
  uint16_t size;
  uint16_t offset;
  int64_t table_id;
  pagenum_t page_id;
  int page_idx;
  int loop;
  int trx_id;
  LSN_t LSN;
  LSN_t next_undo_LSN;
  main_log_t* rollback_log;
  main_log_t* new_main_log;
  update_log_t* new_update_log;
  page_t* page;
  priority_table_t priority_table;
  fprintf(logmsgFP, "[UNDO] Undo pass start\n");

  for (int trx_id : loser) {
    LSN = top_update_stack(trx_id);
    if (!LSN) {
      rollback_log = make_main_log(trx_id, ROLLBACK, MAINLOG, activetrans[trx_id]->last_LSN);
      push_log_to_buffer(rollback_log, 0, 0, 0, 0);
      erase_active_trx(trx_id);
    } 
    else
      priority_table.push(top_update_stack(trx_id));
  }

  loop = 0;
  main_log = new main_log_t();
  update_log = new update_log_t();
  while (!priority_table.empty() && (log_num == -1 || ++loop <= log_num)) {
    LSN = priority_table.top();
    priority_table.pop();

    pread(logFD, main_log, MAINLOG, LSN);
    pread(logFD, update_log, UPDATELOG, LSN + MAINLOG);
    trx_id = main_log->trx_id;
    table_id = update_log->table_id;
    page_id = update_log->page_id;
    offset = update_log->offset - 128;
    size = update_log->valsize;

    new_img = new char[size + 2]();
    old_img = new char[size + 2]();
    pread(logFD, old_img, size, LSN + MAINLOG + UPDATELOG);
    pread(logFD, new_img, size, LSN + MAINLOG + UPDATELOG + size);

    pop_update_stack(trx_id);
    next_undo_LSN = top_update_stack(trx_id);
    open_recovery_table(table_id);
    page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
    if (page->LSN >= LSN) {
      new_main_log =
          make_main_log(trx_id, COMPENSATE, MAINLOG + UPDATELOG + 2 * size + 8,
                        activetrans[trx_id]->last_LSN);
      new_update_log = make_update_log(table_id, page_id, size, offset + 128);
      activetrans[trx_id]->last_LSN = new_main_log->LSN;
      push_log_to_buffer(new_main_log, new_update_log, new_img, old_img,
                         next_undo_LSN);

      for (int i = 0; i < size; i++)
        page->leafbody.value[i + offset] = old_img[i];
      fprintf(logmsgFP, "LSN %lu [UPDATE] Transaction id %d undo apply\n", LSN,
              trx_id);
      buffer_write_page(table_id, page_id, page_idx, 1);
    } else {
      fprintf(logmsgFP, "LSN %lu [CONSIDER-UNDO] Transaction id %d\n", LSN,
              trx_id);
      buffer_write_page(table_id, page_id, page_idx, 0);
      delete[] new_img;
      delete[] old_img;
    }

    if (!next_undo_LSN) {
      rollback_log = make_main_log(trx_id, ROLLBACK, MAINLOG,
                                   activetrans[trx_id]->last_LSN);
      push_log_to_buffer(rollback_log, 0, 0, 0, 0);
      erase_active_trx(trx_id);
    } else
      priority_table.push(next_undo_LSN);
  }

  if (log_num == -1) fprintf(logmsgFP, "[UNDO] Undo pass end\n");
  delete main_log;
  delete update_log;
}

int shutdown_log() {
  for (auto it = activetrans.begin(); it != activetrans.end(); it++) {
    delete it->second;
  }
  delete header_log;
  close(logFD);
  fclose(logmsgFP);
  return 0;
}