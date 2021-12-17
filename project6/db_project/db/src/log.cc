#include "log.h"

int logFD;
int buff_pos;
FILE* logmsgFP = nullptr;
header_log_t* header_log;
char log_buffer[LOGBUFFSIZE];
loser_trx_map_t loser_trx_map;
pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

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
  } 
  
  else if(header_log->flushed_LSN > HEADERLOG) {
    analysis();
    if (flag == REDO_CRASH) 
      redo(log_num);
    else if (flag == UNDO_CRASH) {
      redo(NO_CRASH);
      undo(log_num);
    }
    else {
      redo(NO_CRASH);
      undo(NO_CRASH);
    }
    log_flush();
    buffer_flush();
    file_close_table_files();

    if(flag == RECOVERY) {
      header_log->flushed_LSN = HEADERLOG;
      pwrite(logFD, header_log, HEADERLOG, 0);
      ftruncate(logFD, HEADERLOG);
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
    for(int i=0; i<valsize; i++) log_buffer[buff_pos+i] = old_img[i];
    buff_pos += valsize;
    for(int i=0; i<valsize; i++) log_buffer[buff_pos+i] = new_img[i];
    buff_pos += valsize;
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
  buff_pos = 0;
  pwrite(logFD, header_log, HEADERLOG, 0);
  fsync(logFD);

  memset(log_buffer, 0, LOGBUFFSIZE);
  UNLOCK(log_mutex);
}

void analysis() {
  LSN_t LSN;
  main_log_t* main_log;
  int max_winner_id = 0;
  int max_loser_id = 0;
  std::set<int> winner;
  std::set<int> loser;
  int trx_id;
  loser_trx_t* loser_trx;

  fprintf(logmsgFP, "[ANALYSIS] Analysis pass start\n");

  main_log = new main_log_t();
  LSN = HEADERLOG;
  while (LSN < header_log->flushed_LSN) {
    if (pread(logFD, main_log, MAINLOG, LSN) != MAINLOG) break;
    trx_id = main_log->trx_id;
    switch (main_log->type) {
      case BEGIN:
        loser_trx = new loser_trx_t;
        loser_trx->trx_id = trx_id;
        loser_trx->begin_LSN = LSN;
        loser_trx->last_LSN = LSN;
        loser_trx_map.insert({trx_id, loser_trx});
        loser.insert(trx_id);
        break;
      case COMMIT:
      case ROLLBACK:
        delete loser_trx_map[trx_id];
        loser_trx_map.erase(trx_id);
        loser.erase(trx_id);
        winner.insert(trx_id);
        break;
      default:
        loser_trx_map[trx_id]->last_LSN = LSN;
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
  int trx_id;
  int page_idx;
  int type;
  int loop;
  LSN_t LSN;
  LSN_t next_undo_LSN;
  page_t* page;

  LSN = HEADERLOG;
  main_log = new main_log_t();
  update_log = new update_log_t();
  fprintf(logmsgFP, "[REDO] Redo pass start\n");
  loop = 0;
  while ((LSN < header_log->flushed_LSN) && (log_num == NO_CRASH || loop < log_num)) {
    if (pread(logFD, main_log, MAINLOG, LSN) != MAINLOG) break;
    trx_id = main_log->trx_id;
    type = main_log->type;
    if (type == UPDATE || type == COMPENSATE) 
    {
      pread(logFD, update_log, UPDATELOG, LSN+MAINLOG);
      valsize = update_log->valsize;
      offset = update_log->offset - 128;
      table_id = update_log->table_id;
      page_id = update_log->page_id;
      open_recovery_table(table_id);

      page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
      if (page->LSN < LSN) 
      {
        new_img = new char[valsize + 2];
        pread(logFD, new_img, valsize, LSN+MAINLOG+UPDATELOG+valsize);

        for (int i = 0; i < valsize; i++) 
          page->leafbody.value[i + offset] = new_img[i];
        page->LSN = LSN;

        if (type == UPDATE)
          fprintf(logmsgFP, "LSN %lu [UPDATE] Transaction id %d redo apply\n", LSN, main_log->trx_id);
        else {
          pread(logFD, &next_undo_LSN, 8, LSN+MAINLOG+UPDATELOG+(2*valsize));
          fprintf(logmsgFP, "LSN %lu [CLR] next undo lsn %lu\n", LSN, next_undo_LSN);
        }
        buffer_write_page(table_id, page_id, page_idx, 1);

        delete[] new_img;
      }
      else 
      {
        fprintf(logmsgFP, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", LSN, main_log->trx_id);
        buffer_write_page(table_id, page_id, page_idx, 0);
      }
    }

    else 
    {
      if (type == BEGIN)
        fprintf(logmsgFP, "LSN %lu [BEGIN] Transaction id %d\n", LSN, trx_id);
      else if (main_log->type == COMMIT)
        fprintf(logmsgFP, "LSN %lu [COMMIT] Transaction id %d\n", LSN, trx_id);
      else
        fprintf(logmsgFP, "LSN %lu [ROLLBACK] Transaction id %d\n", LSN, trx_id);
    }
    LSN += main_log->log_size;
    loop++;
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
  int type;
  LSN_t LSN;
  LSN_t last_LSN;
  LSN_t next_undo_LSN;
  LSN_t prev_LSN;
  LSN_t cur_LSN;
  main_log_t* rollback_log;
  main_log_t* new_main_log;
  update_log_t* new_update_log;
  page_t* page;
  priority_table_t priority_table;
  loser_trx_map_t::iterator it;
  loser_trx_t* loser_trx;

  fprintf(logmsgFP, "[UNDO] Undo pass start\n");

  for (it=loser_trx_map.begin(); it!=loser_trx_map.end(); it++) {
    loser_trx = it->second;
    trx_id = loser_trx->trx_id;
    priority_table.push(loser_trx->last_LSN);
  }

  loop = 0;
  main_log = new main_log_t();
  update_log = new update_log_t();

  while (!priority_table.empty() && (log_num == NO_CRASH || ++loop <= log_num)) {
    LSN = priority_table.top();
    priority_table.pop();
    
    pread(logFD, main_log, MAINLOG, LSN);
    trx_id = main_log->trx_id;
    prev_LSN = main_log->prev_LSN;
    type = main_log->type;
    loser_trx = loser_trx_map[trx_id];

    if(type == BEGIN) 
    {
      rollback_log = make_main_log(trx_id, ROLLBACK, MAINLOG, loser_trx->last_LSN);
      delete loser_trx;
      loser_trx_map.erase(trx_id);
      push_log_to_buffer(rollback_log, 0, 0, 0, 0);
    }

    else if(type == COMPENSATE)
    {
      while(main_log->type == COMPENSATE) {
        pread(logFD, main_log, MAINLOG, main_log->prev_LSN);
      } 
      priority_table.push(main_log->LSN);
    }

    else {
      pread(logFD, update_log, UPDATELOG, LSN+MAINLOG);
      table_id = update_log->table_id;
      page_id = update_log->page_id;
      offset = update_log->offset - 128;
      size = update_log->valsize;

      old_img = new char[size+2];
      new_img = new char[size+2];
      pread(logFD, new_img, size, LSN+MAINLOG+UPDATELOG);
      pread(logFD, old_img, size, LSN+MAINLOG+UPDATELOG+size);
  
      pread(logFD, main_log, MAINLOG, prev_LSN);
      next_undo_LSN = (main_log->type == UPDATE) ? prev_LSN : 0;

      open_recovery_table(table_id);
      page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
      
      if(page->LSN >= LSN) {
        new_main_log = make_main_log(trx_id, COMPENSATE, MAINLOG + UPDATELOG + (2*size) + 8, next_undo_LSN);
        new_update_log = make_update_log(table_id, page_id, size, offset+128);
        loser_trx->last_LSN = new_main_log->LSN;
        for (int i=0; i<size; i++)  
          page->leafbody.value[i+offset] = new_img[i];
        push_log_to_buffer(new_main_log, new_update_log, old_img, new_img, next_undo_LSN);
        
        page->LSN = LSN;
        fprintf(logmsgFP, "LSN %lu [UPDATE] Transaction id %d undo apply\n", LSN, trx_id);
        buffer_write_page(table_id, page_id, page_idx, 1);
      }

      if(!next_undo_LSN) {
        rollback_log = make_main_log(trx_id, ROLLBACK, MAINLOG, loser_trx->last_LSN);
        delete loser_trx;
        loser_trx_map.erase(trx_id);
        push_log_to_buffer(rollback_log, 0, 0, 0, 0);
      } 
      else
        priority_table.push(next_undo_LSN);
    }
  }
  delete main_log;
  delete update_log;

  if (log_num == -1) fprintf(logmsgFP, "[UNDO] Undo pass end\n");
}

int shutdown_log() {
  for (auto it = loser_trx_map.begin(); it != loser_trx_map.end(); it++)
    delete it->second;
  delete header_log;
  close(logFD);
  fclose(logmsgFP);
  return 0;
}