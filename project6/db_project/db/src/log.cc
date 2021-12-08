#include "log.h"

pthread_mutex_t             log_mutex;
header_log_t*               header_log;
log_trx_table_t             log_trx_table;
std::set<int>               loser;
std::set<int>               winner;
uint64_t                    buff_tail;
uint64_t                    non_flushed_LSN;
FILE*                       logmsg_file;
int                         logFD;
char                        log_buffer[LOGBUFFSIZE];

void
open_recovery_file(int64_t table_id)
{ 
  char file_path[40];

  sprintf(file_path, "DATA%ld", table_id);
  file_open_table_file(file_path);
}

log_t*
make_log(int log_size, uint64_t prev_LSN, int trx_id, int type)
{
  log_t*              log;

  LOCK(log_mutex);
  log = new log_t;
  log->log_size = log_size;
  log->LSN = non_flushed_LSN + buff_tail;
  log->type = type;
  log->trx_id = trx_id;
  log->prev_LSN = prev_LSN;
  
  return log;
}

update_log_t*
make_up_log(int64_t table_id, pagenum_t page_id, uint16_t offset, uint16_t valsize)
{
  update_log_t*      up_log;

  up_log = new update_log_t;
  up_log->offset = offset;
  up_log->page_id = page_id;
  up_log->table_id = table_id;
  up_log->valsize = valsize;

  return up_log;
}

void
push_log_buf(log_t* log, update_log_t* up_log, char* old_img, char* new_img, uint64_t next_undo_LSN) 
{
  if(buff_tail>=LOGTHRESHOLD) {
    pwrite(logFD, log_buffer, buff_tail, non_flushed_LSN);
    header_log->flushed_LSN = non_flushed_LSN + buff_tail;
    pwrite(logFD, header_log, LGHDSIZE, 0);
    non_flushed_LSN = header_log->flushed_LSN;
    fsync(logFD);

    buff_tail = 0;
  }
  memcpy(log_buffer+buff_tail, log, LGSIZE);
  buff_tail+=LGSIZE;
  if(log->type == UPDATED || log->type == COMPENSATE) 
  {
    memcpy(log_buffer+buff_tail, up_log, UPSIZE);
    buff_tail+=UPSIZE;
    memcpy(log_buffer+buff_tail, old_img, up_log->valsize);
    buff_tail+=up_log->valsize;
    memcpy(log_buffer+buff_tail, new_img, up_log->valsize);
    buff_tail+=up_log->valsize;
    if(log->type == COMPENSATE)  {
      memcpy(log_buffer+buff_tail, &next_undo_LSN, 8);
      buff_tail+=8;
    }
    delete[] old_img;
    delete[] new_img;
    delete up_log;
  }
  delete log;

  UNLOCK(log_mutex);
}

void
log_flush()
{
  LOCK(log_mutex);
  if(!buff_tail) {
    UNLOCK(log_mutex);
    return;  
  }
  pwrite(logFD, log_buffer, buff_tail, non_flushed_LSN);
  header_log->flushed_LSN = non_flushed_LSN + buff_tail;
  pwrite(logFD, header_log, LGHDSIZE, 0);
  non_flushed_LSN = header_log->flushed_LSN;
  fsync(logFD);

  buff_tail = 0;

  UNLOCK(log_mutex);
}

int give_last_trx_id() { return header_log->last_trx_id; }

void 
analysis() 
{
  log_trx_t*        log_trx;
  log_t*            log;
  uint64_t          LSN;
  int               max_winner_id;
  int               max_loser_id;

  fprintf(logmsg_file, "[ANALYSIS] Analysis pass start\n");

  log = new log_t;
  LSN = LGHDSIZE;
  while (pread(logFD, log, LGSIZE, LSN)) {
    switch (log->type) {
      case BEGIN:
        log_trx = new log_trx_t();
        log_trx->trx_id = log->trx_id;
        log_trx->last_LSN = log->LSN;
        loser.insert(log->trx_id);
        log_trx_table[log->trx_id] = log_trx;
        break;
      case COMMITED:
      case ROLLBACK:
        delete log_trx_table[log->trx_id];
        log_trx_table.erase(log->trx_id);
        loser.erase(log->trx_id);
        winner.insert(log->trx_id);
        break;
      default:
        break;
    }
    LSN += log->log_size;
    if(LSN >= non_flushed_LSN) break;
  }
  delete log;
  fprintf(logmsg_file, "[ANALYSIS] Analysis success. Winner:");
  for(auto it = winner.begin(); it != winner.end(); it++) {
    max_winner_id = (*it);
    fprintf(logmsg_file, " %d", (*it));
  }
  fprintf(logmsg_file, ", Loser:");
  for(auto it = loser.begin(); it != loser.end(); it++) {
    max_loser_id = (*it);
    fprintf(logmsg_file, " %d", (*it));
  }
  fprintf(logmsg_file, "\n");
  header_log->last_trx_id = ((max_winner_id > max_loser_id) ? max_winner_id : max_loser_id);
}

void
redo(int log_num)
{ 
  log_t*            log;
  page_t*           page;
  log_trx_t*        log_trx;
  update_log_t*     up_log;
  char*             new_img;
  pagenum_t         page_id;
  uint64_t          LSN;
  uint64_t          next_undo_LSN;
  uint16_t          offset;
  uint16_t          size;
  int64_t           table_id;
  int               page_idx;
  int               loop;
  

  fprintf(logmsg_file, "[REDO] Redo pass start\n");

  log = new log_t;
  up_log = new update_log_t;
  LSN = LGHDSIZE;
  loop = 0;
  while((pread(logFD, log, LGSIZE, LSN)) && (log_num == NO_CRASH || ++loop<=log_num))
  {
    if(log_trx_table.find(log->trx_id) != log_trx_table.end()) {
      log_trx = log_trx_table[log->trx_id];
      if(log->type == UPDATED)
        log_trx->update_S.push(log->LSN);
      else if(log->type == COMPENSATE) {
        if(!log_trx->update_S.empty()) log_trx->update_S.pop();
      }
      log_trx->last_LSN = log->LSN;
    }

    if(log->type == UPDATED || log->type == COMPENSATE) 
    {
      pread(logFD, up_log, UPSIZE, LSN + LGSIZE);
      table_id = up_log->table_id;
      page_id = up_log->page_id;
      open_recovery_file(table_id);
      page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
      if(page->LSN >= LSN) {
        fprintf(logmsg_file, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", log->LSN, log->trx_id);
        buffer_write_page(table_id, page_id, page_idx, 0);
      }
      else 
      {
        offset = up_log->offset-128;
        size = up_log->valsize;
        new_img = new char[size + 1];
        pread(logFD, new_img, size, LSN + LGSIZE + UPSIZE + size);
        for(int i=0; i<size; i++) 
          page->leafbody.value[i+offset] = new_img[i];
        page->LSN = log->LSN;
        buffer_write_page(table_id, page_id, page_idx, 1);
        if(log->type == UPDATED) 
          fprintf(logmsg_file, "LSN %lu [UPDATE] Transaction id %d redo apply\n", log->LSN, log->trx_id);
        else {
          pread(logFD, &next_undo_LSN, 8, LSN + LGSIZE + UPSIZE + 2*size);
          fprintf(logmsg_file, "LSN %lu [CLR] next undo lsn %lu\n", log->LSN, next_undo_LSN);
        }
        delete[] new_img;
      }
    } 
    else
    {
      if(log->type == BEGIN) 
        fprintf(logmsg_file, "LSN %lu [BEGIN] Transaction id %d\n", log->LSN, log->trx_id);
      else if(log->type == COMMITED)
        fprintf(logmsg_file, "LSN %lu [COMMIT] Transaction id %d\n", log->LSN, log->trx_id);
      else
        fprintf(logmsg_file, "LSN %lu [ROLLBACK] Transaction id %d\n", log->LSN, log->trx_id);
    }
    LSN += log->log_size;
    if(LSN >= non_flushed_LSN) break;
  }
  if(log_num == -1) fprintf(logmsg_file, "[REDO] Redo pass end\n");
  delete log;
  delete up_log;
}

void
undo(int log_num)
{
  priority_table_t    priority_table;
  uint64_t            last_LSN;
  priority_trx_t      prior_trx;
  log_t*              log;
  log_t*              new_log;
  log_t*              rollback_log;
  page_t*             page;
  log_trx_t*          log_trx;
  
  update_log_t*       up_log;
  update_log_t*       new_up_log;
  char*               new_old_img;
  char*               new_new_img;
  char*               old_img;
  char*               new_img;  
  pagenum_t           page_id;
  int64_t             table_id;
  int                 page_idx;
  int                 trx_id;
  int                 loop;
  uint16_t            offset;
  uint16_t            size;

  fprintf(logmsg_file, "[UNDO] Undo pass start\n");
  
  for(auto it = loser.begin(); it != loser.end(); it++) 
  {
    if(log_trx_table[(*it)]->update_S.empty()) {
      rollback_log = make_log(LGSIZE, log_trx_table[(*it)]->last_LSN, (*it), ROLLBACK);
      push_log_buf(rollback_log, nullptr, nullptr, nullptr, 0);
      continue;
    }
    prior_trx.last_LSN = log_trx_table[(*it)]->update_S.top();
    prior_trx.trx_id = (*it);
    priority_table.push(prior_trx);
  }

  loop = 0;
  log = new log_t;
  up_log = new update_log_t;
  while(!(priority_table.empty()) && (log_num == NO_CRASH || ++loop<=log_num))
  {
    prior_trx = priority_table.top(); priority_table.pop();
    last_LSN = prior_trx.last_LSN;
    trx_id = prior_trx.trx_id;
    log_trx = log_trx_table[trx_id];
    if(!log_trx->update_S.empty()) log_trx->update_S.pop();

    pread(logFD, log, LGSIZE, last_LSN);
    pread(logFD, up_log, UPSIZE, last_LSN + LGSIZE);
    table_id = up_log->table_id;
    page_id = up_log->page_id;
    offset = up_log->offset;
    size = up_log->valsize;

    old_img = new char[size+1];
    new_img = new char[size+1];
    pread(logFD, old_img, size, last_LSN + LGSIZE + UPSIZE);
    pread(logFD, new_img, size, last_LSN + LGSIZE + UPSIZE + size);

    page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
    if(page->LSN >= log->LSN) 
    {
      new_log = make_log(LGSIZE + UPSIZE + 2*size + 8, log_trx->last_LSN, trx_id, COMPENSATE);
      new_up_log = make_up_log(table_id, page_id, offset, size);
      new_old_img = new char[size+1];
      new_new_img = new char[size+1];
      for(int i=0; i<size; i++) {
        new_old_img[i] = new_img[i];
        new_new_img[i] = old_img[i];
      }
      log_trx->last_LSN = new_log->LSN;
      push_log_buf(new_log, new_up_log, new_old_img, new_new_img, (log_trx->update_S.empty()) ? 0 : log_trx->update_S.top());

      for(int i=0; i<size; i++) 
        page->leafbody.value[offset-128+i] = old_img[i];
      page->LSN = new_log->LSN;
      fprintf(logmsg_file, "LSN %lu [UPDATE] Transaction id %d undo apply\n", log->LSN, trx_id);
      buffer_write_page(table_id, page_id, page_idx, 1);
    } 
    else buffer_write_page(table_id, page_id, page_idx, 0);

    if(log_trx->update_S.empty()) {
      rollback_log = make_log(LGSIZE, log_trx->last_LSN, log_trx->trx_id, ROLLBACK);
      push_log_buf(rollback_log, nullptr, nullptr, nullptr, 0);
    } else {
      prior_trx.last_LSN = log_trx->update_S.top();
      priority_table.push(prior_trx);
    }
    delete[] old_img;
    delete[] new_img;
  }
  delete log;
  delete up_log;
  if(log_num == -1) fprintf(logmsg_file, "[UNDO] Undo pass end\n");
}

int 
init_log(int flag, int log_num, char* log_path, char* logmsg_path) 
{
  int   first;

  log_mutex = PTHREAD_MUTEX_INITIALIZER;
  if ((logFD = open(log_path, O_RDWR | O_CREAT, 0777)) <= 0) return 1;
  if (!(logmsg_file = fopen(logmsg_path, "w+"))) return 1;

  header_log = new header_log_t();
  first = pread(logFD, header_log, LGHDSIZE, 0);
  buff_tail = 0;
  if(first <= 0) {
    header_log->last_trx_id = 0;
    header_log->flushed_LSN = LGHDSIZE;
    non_flushed_LSN = LGHDSIZE;
  } else {
    if(!(non_flushed_LSN = header_log->flushed_LSN)) non_flushed_LSN = LGHDSIZE;
    analysis();
    if(flag == RECOVERY) {
      redo(NO_CRASH);
      undo(NO_CRASH);
      log_flush();
    } else if(flag == REDO_CRASH) {
      redo(log_num);
    } else {
      redo(NO_CRASH);
      undo(log_num);
    }
  }
  return 0;
}

int
shutdown_log()
{
  log_flush();
  delete header_log;
  log_trx_table.clear();
  close(logFD);
  fclose(logmsg_file);
  file_close_table_files();
  return 0;
}