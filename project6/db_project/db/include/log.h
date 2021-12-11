#ifndef __LOG_H__
#define __LOG_H__

#include "file.h"
#include "buffer.h"

#define RECOVERY 0
#define REDO_CRASH 1
#define UNDO_CRASH 2

#define BEGIN 0
#define UPDATE 1
#define COMMIT 2
#define ROLLBACK 3
#define COMPENSATE 4
#define LOGBUFFSIZE 4096
#define LOGTHRESHOLD 3600

#define HEADERLOG 12
#define MAINLOG 28
#define UPDATELOG 20

#define LOCK(X) (pthread_mutex_lock(&(X)))
#define UNLOCK(X) (pthread_mutex_unlock(&(X)))

typedef uint64_t LSN_t;

typedef struct __attribute__((__packed__)) header_log_t {
  LSN_t flushed_LSN;
  int last_trx_id;
} header_log_t;

typedef struct __attribute__((__packed__)) main_log_t {
  int log_size;
  LSN_t LSN;
  LSN_t prev_LSN;
  int trx_id;
  int type;
} main_log_t;

typedef struct __attribute__((__packed__)) update_log_t {
  int64_t table_id;
  pagenum_t page_id;
  uint16_t offset;
  uint16_t valsize;
} update_log_t;

typedef struct loser_trx_t {
  int trx_id;
  LSN_t begin_LSN;
  LSN_t last_LSN;
} active_trx;
typedef std::unordered_map<int, active_trx*> loser_trx_map_t;

typedef std::priority_queue<LSN_t> priority_table_t;

void file_read_mainlog(main_log_t* main_log, LSN_t LSN);
void push_log_to_buffer(main_log_t* main_log, update_log_t* update_log,
                        char* old_img, char* new_img, LSN_t next_undo_LSN);
main_log_t* make_main_log(int trx_id, int type, int log_size, LSN_t prev_LSN);
update_log_t* make_update_log(int64_t table_id, pagenum_t page_id,
                              uint16_t valsize, uint16_t offset);
void analysis();
int last_trx_id();
int init_log(int flag, int log_num, char* log_path, char* logmsg_path);
void log_flush();
int shutdown_log();
void redo(int log_num);
void undo(int log_num);

#endif