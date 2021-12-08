#ifndef __LOG_H__
#define __LOG_H__

#include "file.h"
#include "buffer.h"

#define RECOVERY (0)
#define REDO_CRASH (1)
#define UNDO_CRASH (2)
#define NO_CRASH (-1)
#define BEGIN (0)
#define UPDATED (1)
#define COMMITED (2)
#define ROLLBACK (3)
#define COMPENSATE (4)
#define LOGBUFFSIZE (4096UL)
#define LOGTHRESHOLD (LOGBUFFSIZE - 280)
#define LGHDSIZE (16)
#define LGSIZE   (28)
#define UPSIZE   (20)

#define LOCK(X) (pthread_mutex_lock(&(X)))
#define UNLOCK(X) (pthread_mutex_unlock(&(X)))

typedef struct header_log_t {
  uint64_t flushed_LSN;
  int last_trx_id;
} header_log_t;

typedef struct __attribute__((__packed__)) log_t {
  int log_size;
  uint64_t LSN;
  uint64_t prev_LSN;
  int trx_id;
  int type;
} log_t;

typedef struct __attribute__((__packed__)) update_log_t {
  int64_t table_id;
  pagenum_t page_id;
  uint16_t offset;
  uint16_t valsize;
} update_log_t;

typedef struct log_trx_t {
  int trx_id;
  uint64_t last_LSN;
	std::stack<uint64_t> update_S;
} log_trx_t;

typedef struct priority_trx_t {
  int trx_id;
  uint64_t last_LSN;
} priority_trx_t;

struct compare {
  bool operator()(const priority_trx_t& trx1, const priority_trx_t& trx2) {
    return trx1.last_LSN < trx2.last_LSN;
  }
};

typedef std::unordered_map<int, log_trx_t*> log_trx_table_t;
typedef std::priority_queue<priority_trx_t, std::vector<priority_trx_t>, compare> priority_table_t;

log_t* make_log(int log_size, uint64_t prev_LSN, int trx_id, int type);
update_log_t* make_up_log(int64_t table_id, pagenum_t page_id, uint16_t offset, uint16_t valsize);
void push_log_buf(log_t* log, update_log_t* up_log, char* old_img, char* new_img, uint64_t next_undo_LSN);
void log_flush();
void analysis();
void redo(int log_num);
int give_last_trx_id();
void undo(int log_num);
int init_log(int flag, int log_num, char* log_path, char* logmsg_path);
int shutdown_log();

#endif