#ifndef __TRX_H__
#define __TRX_H__

#include <buffer.h>

#define SHARED 0
#define EXCLUSIVE 1

#define ACQUIRED 0
#define WAITING 1

#define NORMAL 0
#define DEADLOCK 1

#define ACTIVE 0
#define COMMIT 1
#define ABORT 2

#define MASK(X) (1UL << (63 - (X)))
#define WAIT(X, Y) (pthread_cond_wait(&(X), &(Y)))
#define BROADCAST(X) (pthread_cond_broadcast(&(X)))

typedef struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2>& pair) const {
    return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
  }
} pair_hash;
typedef std::pair<int64_t, pagenum_t> key_pair_t;

typedef struct entry_t entry_t;

typedef struct lock_t {
  pthread_cond_t cond;
  lock_t* lock_prev;
  lock_t* lock_next;
  lock_t* trx_next;
  entry_t* sent_point;
  uint64_t bitmap;
  int64_t key;
  int owner_trx_id;
  bool lock_mode;
} lock_t;

typedef struct entry_t {
  int64_t table_id;
  pagenum_t page_id;
  lock_t* tail;
  lock_t* head;
} entry_t;

typedef std::unordered_map<key_pair_t, entry_t*, pair_hash> lock_table_t;

typedef struct undo_t {
  int64_t table_id;
  int64_t key;
  char* old_value;
  int16_t val_size;
} undo_t;
typedef std::pair<int64_t, int64_t> log_key_t;
typedef std::stack<undo_t*> undo_table_t;

typedef struct trx_t {
  undo_table_t undo_table;
  lock_t* trx_next;
  int wait_trx_id;
  int state;
  int last_LSN;
} trx_t;
typedef std::vector<trx_t*> trx_table_t;

int init_trx(int num_buf, int flag, int log_num, char* log_path, char* logmsg_path);
int shutdown_trx();
lock_t* give_lock(int64_t key, uint64_t bitmap, int trx_id, bool lock_mode);
entry_t* give_entry(int64_t table_id, pagenum_t page_id);
int trx_begin(void);
trx_t* give_trx(int trx_id);
int trx_commit(int trx_id);
int trx_abort(int trx_id);
int lock_release(trx_t* trx);
bool deadlock_detect(int trx_id);
void append_lock(entry_t* entry, lock_t* lock, trx_t* trx);
bool lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int kindex,
                  int trx_id, bool lock_mode);

#endif