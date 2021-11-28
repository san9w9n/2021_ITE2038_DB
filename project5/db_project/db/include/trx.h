#ifndef __TRX_H__
#define __TRX_H__

#include <bpt.h>
#include <unordered_map>
#include <vector>
typedef uint64_t pagenum_t;

// for lock manager

typedef struct pair_hash {
  	template <class T1, class T2>
  	std::size_t operator() (const std::pair<T1, T2> &pair) const {
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
    int owner_trx_id;
    bool lock_mode;
} lock_t;

typedef struct entry_t {
	int64_t table_id;
	pagenum_t page_id;
	lock_t* tail;
	lock_t* head;
} entry_t;

typedef std::unordered_map<key_pair_t, entry_t* , pair_hash> lock_table_t;
typedef struct lock_manager_t {
    lock_table_t lock_table;
} lock_manager_t;

// for trx manager

typedef struct log_t {
    int64_t table_id;
    int64_t key;
    char* old_value;
    int16_t val_size;
} log_t;
typedef std::pair<int64_t, int64_t> log_key_t;
typedef std::unordered_map<log_key_t, log_t* , pair_hash> log_table_t;

typedef struct trx_t {
    log_table_t log_table;
    lock_t* trx_next;
    int wait_trx_id;
    int state;
} trx_t;
typedef std::vector<trx_t*> trx_table_t;

typedef struct trx_manager_t {
    trx_table_t trx_table;
    int trx_cnt;
} trx_manager_t;


typedef std::unordered_map<int, bool> visit_t;

lock_t* give_lock(uint64_t bitmap, int trx_id, bool lock_mode);
entry_t* give_entry(int64_t table_id, pagenum_t page_id);
int init_db(int num_buf);
int shutdown_db();
int init_lock_table(void);
int init_trx_table(void);
int trx_begin(void);
trx_t* give_trx(int trx_id);
int trx_commit(int trx_id);
void trx_abort(int trx_id);
int lock_release(trx_t* trx);
bool deadlock_detect(int trx_id);
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t *val_size, int trx_id);
int db_update(int64_t table_id, int64_t key, char* values, uint16_t new_val_size, uint16_t* old_val_size, int trx_id);
void append_lock(entry_t* entry, lock_t* lock, trx_t* trx);
int lock_acquire(int64_t table_id, pagenum_t page_id, int kindex, int trx_id, bool lock_mode);

#endif