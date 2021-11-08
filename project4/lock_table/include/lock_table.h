#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>

typedef struct lock_t lock_t;
typedef struct link_t link_t;
typedef struct entry_t entry_t;

int init_lock_table();
lock_t *lock_acquire(int64_t table_id, int64_t key);
int lock_release(lock_t* lock_obj);

#endif /* __LOCK_TABLE_H__ */
