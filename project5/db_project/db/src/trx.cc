#include "trx.h"

#pragma GCC optimize("O3")
#pragma GCC optimize("Ofast")
#pragma GCC optimize("unroll-loops")
#include <stdlib.h>
#include <cstring>
#include <stdio.h>
#include <queue>

#define SHARED 0
#define EXCLUSIVE 1

#define ACQUIRED 0
#define WAITING 1

#define NORMAL 0
#define DEADLOCK 1
#define CHECK_IMPL 2

#define ACTIVE 0
#define COMMIT 1
#define ABORT 2

#define MASK(X) (1UL<<(63-(X)))
#define LOCK(X) (pthread_mutex_lock(&(X)))
#define UNLOCK(X) (pthread_mutex_unlock(&(X)))
#define WAIT(X, Y) (pthread_cond_wait(&(X), &(Y)))
#define SIGNAL(X) (pthread_cond_signal(&(X)))

lock_manager_t* lock_manager = nullptr;
trx_manager_t* trx_manager = nullptr;
pthread_mutex_t lock_mutex;
pthread_mutex_t trx_mutex;

lock_t*
give_lock(int64_t key, int trx_id, bool lock_mode)
{
  lock_t*                 lock;

  lock = new lock_t;
  lock->lock_prev = lock->lock_next = nullptr;
  lock->trx_next = nullptr;
  lock->sent_point = nullptr;
  lock->cond = PTHREAD_COND_INITIALIZER;
  lock->key = key;
  lock->owner_trx_id = trx_id;
  lock->lock_mode = lock_mode;
  lock->lock_state = WAITING;

  return lock;
}

entry_t*
give_entry(int64_t table_id, pagenum_t page_id)
{
  entry_t*                entry;

  entry = new entry_t;
  entry->page_id = page_id;
  entry->table_id = table_id;
  entry->head = entry->tail = nullptr;

  return entry;
}

int 
init_db(int num_buf)
{
  init_lock_table();
  init_trx_table();
  return init_buffer(num_buf);
}

int 
shutdown_db() 
{
  if(lock_manager) delete lock_manager;
  if(trx_manager) delete trx_manager;
  return shutdown_buffer();
}

int
init_lock_table(void)
{
  if(lock_manager) return 1;
  lock_manager = new lock_manager_t();
  lock_mutex = PTHREAD_MUTEX_INITIALIZER;
  return 0;
}

int
init_trx_table(void)
{
  if(trx_manager) return 1;
  trx_manager = new trx_manager_t();
  trx_manager->trx_cnt = 0;
  trx_mutex = PTHREAD_MUTEX_INITIALIZER;
  return 0;
}

int
trx_begin(void)
{
  trx_t*                  trx;
  trx_table_t::iterator   it;
  int                     trx_id;

  LOCK(trx_mutex);
  trx_id = ++trx_manager->trx_cnt;
  it = trx_manager->trx_table.find(trx_id);
  if(it==trx_manager->trx_table.end()) {
    trx = new trx_t();
    trx->small_trx_mutex = PTHREAD_MUTEX_INITIALIZER;
    trx->state = ACTIVE;
    trx->trx_next = trx->waiting_lock = nullptr;
    trx_manager->trx_table[trx_id] = trx;
  } else {
    UNLOCK(trx_mutex);
    return 0;
  }
  UNLOCK(trx_mutex);

  return trx_id;
}

bool
isValid_trx(int trx_id) 
{
  trx_table_t::iterator it;
  LOCK(trx_mutex);
  it = trx_manager->trx_table.find(trx_id);
  if(it == trx_manager->trx_table.end()) {
    UNLOCK(trx_mutex);
    return false;
  }
  if(it->second->state==ACTIVE) {
    UNLOCK(trx_mutex);
    return true;
  }
  UNLOCK(trx_mutex);
  return false;
}

int
trx_commit(int trx_id)
{
  trx_t*                  trx;
  trx_table_t::iterator   it;

  LOCK(trx_mutex);
  it = trx_manager->trx_table.find(trx_id);
  if(it == trx_manager->trx_table.end()) {
    UNLOCK(trx_mutex);
    return 0;
  }
  trx = it->second;
  LOCK(trx->small_trx_mutex);
  UNLOCK(trx_mutex);

  if(trx->state == ACTIVE) {
    lock_release(trx);
    for(auto log_it = trx->log_table.begin(); log_it != trx->log_table.end(); log_it++) {
      delete[] log_it->second->old_value;
      delete log_it->second;
    }
    trx->log_table.clear();
  }
  trx->state == COMMIT;
  UNLOCK(trx->small_trx_mutex);

  return trx_id;
}

void
trx_abort(int trx_id)
{
  int                     leaf_idx;
  int                     i;
  uint16_t                offset;
  uint16_t                size;
  int64_t                 table_id;
  pagenum_t               page_id;
  pagenum_t               root_num;
  int64_t                 key;
  trx_t*                  trx;
  lock_t*                 lock;
  lock_t*                 prev_lock;
  page_t*                 leaf_page;
  page_t*                 header_page;
  int                     header_idx;
  char*                   old_value;
  trx_table_t::iterator   it;
  log_t*                  log;


  trx = trx_manager->trx_table[trx_id];
  for(auto log_it = trx->log_table.begin(); log_it != trx->log_table.end(); log_it++) 
  {
    log = log_it->second;

    table_id = log->table_id;
    key = log->key;

    header_page = buffer_read_page(table_id, 0, &header_idx, READ);
    root_num = header_page->root_num;
    page_id = find_leaf(table_id, root_num, key);
    leaf_page = buffer_read_page(table_id, page_id, &leaf_idx, WRITE);
    for(i=0; i<leaf_page->info.num_keys; i++) {
      if(leaf_page->leafbody.slot[i].key == key) break;
    }

    size = log->val_size;
    leaf_page->leafbody.slot[i].size = size;
    offset = leaf_page->leafbody.slot[i].offset-128;
    for(int k=offset; k<offset+size; k++) {
      leaf_page->leafbody.value[k] = log->old_value[k-offset];
    }
    buffer_write_page(table_id, page_id, leaf_idx, 1);

    delete[] log->old_value;
    delete log;
    log = nullptr;
  }
  trx->log_table.clear();
  
  lock_release(trx);

  trx->state == ABORT;
  UNLOCK(trx->small_trx_mutex);
}

int 
lock_release(trx_t* trx)
{
  lock_t*                 point;
  lock_t*                 lock;
  lock_t*                 del;
  lock_t*                 tmp;
  entry_t*                entry;
  int                     lock_mode;
  int                     flag;
  int                     leaf_idx;
  int                     cnt;
  int64_t                 key;
  uint64_t                bitmap;
  int64_t                 table_id;
  pagenum_t               page_id;
  page_t*                 leaf;

  LOCK(lock_mutex);

  point = trx->trx_next;
  while(point) 
  {
    entry = point->sent_point;
    table_id = entry->table_id;
    page_id = entry->page_id;
    key = point->key;
    lock_mode = point->lock_mode;
    lock = point->lock_next;

    if(entry->head == point) entry->head = point->lock_next;
    if(entry->tail == point) entry->tail = point->lock_prev;
    if(point->lock_next) point->lock_next->lock_prev = point->lock_prev;
    if(point->lock_prev) point->lock_prev->lock_next = point->lock_next;

    if(!entry->head) {
      delete entry;
      lock_manager->lock_table.erase({table_id, page_id});
      goto CONTINUE;
    }

    if(point->lock_state == WAITING)
      goto CONTINUE;

    if(lock_mode == SHARED) 
    {
      cnt = 0;
      tmp = entry->head;
      while(tmp) {
        if((tmp->key == key) && (tmp->lock_state == ACQUIRED) && (tmp->lock_mode == SHARED))
          cnt++;
        tmp = tmp->lock_next;
      }
      while(lock) 
      {
        if((lock->key == key) && (lock->lock_mode == EXCLUSIVE)) {
          lock->lock_state = ACQUIRED;
          trx_manager->trx_table[lock->owner_trx_id]->waiting_lock = nullptr;
          SIGNAL(lock->cond);
          break;
        }
        lock = lock->lock_next;
      }
    }

    else 
    {
      flag = 0;
      while(lock) 
      {
        if(lock->key == key) {
          if(lock->lock_mode == SHARED) {
            flag = 1;
            lock->lock_state = ACQUIRED;
            trx_manager->trx_table[lock->owner_trx_id]->waiting_lock = nullptr;
            SIGNAL(lock->cond);
          }
          else {
            if(!flag) {
              lock->lock_state = ACQUIRED;
              trx_manager->trx_table[lock->owner_trx_id]->waiting_lock = nullptr;
              SIGNAL(lock->cond);
            }
            break;
          }
        }
        lock = lock->lock_next;
      }
    }

CONTINUE:
    del = point;
    point = point->trx_next;
    trx->trx_next = point;
    delete del;
  }
  UNLOCK(lock_mutex);

  return 0;
}

bool
deadlock_detect(lock_t* lock_obj) 
{
  std::queue<int>         Q;
  visit_t                 visit;
  lock_t*                 point;
  page_t*                 leaf;
  lock_t*                 tmp;
  trx_t*                  trx;
  int                     cur;
  int                     leaf_idx;
  int                     i;
  int64_t                 key;

  LOCK(trx_mutex);
  key = lock_obj->key;
  point = lock_obj->lock_prev;
  while(point) {
    if((point->key == key) && (point->owner_trx_id != lock_obj->owner_trx_id))
      Q.push(point->owner_trx_id);
    point = point->lock_prev;
  }

  while(!Q.empty()) 
  {
    cur = Q.front(); Q.pop();
    if(visit.find(cur) != visit.end()) continue;
    
    trx = trx_manager->trx_table[cur];
    point = trx_manager->trx_table[cur]->waiting_lock;
    if(!point) {
      visit[cur] = true;
      continue;
    }

    key = point->key;
    point = point->lock_prev;
    while(point) {
      if(point->owner_trx_id==lock_obj->owner_trx_id) {
        UNLOCK(trx_mutex);
        return true;
      }
      if(point->key == key) {
        if(visit.find(point->owner_trx_id) == visit.end())
          Q.push(point->owner_trx_id);
      }
      point = point->lock_prev;
    }
    visit[cur] = true;
  }
  UNLOCK(trx_mutex);
  return false;
}

int 
db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t *val_size, int trx_id)
{
  trx_t*        trx;
  page_t*       header;
  page_t*       page;
  pagenum_t     root_num;
  pagenum_t     page_id;
  int           header_idx;
  int           page_idx;
  int           flag;
  int           key_index;
  uint16_t      size;
  uint16_t      offset;

  if(!isValid_trx(trx_id)) return 1;
  if(!isValid(table_id)) {
    trx_abort(trx_id);
    return 1;
  }
  trx = trx_manager->trx_table[trx_id];
  LOCK(trx->small_trx_mutex);
  header = buffer_read_page(table_id, 0, &header_idx, READ);
  root_num = header->root_num;
  if(!root_num) {
    UNLOCK(trx->small_trx_mutex);
    return 1;
  }

  page_id = find_leaf(table_id, root_num, key);
  page = buffer_read_page(table_id, page_id, &page_idx, READ);
  for(key_index=0; key_index<page->info.num_keys; key_index++) {
    if(page->leafbody.slot[key_index].key == key) break;
  }
  if(key_index == page->info.num_keys) {
    UNLOCK(trx->small_trx_mutex);
    return 1;
  }

  flag = lock_acquire(table_id, page_id, key, trx_id, SHARED);
  if(flag == DEADLOCK) {
    trx_abort(trx_id);
    UNLOCK(trx->small_trx_mutex);
    return 1;
  }

  page = buffer_read_page(table_id, page_id, &page_idx, READ);
  offset = page->leafbody.slot[key_index].offset-128;
  size = page->leafbody.slot[key_index].size;
  for(int i=offset, j=0; i<offset+size; j++, i++)
    ret_val[j] = page->leafbody.value[i];
  *val_size = size;

  UNLOCK(trx->small_trx_mutex);
  return 0;
}

int
db_update(int64_t table_id, int64_t key, char* values, uint16_t new_val_size, uint16_t* old_val_size, int trx_id)
{
  page_t*                 header;
  page_t*                 page;
  int                     header_idx;
  int                     page_idx;
  int                     key_index;
  int                     flag;
  pagenum_t               root_num;
  pagenum_t               page_id;
  trx_t*                  trx;
  trx_t*                  impl_trx;
  entry_t*                entry;
  log_t*                  log;
  log_table_t::iterator   log_it;
  uint16_t                offset;
  uint16_t                size;
  char*                   old_value;
  trx_table_t::iterator   it;

  if(!isValid_trx(trx_id)) return 1;
  if(!isValid(table_id)) {
    trx_abort(trx_id);
    return 1;
  }
  trx = trx_manager->trx_table[trx_id];
  LOCK(trx->small_trx_mutex);
  header = buffer_read_page(table_id, 0, &header_idx, READ);
  root_num = header->root_num;
  if(!root_num) {
    UNLOCK(trx->small_trx_mutex);
    return 1;
  }

  page_id = find_leaf(table_id, root_num, key);
  page = buffer_read_page(table_id, page_id, &page_idx, READ);
  for(key_index=0; key_index<page->info.num_keys; key_index++) {
    if(page->leafbody.slot[key_index].key == key) break;
  }
  if(key_index == page->info.num_keys) {
    UNLOCK(trx->small_trx_mutex);
    return 1;
  }

  flag = lock_acquire(table_id, page_id, key, trx_id, EXCLUSIVE);
  if(flag == DEADLOCK) {
    trx_abort(trx_id);
    UNLOCK(trx->small_trx_mutex);
    return 1;
  }

  page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
  offset = page->leafbody.slot[key_index].offset-128;
  size = page->leafbody.slot[key_index].size;
  *old_val_size = size;
  old_value = new char[size + 1];
  for(int i=offset, j=0; i<offset+size; j++,i++)
    old_value[j] = page->leafbody.value[i];

  for(int i=offset, j=0; i<offset+new_val_size; j++, i++)
    page->leafbody.value[i] = values[j];

  page->leafbody.slot[key_index].size = new_val_size;
  page->leafbody.slot[key_index].trx_id = trx_id;

  buffer_write_page(table_id, page_id, page_idx, 1);

  trx = trx_manager->trx_table[trx_id];
  log_it = trx->log_table.find({table_id, key});
  if(log_it == trx->log_table.end()) {
    log = new log_t;
    log->old_value = new char[size+1];
    log->table_id = table_id;
    log->key = key;
    log->val_size = size;
    for(int k=0; k<size; k++) {
      log->old_value[k] = old_value[k];
    }
    trx->log_table[{table_id, key}] = log;
  }
  UNLOCK(trx->small_trx_mutex);
  return 0;
}

void
append_lock(entry_t* entry, lock_t* lock, trx_t* trx)
{
  lock_t*     tail;

  if(!entry->head) {
    entry->head = entry->tail = lock;
    lock->lock_prev = lock->lock_next = nullptr;
  } else {
    tail = entry->tail;
    tail->lock_next = lock;
    lock->lock_prev = tail;
    entry->tail = lock;
  }
  lock->sent_point = entry;

  lock->trx_next = trx->trx_next;
  trx->trx_next = lock;
}

int
lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, bool lock_mode)
{
  int           cnt;
  entry_t*      entry;
  lock_t*       point;
  lock_t*       tmp;
  lock_t*       tail;
  lock_t*       new_lock;
  lock_t*       my_lock;
  trx_t*        trx;
  bool          mine;
  lock_table_t::iterator lock_it;

  LOCK(lock_mutex);

  trx = trx_manager->trx_table[trx_id];

  new_lock = give_lock(key, trx_id, lock_mode);
  lock_it = lock_manager->lock_table.find({table_id, page_id});

  if(lock_it == lock_manager->lock_table.end()) {
    entry = give_entry(table_id, page_id);
    new_lock->lock_state = ACQUIRED;
    append_lock(entry, new_lock, trx);
    trx->waiting_lock = nullptr;
    lock_manager->lock_table[{table_id, page_id}] = entry;
    UNLOCK(lock_mutex);

    return NORMAL;
  }

  entry = lock_it->second;
  point = entry->head;
  while(point) 
  {
    if(point->key == key) {
      if(point->lock_mode == EXCLUSIVE) {
        if(point->owner_trx_id = trx_id) {
          delete new_lock;
          trx->waiting_lock = nullptr;
          UNLOCK(lock_mutex);
          return NORMAL;
        }
        trx->waiting_lock = new_lock;
        new_lock->lock_state = WAITING;
        append_lock(entry, new_lock, trx);
        if(deadlock_detect(new_lock)) {
          UNLOCK(lock_mutex);
          return DEADLOCK;
        }
        WAIT(new_lock->cond, lock_mutex);
        UNLOCK(lock_mutex);
        return NORMAL;
      }
      
      mine = false;
      point = entry->head;
      while(point) {
        if(point->key == key && point->owner_trx_id == trx_id) {
          my_lock = point;
          mine = true;
          break;
        }
        point = point->lock_next;
      }

      if(mine) {
        if(lock_mode == SHARED) {
          delete new_lock;
          trx->waiting_lock = nullptr;
          UNLOCK(lock_mutex);
          return NORMAL;
        }

        tmp = entry->head;
        cnt = 0;
        while(tmp) {
          if(tmp->key == key) {
            if(tmp->lock_state == ACQUIRED)
              cnt++;
            else {
              trx->waiting_lock = new_lock;
              new_lock->lock_state = WAITING;
              append_lock(entry, new_lock, trx);
              if(deadlock_detect(new_lock)) {
                UNLOCK(lock_mutex);
                return DEADLOCK;
              }
              WAIT(new_lock->cond, lock_mutex);
              UNLOCK(lock_mutex);
              return NORMAL;
            }
          }
          tmp = tmp->lock_next;
        }
        if(cnt==1) {
          delete new_lock;
          my_lock->lock_mode = EXCLUSIVE;
          trx->waiting_lock = nullptr;
          UNLOCK(lock_mutex);
          return NORMAL;
        }
        delete new_lock;
        UNLOCK(lock_mutex);
        return DEADLOCK;
      }

      if(lock_mode == EXCLUSIVE) {
        trx->waiting_lock = new_lock;
        new_lock->lock_state = WAITING;
        append_lock(entry, new_lock, trx);
        if(deadlock_detect(new_lock)) {
          UNLOCK(lock_mutex);
          return DEADLOCK;
        }
        WAIT(new_lock->cond, lock_mutex);
        UNLOCK(lock_mutex);
        return NORMAL;
      }

      tail = entry->tail;
      while(tail) {
        if(tail->lock_state == WAITING && tail->key == key) {
          trx->waiting_lock = new_lock;
          new_lock->lock_state = WAITING;
          append_lock(entry, new_lock, trx);
          if(deadlock_detect(new_lock)) {
            UNLOCK(lock_mutex);
            return DEADLOCK;
          }
          WAIT(new_lock->cond, lock_mutex);
          UNLOCK(lock_mutex);
          return NORMAL;
        }
        tail = tail->lock_prev;
      }

      trx->waiting_lock = nullptr;
      new_lock->lock_state = ACQUIRED;
      append_lock(entry, new_lock, trx);
      UNLOCK(lock_mutex);
      return NORMAL;
    }
    point = point->lock_next;
  }
  trx->waiting_lock = nullptr;
  new_lock->lock_state = ACQUIRED;
  append_lock(entry, new_lock, trx);
  UNLOCK(lock_mutex);
  return NORMAL;
}