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
give_lock(int64_t key, int trx_id, bool lock_mode, int i)
{
  lock_t*                 lock;

  lock = (lock_t*)malloc(sizeof(lock_t));
  lock->lock_prev = lock->lock_next = nullptr;
  lock->trx_next = nullptr;
  lock->sent_point = nullptr;
  lock->cond = PTHREAD_COND_INITIALIZER;
  lock->key = key;
  lock->owner_trx_id = trx_id;
  lock->lock_mode = lock_mode;
  lock->lock_state = WAITING;
  lock->bitmap = MASK(i);

  return lock;
}

entry_t*
give_entry(int64_t table_id, pagenum_t page_id)
{
  entry_t*                entry;

  entry = (entry_t*)malloc(sizeof(entry_t));
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
shutdown_db() {
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
    trx->trx_next = trx->waiting_lock = nullptr;
    trx_manager->trx_table[trx_id] = trx;
  } else {
    UNLOCK(trx_mutex);
    return 0;
  }

  UNLOCK(trx_mutex);

  return trx_id;
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
  UNLOCK(trx_mutex);

  lock_release(trx);
  for(auto log_it = trx->log_table.begin(); log_it != trx->log_table.end(); log_it++) {
    free(log_it->second->old_value);
    free(log_it->second);
  }
  trx->log_table.clear();
  delete trx;
  
  LOCK(trx_mutex);
  trx_manager->trx_table.erase(trx_id);
  UNLOCK(trx_mutex);

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

    free(log->old_value);
    free(log);
    log = nullptr;
  }
  trx->log_table.clear();
  
  lock_release(trx);

  delete trx;
  
  LOCK(trx_mutex);
  trx_manager->trx_table.erase(trx_id);
  UNLOCK(trx_mutex);
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
      free(entry);
      lock_manager->lock_table.erase({table_id, page_id});
      goto CONTINUE;
    }
    if(point->lock_state == WAITING)
      goto CONTINUE;

    if(lock_mode == SHARED) 
    {
      std::unordered_map<int, int> key_map;
      std::unordered_map<int, int>::iterator it;
      uint64_t bit;
      int index;

      bitmap = point->bitmap;
      leaf = buffer_read_page(table_id, page_id, &leaf_idx, READ);
      for(int i=0; i<leaf->info.num_keys; i++) {
        if(MASK(i) & bitmap) 
          key_map[leaf->leafbody.slot[i].key] = 0;
      }

      tmp = entry->head;
      while(tmp) 
      {
        if((tmp->lock_state == ACQUIRED) && (tmp->lock_mode == SHARED)) {
          bit = tmp->bitmap;
          for(int i=0; i<leaf->info.num_keys; i++) {
            if(MASK(i) & bit) 
              key_map[leaf->leafbody.slot[i].key] = 1;
          }
        }
        tmp = tmp->lock_next;
      }

      while(lock) 
      {
        if((lock->lock_state == WAITING) && (lock->lock_mode == EXCLUSIVE)) {
          it = key_map.find(lock->key);
          if(it != key_map.end() && it->second == 0) {
            key_map[lock->key] = 1;
            lock->lock_state = ACQUIRED;
            trx_manager->trx_table[lock->owner_trx_id]->waiting_lock = nullptr;
            SIGNAL(lock->cond);
          }
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
    free(del);
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
  uint64_t                bitmap;

  LOCK(trx_mutex);
  bitmap = lock_obj->bitmap;
  key = lock_obj->key;
  point = lock_obj->lock_prev;

  while(point) {
    if(point->bitmap & bitmap)
      Q.push(point->owner_trx_id);
    point = point->lock_prev;
  }

  while(!Q.empty()) 
  {
    cur = Q.front(); Q.pop();
    if(visit.find(cur) != visit.end()) continue;
    
    point = trx_manager->trx_table[cur]->waiting_lock;
    if(!point) {
      visit[cur] = true;
      continue;
    }

    key = point->key;
    bitmap = point->bitmap;
    point = point->lock_prev;
    while(point) {
      if(point->owner_trx_id==lock_obj->owner_trx_id) {
        UNLOCK(trx_mutex);
        return true;
      }
      if(point->bitmap & bitmap) {
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
  page_t*                 header;
  page_t*                 page;
  pagenum_t               root_num;
  pagenum_t               page_id;
  int                     i;
  int                     header_idx;
  int                     leaf_idx;
  int                     flag;
  uint16_t                offset;
  uint16_t                size;
  trx_table_t::iterator   it;

  LOCK(trx_mutex);
  it = trx_manager->trx_table.find(trx_id);
  if(it == trx_manager->trx_table.end()) {
    UNLOCK(trx_mutex);
    return 1;
  }
  UNLOCK(trx_mutex);

  if(!isValid(table_id)) {
    trx_abort(trx_id);
    return 1;
  }

  header = buffer_read_page(table_id, 0, &header_idx, READ);
  root_num = header->root_num;
  if(!root_num) {
    trx_abort(trx_id);
    return 1;
  }

  page_id = find_leaf(table_id, root_num, key);
  page = buffer_read_page(table_id, page_id, &leaf_idx, WRITE);
  for(i=0; i<page->info.num_keys; i++) {
    if(page->leafbody.slot[i].key == key) break;
  }
  if(i == page->info.num_keys) {
    buffer_write_page(table_id, page_id, leaf_idx, 0);
    trx_abort(trx_id);
    return 1;
  }
  buffer_write_page(table_id, page_id, leaf_idx, 0);

  flag = lock_acquire(table_id, page_id, key, trx_id, SHARED, i);
  if(flag == DEADLOCK) {
    trx_abort(trx_id);
    return 1;
  }
  if(flag == CHECK_IMPL) {
    if(impl_to_expl(table_id, page_id, key, trx_id, SHARED, i) == DEADLOCK) {
      trx_abort(trx_id);
      return 1;
    }
  }

  page = buffer_read_page(table_id, page_id, &leaf_idx, WRITE);
  if(page->leafbody.slot[i].key != key) {
    buffer_write_page(table_id, page_id, leaf_idx, 0);
    trx_abort(trx_id);
    return 1;
  }

  offset = page->leafbody.slot[i].offset-128;
  size = page->leafbody.slot[i].size;
  for(int k=offset; k<offset+size; k++) {
    ret_val[k-offset] = page->leafbody.value[k];
  }
  *val_size = size;
  buffer_write_page(table_id, page_id, leaf_idx, 0);

  return 0;
}

int
db_update(int64_t table_id, int64_t key, char* values, uint16_t new_val_size, uint16_t* old_val_size, int trx_id)
{
  page_t*                 header;
  page_t*                 page;
  int                     header_idx;
  int                     leaf_idx;
  int                     i;
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

  LOCK(trx_mutex);
  it = trx_manager->trx_table.find(trx_id);
  if(it == trx_manager->trx_table.end()) {
    UNLOCK(trx_mutex);
    return 1;
  }
  UNLOCK(trx_mutex);

  if(!isValid(table_id)) {
    trx_abort(trx_id);
    return 1;
  }

  header = buffer_read_page(table_id, 0, &header_idx, READ);
  root_num = header->root_num;
  if(!root_num) {
    trx_abort(trx_id);
    return 1;
  }

  page_id = find_leaf(table_id, root_num, key);
  page = buffer_read_page(table_id, page_id, &leaf_idx, WRITE);
  for(i=0; i<page->info.num_keys; i++) {
    if(page->leafbody.slot[i].key == key) break;
  }
  if(i == page->info.num_keys) {
    buffer_write_page(table_id, page_id, leaf_idx, 0);
    trx_abort(trx_id);
    return 1;
  }
  buffer_write_page(table_id, page_id, leaf_idx, 0);

  
  flag = lock_acquire(table_id, page_id, key, trx_id, EXCLUSIVE, i);
  if(flag == DEADLOCK) {
    trx_abort(trx_id);
    return 1;
  }
  if(flag == CHECK_IMPL) {
    if(impl_to_expl(table_id, page_id, key, trx_id, EXCLUSIVE, i) == DEADLOCK) {
      trx_abort(trx_id);
      return 1;
    }
  }

  page = buffer_read_page(table_id, page_id, &leaf_idx, WRITE);
  if(page->leafbody.slot[i].key != key) {
    buffer_write_page(table_id, page_id, leaf_idx, 0);
    trx_abort(trx_id);
    return 1;
  }

  offset = page->leafbody.slot[i].offset-128;
  size = page->leafbody.slot[i].size;

  *old_val_size = size;
  old_value = (char*)malloc(sizeof(char) * size + 1);
  for(int k = offset; k<offset+size; k++)
    old_value[k-offset] = page->leafbody.value[k];

  for(int k = offset; k<offset+new_val_size; k++)
    page->leafbody.value[k] = values[k-offset];

  page->leafbody.slot[i].size = new_val_size;
  page->leafbody.slot[i].trx_id = trx_id;

  buffer_write_page(table_id, page_id, leaf_idx, 1);

  trx = trx_manager->trx_table[trx_id];
  log_it = trx->log_table.find({table_id, key});
  if(log_it == trx->log_table.end()) {
    log = (log_t*)malloc(sizeof(log_t));
    log->old_value = (char*)malloc(sizeof(char) * (size+1));
    log->table_id = table_id;
    log->key = key;
    log->val_size = size;
    for(int k=0; k<size; k++) {
      log->old_value[k] = old_value[k];
    }
    trx->log_table[{table_id, key}] = log;
  }

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
  lock->trx_next = trx->trx_next;
  trx->trx_next = lock;
}

int
lock_append_wait(entry_t* entry, lock_t* lock, trx_t* trx)
{
  append_lock(entry, lock, trx);
  trx->waiting_lock = lock;
  lock->lock_state = WAITING;
  if(deadlock_detect(lock)) {
    UNLOCK(lock_mutex);
    return DEADLOCK;
  }
  WAIT(lock->cond, lock_mutex);
  UNLOCK(lock_mutex);
  return NORMAL;
}

int
lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, bool lock_mode, int index)
{
  int                     count;
  bool                    mine;
  page_t*                 leaf;
  int                     leaf_idx;
  uint64_t                mask;
  uint64_t                bit;
  lock_t*                 point;
  lock_t*                 front;
  lock_t*                 tail;
  entry_t*                entry;
  lock_t*                 lock;
  trx_t*                  trx;
  lock_table_t::iterator  it;

  LOCK(lock_mutex);
  
  it = lock_manager->lock_table.find({table_id, page_id});
  if(it == lock_manager->lock_table.end()) {
    UNLOCK(lock_mutex);
    return CHECK_IMPL;
  }

  trx = trx_manager->trx_table[trx_id];
  mask = MASK(index);
  entry = it->second;
  lock = give_lock(key, trx_id, lock_mode, index);
  lock->sent_point = entry;

  point = entry->head;
  while(point) 
  {
    if(point->bitmap & mask) 
    {
      if(point->lock_mode == EXCLUSIVE) {
        if(point->owner_trx_id == trx_id) {
          free(lock);
          UNLOCK(lock_mutex);
          return NORMAL;
        }
        return lock_append_wait(entry, lock, trx);
      }

      else
      {
        front = entry->head;
        tail = entry->tail;
        mine = false;
        while(front) {
          if((front->owner_trx_id == trx_id) && (front->bitmap & mask)) {
            mine = true;
            break;
          }
          front = front->lock_next;
        }

        if(mine)
        {
          if(lock_mode == SHARED) {
            free(lock);
            UNLOCK(lock_mutex);
            return NORMAL;
          }
          
          count = 0;
          while(tail && count<3) {
            if(tail->bitmap & mask) {
              if(tail->lock_state == WAITING) {
                count = 2;
                break;
              }
              count++;
            }
            tail = tail->lock_prev;
          }

          if(count==1) 
          {
            free(lock);
            front->bitmap &= ~mask;
            if(!front->bitmap) 
            {
              lock_t*     tmp_lock;
              lock_t*     prev_lock;

              if(entry->head == front) entry->head = front->lock_next;
              if(entry->tail == front) entry->tail = front->lock_prev;
              if(front->lock_prev) front->lock_prev->lock_next = front->lock_next;
              if(front->lock_next) front->lock_next->lock_prev = front->lock_prev;
              if(!entry->head) {
                free(entry);
                lock_manager->lock_table.erase({table_id, page_id});
              }

              prev_lock = nullptr;
              tmp_lock = trx->trx_next;
              while(tmp_lock!=front) {
                prev_lock = tmp_lock;
                tmp_lock = tmp_lock->trx_next;
              }
              if(!prev_lock) trx->trx_next = front->trx_next;
              else prev_lock->trx_next = front->trx_next;

              free(front);
            }
            else 
            {
              leaf = buffer_read_page(table_id, entry->page_id, &leaf_idx, READ);
              if(front->key == key) {
                int i = 63;
                bit = front->bitmap;
                while(bit && i) {
                  if(bit & 1) {
                    front->key = leaf->leafbody.slot[i].key;
                    break;
                  }
                  bit>>=1; i--;
                }
              }
            }
            trx->waiting_lock = nullptr;
            UNLOCK(lock_mutex);
            return NORMAL;
          }
          free(lock);
          UNLOCK(lock_mutex);
          return DEADLOCK;
        }

        // not mine.
        while(tail) {
          if((tail->lock_state == WAITING) && (tail->bitmap & mask)) 
            return lock_append_wait(entry, lock, trx);
          tail = tail->lock_prev;
        }
        if(lock_mode == SHARED) {
          front = entry->head;
          while(front) {
            if(front->owner_trx_id == trx_id && front->lock_mode == SHARED) {
              free(lock);
              front->bitmap |= mask;
              UNLOCK(lock_mutex);
              return NORMAL;
            }
            front = front->lock_next;
          }
        }
        if(lock_mode == SHARED) {
          append_lock(entry, lock, trx);
          trx->waiting_lock = nullptr;
          lock->lock_state = ACQUIRED;
          UNLOCK(lock_mutex);
          return NORMAL;
        } 
        return lock_append_wait(entry, lock, trx);
      }
    }
    point = point->lock_next;
  }
  free(lock);
  UNLOCK(lock_mutex);
  return CHECK_IMPL;
}

bool 
impl_to_expl(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, bool lock_mode, int i)
{
  entry_t*                entry;
  lock_t*                 impl_lock;
  lock_t*                 lock;
  lock_t*                 point;
  trx_t*                  trx;
  page_t*                 page;
  int                     leaf_idx;
  int                     impl_trx_id;
  uint64_t                mask;
  trx_table_t::iterator   it;
  lock_table_t::iterator  lock_it;

  mask = MASK(i);
  page = buffer_read_page(table_id, page_id, &leaf_idx, WRITE);
  if(page->leafbody.slot[i].key != key) {
    buffer_write_page(table_id, page_id, leaf_idx, 0);
    return DEADLOCK;
  }
  
  LOCK(trx_mutex);
  impl_trx_id = page->leafbody.slot[i].trx_id;
  if(impl_trx_id == trx_id) {
    UNLOCK(trx_mutex);
    buffer_write_page(table_id, page_id, leaf_idx, 0);
    return NORMAL;
  }
  
  it = trx_manager->trx_table.find(impl_trx_id);
  buffer_write_page(table_id, page_id, leaf_idx, 0);
  if(it == trx_manager->trx_table.end()) {
    UNLOCK(trx_mutex);
    if(lock_mode == SHARED) 
    {
      LOCK(lock_mutex);
      trx = trx_manager->trx_table[trx_id];
      trx->waiting_lock = nullptr;

      lock_it = lock_manager->lock_table.find({table_id, page_id});
      lock = give_lock(key, trx_id, SHARED, i);
      if(lock_it == lock_manager->lock_table.end()) {
        entry = give_entry(table_id, page_id);
        lock_manager->lock_table[{table_id, page_id}] = entry;
      } else {
        entry = lock_it->second;
        point = entry->head;
        while(point) 
        {
          if((point->lock_mode == SHARED) && (point->owner_trx_id == trx_id)) {
            free(lock);
            point->bitmap |= mask;
            UNLOCK(lock_mutex);
            return NORMAL;
          }
          point = point->lock_next;
        }
      }
      lock->sent_point = entry;
      lock->lock_state = ACQUIRED;
      append_lock(entry, lock, trx);
      UNLOCK(lock_mutex);   
    }
    return NORMAL;
  }
  
  LOCK(lock_mutex);
  lock_it = lock_manager->lock_table.find({table_id, page_id});
  if(lock_it == lock_manager->lock_table.end()) {
      entry = give_entry(table_id, page_id);
      lock_manager->lock_table[{table_id, page_id}] = entry;
  }
  else entry = lock_it->second;

  impl_lock = give_lock(key, impl_trx_id, EXCLUSIVE, i);
  impl_lock->lock_state = ACQUIRED;
  impl_lock->sent_point = entry;
  append_lock(entry, impl_lock, it->second);
  UNLOCK(trx_mutex);

  trx = trx_manager->trx_table[trx_id];
  lock = give_lock(key, trx_id, lock_mode, i);
  lock->sent_point = entry;

  return lock_append_wait(entry, lock, trx);
}