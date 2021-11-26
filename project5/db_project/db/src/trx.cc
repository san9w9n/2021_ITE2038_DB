#include "trx.h"

#pragma GCC optimize("O3")

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
  int                     trx_id;

  LOCK(trx_mutex);
  trx_id = ++trx_manager->trx_cnt;
  trx = new trx_t();
  trx->state = ACTIVE;
  trx->trx_next = trx->waiting_lock = nullptr;
  trx_manager->trx_table.push_back(trx);
  UNLOCK(trx_mutex);

  return trx_id;
}

trx_t*
give_trx(int trx_id) 
{
  LOCK(trx_mutex);
  if(trx_id>trx_manager->trx_table.size()) {
    UNLOCK(trx_mutex);
    return nullptr;
  }
  if(trx_manager->trx_table[trx_id-1]->state != ACTIVE) {
    UNLOCK(trx_mutex);
    return nullptr;
  }
  UNLOCK(trx_mutex);
  return trx_manager->trx_table[trx_id-1];
}

int
trx_commit(int trx_id)
{
  trx_t*                  trx;
  trx_table_t::iterator   it;

  if(!(trx = give_trx(trx_id))) return 0;

  lock_release(trx);

  // for(auto log_it = trx->log_table.begin(); log_it != trx->log_table.end(); log_it++) {
  //   delete[] log_it->second->old_value;
  //   delete log_it->second;
  // }
  // trx->log_table.clear();
  trx->state = COMMIT;

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


  trx = trx_manager->trx_table[trx_id-1];
  // for(auto log_it = trx->log_table.begin(); log_it != trx->log_table.end(); log_it++) 
  // {
  //   log = log_it->second;

  //   table_id = log->table_id;
  //   key = log->key;

  //   header_page = buffer_read_page(table_id, 0, &header_idx, READ);
  //   root_num = header_page->root_num;
  //   page_id = find_leaf(table_id, root_num, key);
  //   leaf_page = buffer_read_page(table_id, page_id, &leaf_idx, WRITE);
  //   for(i=0; i<leaf_page->info.num_keys; i++) {
  //     if(leaf_page->leafbody.slot[i].key == key) break;
  //   }

  //   size = log->val_size;
  //   leaf_page->leafbody.slot[i].size = size;
  //   offset = leaf_page->leafbody.slot[i].offset-128;
  //   for(int k=offset; k<offset+size; k++) {
  //     leaf_page->leafbody.value[k] = log->old_value[k-offset];
  //   }
  //   buffer_write_page(table_id, page_id, leaf_idx, 1);

  //   delete[] log->old_value;
  //   delete log;
  //   log = nullptr;
  // }
  // trx->log_table.clear();
  lock_release(trx);
  trx->state = ABORT;
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
        if((tmp->key == key)) {
          if((tmp->lock_state == ACQUIRED) && (tmp->lock_mode == SHARED)) {
            cnt++;
            break;
          }
          else if((tmp->lock_mode == EXCLUSIVE)) {
            if((tmp->key == key) && (tmp->lock_mode == EXCLUSIVE)) {
              SIGNAL(tmp->cond);
              break;
            }
          }
        }
        tmp = tmp->lock_next;
      }
    }

    else 
    {
      flag = 0;
      while(lock) {
        if(lock->key == key) {
          if(lock->lock_mode == SHARED) {
            flag = 1;
            SIGNAL(lock->cond);
          }
          else {
            if(!flag) 
              SIGNAL(lock->cond);
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
  lock_t*             lock;
  lock_t*             head;
  entry_t*            entry;
  trx_t*              trx;
  int64_t             key;
  int                 slower;
  int                 faster;
  int                 trx_id;
  bool                flag;

  LOCK(trx_mutex);
  std::vector<int> edge(trx_manager->trx_cnt + 1);

  for(int i=0; i<trx_manager->trx_cnt; i++) {
    trx = trx_manager->trx_table[i];
    if(trx->state != ACTIVE) continue;
    if(!(lock = trx->waiting_lock)) continue;
    key = lock->key;
    entry = lock->sent_point;
    head = entry->head;
    while(head && head->key!=key)
      head = head->lock_next;
    edge[lock->owner_trx_id] = head->owner_trx_id;
  }

  flag = false;
  slower = faster = lock_obj->owner_trx_id;
  while (slower && faster && edge[faster]) {
      slower = edge[slower];
      faster = edge[edge[faster]];
      if(slower == faster) {
        flag = true;
        break;
      }
  }
  UNLOCK(trx_mutex);
  return flag;
}

int 
db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t *val_size, int trx_id)
{
  trx_t*        trx;
  page_t*       header;
  page_t*       page;
  pagenum_t     page_id;
  int           header_idx;
  int           page_idx;
  int           flag;
  int           key_index;
  uint16_t      size;
  uint16_t      offset;

  if(!isValid(table_id))
    return 1;

  if(!(trx = give_trx(trx_id))) return 1;

  header = buffer_read_page(table_id, 0, &header_idx, READ);
  page_id = header->root_num;
  if(!page_id) return 1;

  page = buffer_read_page(table_id, page_id, &page_idx, READ);
  while(!page->info.isLeaf) {
    if(key<page->branch[0].key) page_id = page->leftmost;
    else {
      uint32_t i;
      for(i=0; i<page->info.num_keys-1; i++) {
        if(key<page->branch[i+1].key) break;
      }
      page_id = page->branch[i].pagenum;
    }
    page = buffer_read_page(table_id, page_id, &page_idx, READ);
  }
  
  for(key_index=0; key_index<page->info.num_keys; key_index++) {
    if(page->leafbody.slot[key_index].key == key) break;
  }
  if(key_index == page->info.num_keys) return 1;

  flag = lock_acquire(table_id, page_id, key, trx_id, SHARED);
  if(flag == DEADLOCK) {
    trx_abort(trx_id);
    return 1;
  }

  page = buffer_read_page(table_id, page_id, &page_idx, READ);
  offset = page->leafbody.slot[key_index].offset-128;
  size = page->leafbody.slot[key_index].size;
  for(int i=offset, j=0; i<offset+size; j++, i++)
    ret_val[j] = page->leafbody.value[i];
  *val_size = size;

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

  if(!isValid(table_id))
    return 1;
  if(!(trx = give_trx(trx_id))) 
    return 1;

  header = buffer_read_page(table_id, 0, &header_idx, READ);
  page_id = header->root_num;
  if(!page_id) return 1;

  page = buffer_read_page(table_id, page_id, &page_idx, READ);
  while(!page->info.isLeaf) {
    if(key<page->branch[0].key) page_id = page->leftmost;
    else {
      uint32_t i;
      for(i=0; i<page->info.num_keys-1; i++) {
        if(key<page->branch[i+1].key) break;
      }
      page_id = page->branch[i].pagenum;
    }
    page = buffer_read_page(table_id, page_id, &page_idx, READ);
  }
  
  for(key_index=0; key_index<page->info.num_keys; key_index++) {
    if(page->leafbody.slot[key_index].key == key) break;
  }
  if(key_index == page->info.num_keys) return 1;
  

  flag = lock_acquire(table_id, page_id, key, trx_id, EXCLUSIVE);
  if(flag == DEADLOCK) {
    trx_abort(trx_id);
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

  // log_it = trx->log_table.find({table_id, key});
  // if(log_it == trx->log_table.end()) {
  //   log = new log_t;
  //   log->old_value = new char[size+1];
  //   log->table_id = table_id;
  //   log->key = key;
  //   log->val_size = size;
  //   for(int k=0; k<size; k++) {
  //     log->old_value[k] = old_value[k];
  //   }
  //   trx->log_table[{table_id, key}] = log;
  // }
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
  bool          find_wait;
  lock_table_t::iterator lock_it;

  LOCK(lock_mutex);

  trx = trx_manager->trx_table[trx_id-1];

  new_lock = give_lock(key, trx_id, lock_mode);
  lock_it = lock_manager->lock_table.find({table_id, page_id});
  if(lock_it == lock_manager->lock_table.end()) {
    entry = give_entry(table_id, page_id);
    new_lock->lock_state = ACQUIRED;
    trx->waiting_lock = nullptr;
    append_lock(entry, new_lock, trx);
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
        if(point->owner_trx_id == trx_id) {
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
        new_lock->lock_state = ACQUIRED;
        trx->waiting_lock = nullptr;
        UNLOCK(lock_mutex);
        return NORMAL;
      }
      
      cnt=0; mine=false; find_wait=false;
      point = entry->head;
      while(point) {
        if(point->key == key) {
          if(point->owner_trx_id == trx_id) {
            my_lock = point;
            mine = true;
            if(lock_mode == SHARED) {
              delete new_lock;
              trx->waiting_lock = nullptr;
              UNLOCK(lock_mutex);
              return NORMAL;
            }
          }
          else if(point->lock_state == WAITING) {
            find_wait = true;
            cnt++;
          }
          else cnt++;
        }
        point = point->lock_next;
      }

      if(mine) {
        delete new_lock;
        if(find_wait) {
          trx->waiting_lock = nullptr;
          UNLOCK(lock_mutex);
          return DEADLOCK;
        }
        if(!cnt) {
          trx->waiting_lock = nullptr;
          my_lock->lock_state = ACQUIRED;
          my_lock->lock_mode = EXCLUSIVE;
          UNLOCK(lock_mutex);
          return NORMAL;
        }

        if(entry->head == my_lock) entry->head = my_lock->lock_next;
        if(entry->tail == my_lock) entry->tail = my_lock->lock_prev;
        if(my_lock->lock_next) my_lock->lock_next->lock_prev = my_lock->lock_prev;
        if(my_lock->lock_prev) my_lock->lock_prev->lock_next = my_lock->lock_next;

        if(!entry->head) {
          entry->head = entry->tail = my_lock;
          my_lock->lock_next = my_lock->lock_prev = nullptr;
        } else {
          entry->tail->lock_next = my_lock;
          my_lock->lock_prev = entry->tail;
          my_lock->lock_next = nullptr;
          entry->tail = my_lock;
        }

        trx->waiting_lock = my_lock;
        my_lock->lock_mode = EXCLUSIVE;
        my_lock->lock_state = WAITING;
        if(deadlock_detect(my_lock)) {
          UNLOCK(lock_mutex);
          return DEADLOCK;
        }
        WAIT(my_lock->cond, lock_mutex);
        my_lock->lock_state = ACQUIRED;
        trx->waiting_lock = nullptr;
        UNLOCK(lock_mutex);
        return NORMAL;
      }

      if((!find_wait) && (lock_mode == SHARED)) {
        new_lock->lock_state = ACQUIRED;
        trx->waiting_lock = nullptr;
        append_lock(entry, new_lock, trx);
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
      new_lock->lock_state = ACQUIRED;
      trx->waiting_lock = nullptr;
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