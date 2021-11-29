#include "trx.h"

#pragma gcc optimization("O3");
#pragma gcc optimization("unroll-loops");

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
#define BROADCAST(X) (pthread_cond_broadcast(&(X)))

lock_table_t lock_table;
trx_table_t trx_table;
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
  lock_mutex = PTHREAD_MUTEX_INITIALIZER;
  trx_mutex = PTHREAD_MUTEX_INITIALIZER;
  return init_buffer(num_buf);
}

int 
shutdown_db() 
{
  lock_table.clear();
  for(int i=0; i<trx_table.size(); i++)
    delete trx_table[i];
  trx_table.clear();
  return shutdown_buffer();
}

int
trx_begin(void)
{
  trx_t*                  trx;
  int                     trx_id;

  LOCK(trx_mutex);
  trx = new trx_t;
  trx->state = ACTIVE;
  trx->trx_next = nullptr;
  trx->wait_trx_id = 0;
  trx_table.push_back(trx);
  trx_id = trx_table.size();
  UNLOCK(trx_mutex);

  return trx_id;
}

trx_t*
give_trx(int trx_id) 
{
  LOCK(trx_mutex);
  if(trx_id>trx_table.size()) {
    UNLOCK(trx_mutex);
    return nullptr;
  }
  if(trx_table[trx_id-1]->state != ACTIVE) {
    UNLOCK(trx_mutex);
    return nullptr;
  }
  UNLOCK(trx_mutex);
  return trx_table[trx_id-1];
}

int
trx_commit(int trx_id)
{
  trx_t*                  trx;
  trx_table_t::iterator   it;

  LOCK(lock_mutex);

  if(trx_table[trx_id-1]->state != ACTIVE)
    return 0;
  trx = trx_table[trx_id-1];
  trx->wait_trx_id = 0;
  for(auto log_it = trx->log_table.begin(); log_it != trx->log_table.end(); log_it++) {
    delete[] log_it->second->old_value;
    delete log_it->second;
  }
  trx->log_table.clear();
  trx->state = COMMIT;

  
  lock_release(trx);

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

  trx = trx_table[trx_id-1];
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
  trx->wait_trx_id = 0;
  trx->log_table.clear();
  trx->state = ABORT;

  lock_release(trx);
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
  page_t*                 leaf;

  point = trx->trx_next;
  while(point) 
  {
    entry = point->sent_point;

    if(entry->head == point) entry->head = point->lock_next;
    if(entry->tail == point) entry->tail = point->lock_prev;
    if(point->lock_next) point->lock_next->lock_prev = point->lock_prev;
    if(point->lock_prev) point->lock_prev->lock_next = point->lock_next;

    BROADCAST(point->cond);
    if(!entry->head) {
      lock_table.erase({entry->table_id, entry->page_id});
      delete entry;
    }
    del = point;
    point = point->trx_next;
    delete del;
  }

  UNLOCK(lock_mutex);

  return 0;
}

bool
deadlock_detect(int trx_id) 
{
  bool                flag;
  int                 target_id;

  LOCK(trx_mutex);
  std::vector<int> visit(trx_table.size() + 2);
  visit[trx_id] = 1;
  target_id = trx_table[trx_id-1]->wait_trx_id;
  
  while(1) {
    if(!target_id) {
      flag = false;
      break;
    }
    if(visit[target_id]) {
      flag = true;
      if(target_id!=trx_id)
        flag = false;
      break;
    }
    visit[target_id] = 1;
    target_id = trx_table[target_id-1]->wait_trx_id;
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
  pagenum_t     root_num;
  pagenum_t     page_id;
  int           header_idx;
  int           page_idx;
  int           flag;
  int           key_index;
  uint16_t      size;
  uint16_t      offset;

  if(!isValid(table_id))
    return 1;
  if(!(trx = give_trx(trx_id)))
    return 1;

  header = buffer_read_page(table_id, 0, &header_idx, READ);
  root_num = header->root_num;
  if(!root_num)
    return 1;

  page_id = find_leaf(table_id, root_num, key);
  page = buffer_read_page(table_id, page_id, &page_idx, READ);
  for(key_index=0; key_index<page->info.num_keys; key_index++) {
    if(page->leafbody.slot[key_index].key == key) break;
  }
  if(key_index == page->info.num_keys)
    return 1;

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
  root_num = header->root_num;
  if(!root_num) return 1;

  page_id = find_leaf(table_id, root_num, key);
  page = buffer_read_page(table_id, page_id, &page_idx, READ);
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

  trx = trx_table[trx_id-1];
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
  delete[] old_value;
  
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
lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, bool lock_mode)
{
  entry_t*      entry;
  lock_t*       point;
  lock_t*       new_lock;
  trx_t*        trx;
  bool          reacquire;
  bool          conflict;
  lock_table_t::iterator lock_it;

  LOCK(lock_mutex);

  trx = trx_table[trx_id-1];
  new_lock = give_lock(key, trx_id, lock_mode);
  lock_it = lock_table.find({table_id, page_id});
  if(lock_it == lock_table.end()) {
    entry = give_entry(table_id, page_id);
    new_lock->sent_point = entry;
    trx->wait_trx_id = 0;
    append_lock(entry, new_lock, trx);
    lock_table[{table_id, page_id}] = entry;
    UNLOCK(lock_mutex);
    return NORMAL;
  }
  entry = lock_it->second;
  new_lock->sent_point = entry;
  

  conflict = false;
  point = entry->head;
  while(point) 
  {
    if((point->key == key)) 
    { 
      if(point->owner_trx_id == trx_id) {
        if(point->lock_mode>=lock_mode) {
          delete new_lock;
          trx->wait_trx_id = 0;
          UNLOCK(lock_mutex);
          return NORMAL;
        }
      } 
      else if(!conflict && (point->lock_mode == EXCLUSIVE || lock_mode == EXCLUSIVE))
        conflict = true;
    } 
    point = point->lock_next;
  }

  append_lock(entry, new_lock, trx);
  point = entry->head;
  while(conflict) {
    if(point == new_lock) conflict = false;
    if((point->key == key) && (point->owner_trx_id != trx_id))
    {
      if(point->lock_mode == EXCLUSIVE || lock_mode == EXCLUSIVE) {
        trx->wait_trx_id = point->owner_trx_id;
        if(deadlock_detect(trx_id)) {
          return DEADLOCK;
        }
        WAIT(point->cond, lock_mutex);
        trx->wait_trx_id = 0;
        point = entry->head;
        continue;
      }
    }
    point = point->lock_next;
  } 
  trx->wait_trx_id = 0;
  UNLOCK(lock_mutex);
  return NORMAL;
}