#include "trx.h"

lock_table_t lock_table;
trx_table_t trx_table;
pthread_mutex_t lock_mutex;
pthread_mutex_t trx_mutex;

lock_t* 
give_lock(int64_t key, uint64_t bitmap, int trx_id, bool lock_mode)
{
  lock_t*                 lock;

  lock = new lock_t;
  lock->lock_prev = lock->lock_next = nullptr;
  lock->trx_next = nullptr;
  lock->sent_point = nullptr;
  lock->cond = PTHREAD_COND_INITIALIZER;
  lock->key = key;
  lock->bitmap = bitmap;
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
init_trx(int num_buf)
{
  lock_mutex = PTHREAD_MUTEX_INITIALIZER;
  trx_mutex = PTHREAD_MUTEX_INITIALIZER;
  return init_buffer(num_buf);
}

int 
shutdown_trx() 
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

  LOCK(trx_mutex);
  if(trx_table[trx_id-1]->state != ACTIVE) {
    UNLOCK(trx_mutex);
    return 0;
  }
  trx = trx_table[trx_id-1];
  trx->wait_trx_id = 0;
  trx->state = COMMIT;
  UNLOCK(trx_mutex);

  for(auto log_it = trx->log_table.begin(); log_it != trx->log_table.end(); log_it++) {
    delete[] log_it->second->old_value;
    delete log_it->second;
  }
  trx->log_table.clear();

  lock_release(trx);
  return trx_id;
}

void
trx_abort(int trx_id)
{
  int                     page_idx;
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
  page_t*                 page;
  page_t*                 header;
  int                     header_idx;
  char*                   old_value;
  trx_table_t::iterator   it;
  log_t*                  log;

  LOCK(trx_mutex);
  trx = trx_table[trx_id-1];
  trx->wait_trx_id = 0;
  trx->state = ABORT;
  UNLOCK(trx_mutex);
  for(auto log_it = trx->log_table.begin(); log_it != trx->log_table.end(); log_it++) 
  {
    log = log_it->second;

    table_id = log->table_id;
    key = log->key;

    header = buffer_read_page(table_id, 0, &header_idx, READ);
    page_id = header->root_num;
    page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
    while(!page->info.isLeaf) {
      buffer_write_page(table_id, page_id, page_idx, 0);
      if(key<page->branch[0].key) page_id = page->leftmost;
      else {
        uint32_t i=0;
        for(i=0; i<page->info.num_keys-1; i++) 
          if(key<page->branch[i+1].key) break;
        page_id = page->branch[i].pagenum;
      }
      page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
    }

    size = log->val_size;
    page->leafbody.slot[i].size = size;
    offset = page->leafbody.slot[i].offset-128;
    for(int k=offset; k<offset+size; k++)
      page->leafbody.value[k] = log->old_value[k-offset];
    buffer_write_page(table_id, page_id, page_idx, 1);

    delete[] log->old_value;
    delete log;
    log = nullptr;
  }
  trx->log_table.clear();

  lock_release(trx);
}

int 
lock_release(trx_t* trx)
{
  lock_t*                 point;
  lock_t*                 lock;
  lock_t*                 del;
  entry_t*                entry;
  int                     lock_mode;
  int                     flag;
  int                     leaf_idx;
  int                     trx_id;
  int                     cnt;
  page_t*                 leaf;

  LOCK(lock_mutex);

  point = trx->trx_next;
  trx_id = point->owner_trx_id;
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


bool
lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int kindex, int trx_id, bool lock_mode)
{
  lock_table_t::iterator  lock_it;
  page_t*                 page;
  entry_t*                entry;
  lock_t*                 point;
  lock_t*                 new_lock;
  lock_t*                 comp_Slock;
  lock_t*                 conflict_lock;
  lock_t*                 impl_lock;
  trx_t*                  trx;
  trx_t*                  impl_trx;
  uint64_t                bitmap;
  int                     page_idx;
  int                     impl_trx_id;
  bool                    other_Slock;
  bool                    conflict;
  bool                    my_SX;
  bool                    my_impl;
  bool                    no_impl;
  
  LOCK(lock_mutex);

  my_impl = no_impl = false;
  conflict_lock = nullptr;
  conflict = false;
  trx = trx_table[trx_id-1];
  bitmap = MASK(kindex);
  new_lock = give_lock(key, bitmap, trx_id, lock_mode);

  lock_it = lock_table.find({table_id, page_id});
  if(lock_it == lock_table.end()) {
    entry = give_entry(table_id, page_id);
    new_lock->sent_point = entry;
    lock_table[{table_id, page_id}] = entry;
  } 
  else entry = lock_it->second;
  new_lock->sent_point = entry;
  
  if(lock_mode == SHARED) {
    comp_Slock = nullptr;
    other_Slock = false;
    point = entry->head;
    while(point) 
    {
      if(point->bitmap & bitmap) { 
        if(point->owner_trx_id == trx_id) {
          delete new_lock;
          trx->wait_trx_id = 0;
          UNLOCK(lock_mutex);
          return NORMAL;
        }
        if(point->lock_mode == EXCLUSIVE) {
          conflict = true;
          conflict_lock = point;
          break;
        } 
        else other_Slock = true;
      } 
      else if((point->owner_trx_id == trx_id) && (point->lock_mode == SHARED))
        comp_Slock = point;
      point = point->lock_next;
    }

    if(!conflict) {
      if(other_Slock) {
        if(comp_Slock) {
          delete new_lock;
          trx->wait_trx_id = 0;
          comp_Slock->bitmap |= bitmap;
          UNLOCK(lock_mutex);
          return NORMAL;
        }
        trx->wait_trx_id = 0;
        append_lock(entry, new_lock, trx);
        UNLOCK(lock_mutex);
        return NORMAL;
      }

      page = buffer_read_page(table_id, page_id, &page_idx, READ);
      impl_trx_id = page->leafbody.slot[kindex].trx_id;

      LOCK(trx_mutex);
      if(impl_trx_id == trx_id) 
        my_impl = true;
      else if((!impl_trx_id) || (trx_table[impl_trx_id-1]->state != ACTIVE))
        no_impl = true;
      UNLOCK(trx_mutex);

      if(my_impl) {
        delete new_lock;
        trx->wait_trx_id = 0;
        UNLOCK(lock_mutex);
        return NORMAL;
      }
      if(no_impl) {
        if(comp_Slock) {
          delete new_lock;
          trx->wait_trx_id = 0;
          comp_Slock->bitmap |= bitmap;
          UNLOCK(lock_mutex);
          return NORMAL;
        }
        trx->wait_trx_id = 0;
        append_lock(entry, new_lock, trx);
        UNLOCK(lock_mutex);
        return NORMAL;
      }

      impl_lock = give_lock(key, bitmap, impl_trx_id, EXCLUSIVE);
      impl_trx = trx_table[impl_trx_id-1];
      impl_lock->sent_point = entry;
      append_lock(entry, impl_lock, impl_trx);

      conflict_lock = impl_lock;
    }

    append_lock(entry, new_lock, trx);
    point = conflict_lock;
    do {
      if((point->bitmap & bitmap) && (point->lock_mode == EXCLUSIVE)) {
        trx->wait_trx_id = point->owner_trx_id;
        if(deadlock_detect(trx_id)) {
          UNLOCK(lock_mutex);
          return DEADLOCK;
        }
        WAIT(point->cond, lock_mutex);
        trx->wait_trx_id = 0;
        point = entry->head;
        continue;
      }
      point = point->lock_next;
    } while(point != new_lock);
    trx->wait_trx_id = 0;
    UNLOCK(lock_mutex);
    return NORMAL;
  }

  // lock_mode == X mode
  my_SX = false;
  point = entry->head;
  while(point) 
  {
    if(point->bitmap & bitmap)
    { 
      if(point->owner_trx_id == trx_id) {
        if(point->lock_mode == EXCLUSIVE) {
          delete new_lock;
          trx->wait_trx_id = 0;
          UNLOCK(lock_mutex);
          return NORMAL;
        } 
        else my_SX = true;
      } else {
        if(!conflict) conflict_lock = point;
        conflict = true;
        break;
      }
    } 
    point = point->lock_next;
  }

  if(!conflict) {
    if(my_SX) {
      append_lock(entry, new_lock, trx);
      trx->wait_trx_id = 0;
      UNLOCK(lock_mutex);
      return NORMAL;
    }

    page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
    impl_trx_id = page->leafbody.slot[kindex].trx_id;

    LOCK(trx_mutex);
    if(impl_trx_id == trx_id) 
      my_impl = true;
    else if((!impl_trx_id) || (trx_table[impl_trx_id-1]->state != ACTIVE))
      no_impl = true;
    UNLOCK(trx_mutex);

    if(my_impl) {
      buffer_write_page(table_id, page_id, page_idx, 0);
      delete new_lock;
      trx->wait_trx_id = 0;
      UNLOCK(lock_mutex);
      return NORMAL;
    }
    if(no_impl) {
      page->leafbody.slot[kindex].trx_id = trx_id;
      buffer_write_page(table_id, page_id, page_idx, 1);
      trx->wait_trx_id = 0;
      append_lock(entry, new_lock, trx);
      UNLOCK(lock_mutex);
      return NORMAL;
    }
    buffer_write_page(table_id, page_id, page_idx, 0);

    impl_lock = give_lock(key, bitmap, impl_trx_id, EXCLUSIVE);
    impl_trx = trx_table[impl_trx_id-1];
    impl_lock->sent_point = entry;
    append_lock(entry, impl_lock, impl_trx);

    conflict_lock = impl_lock;
  }

  append_lock(entry, new_lock, trx);
  point = conflict_lock;
  do {
    if((point->bitmap & bitmap) && (point->owner_trx_id != trx_id))
    {
      trx->wait_trx_id = point->owner_trx_id;
      if(deadlock_detect(trx_id)) {
        UNLOCK(lock_mutex);
        return DEADLOCK;
      }
      WAIT(point->cond, lock_mutex);
      trx->wait_trx_id = 0;
      point = entry->head;
      continue;
    }
    point = point->lock_next;
  } while(point!=new_lock);
  trx->wait_trx_id = 0;
  UNLOCK(lock_mutex);
  return NORMAL;
}