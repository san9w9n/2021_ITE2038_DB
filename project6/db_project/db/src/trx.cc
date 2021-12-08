#include "trx.h"

lock_table_t lock_table;
trx_table_t trx_table;
pthread_mutex_t lock_mutex;
pthread_mutex_t trx_mutex;
int last_trx_id = 0;

lock_t* give_lock(int64_t key, uint64_t bitmap, int trx_id, bool lock_mode) {
  lock_t* lock;

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

entry_t* give_entry(int64_t table_id, pagenum_t page_id) {
  entry_t* entry;

  entry = new entry_t;
  entry->page_id = page_id;
  entry->table_id = table_id;
  entry->head = entry->tail = nullptr;

  return entry;
}

int init_trx(int num_buf, int flag, int log_num, char* log_path, char* logmsg_path) {
  lock_mutex = PTHREAD_MUTEX_INITIALIZER;
  trx_mutex = PTHREAD_MUTEX_INITIALIZER;
  return init_buffer(num_buf, flag, log_num, log_path, logmsg_path);
}

int shutdown_trx() {
  lock_table.clear();
  for (int i = 0; i < trx_table.size(); i++) delete trx_table[i];
  trx_table.clear();
  return shutdown_buffer();
}

int trx_begin(void) {
  trx_t* trx;
  log_t* log;
  int trx_id;

  LOCK(trx_mutex);
  if(!last_trx_id) 
    last_trx_id = give_last_trx_id();
  trx = new trx_t;
  
  trx->trx_next = nullptr;
  trx->wait_trx_id = 0;
  trx_table.push_back(trx);
  trx->trx_id = trx_id = trx_table.size() + last_trx_id;

  log = make_log(LGSIZE, 0, trx_id, BEGIN);
  trx->last_LSN = log->LSN;
  push_log_buf(log, nullptr, nullptr, nullptr, 0);

  UNLOCK(trx_mutex);

  return trx_id;
}

trx_t* give_trx(int trx_id) {
  LOCK(trx_mutex);
  if ((trx_id-last_trx_id-1 < 0) || trx_id-last_trx_id > trx_table.size()) {
    UNLOCK(trx_mutex);
    return nullptr;
  }
  if (trx_table[trx_id-last_trx_id-1]->state != ACTIVE) {
    UNLOCK(trx_mutex);
    return nullptr;
  }
  UNLOCK(trx_mutex);
  return trx_table[trx_id-last_trx_id-1];
}

int trx_commit(int trx_id) {
  trx_t*        trx;
  undo_t*       undo;
  log_t*        log;

  LOCK(trx_mutex);
  if (trx_table[trx_id-last_trx_id-1]->state != ACTIVE) {
    UNLOCK(trx_mutex);
    return 0;
  }
  trx = trx_table[trx_id-last_trx_id-1];
  trx->wait_trx_id = 0;
  trx->state = COMMIT;
  UNLOCK(trx_mutex);

  std::stack<undo_t*>& S = trx->undo_table;
  while(!S.empty()) {
    undo = S.top(); S.pop();
    delete[] undo->old_value;
    delete undo;
  }

  lock_release(trx);

  log = make_log(LGSIZE, trx->last_LSN, trx_id, COMMITED);
  trx->last_LSN = log->LSN;
  push_log_buf(log, nullptr, nullptr, nullptr, 0);
  log_flush();

  return trx_id;
}

int trx_abort(int trx_id) {
  int page_idx;
  int i;
  uint16_t offset;
  uint16_t old_size;
  uint16_t new_size;
  int64_t table_id;
  pagenum_t page_id;
  pagenum_t root_num;
  int64_t key;
  trx_t* trx;
  lock_t* lock;
  lock_t* prev_lock;
  page_t* page;
  page_t* header;
  int header_idx;
  trx_table_t::iterator it;
  undo_t* undo;
  log_t*  log;
  log_t*  com_log;
  char* old_img;
  char* new_img;
  update_log_t* up_log;

  LOCK(trx_mutex);
  trx = trx_table[trx_id-last_trx_id-1];
  trx->wait_trx_id = 0;
  trx->state = ABORT;
  UNLOCK(trx_mutex);

  std::stack<undo_t*>& S = trx->undo_table;
  while(!S.empty()) 
  {
    undo = S.top(); S.pop();
    table_id = undo->table_id;
    key = undo->key;

    header = buffer_read_page(table_id, 0, &header_idx, READ);
    page_id = header->root_num;
    page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
    while (!page->info.isLeaf) {
      buffer_write_page(table_id, page_id, page_idx, 0);
      if (key < page->branch[0].key)
        page_id = page->leftmost;
      else {
        uint32_t i = 0;
        for (i = 0; i < page->info.num_keys - 1; i++)
          if (key < page->branch[i + 1].key) break;
        page_id = page->branch[i].pagenum;
      }
      page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
    }

    new_size = undo->val_size;
    old_size = page->leafbody.slot[i].size;
    page->leafbody.slot[i].size = new_size;
    offset = page->leafbody.slot[i].offset - 128;
    log = make_log(LGSIZE + UPSIZE + 2*new_size, trx->last_LSN, trx_id, UPDATED);
    up_log = make_up_log(table_id, page_id, offset + 128, new_size);
    trx->last_LSN = log->LSN;
    old_img = new char[old_size+1];
    new_img = new char[new_size+1];
    for(int k=0; k<new_size; k++) 
      new_img[k] = undo->old_value[k];
    for(int k=0; k<old_size; k++) 
      old_img[k] = page->leafbody.value[k+offset];

    push_log_buf(log, up_log, old_img, new_img, 0);

    page->LSN = log->LSN;
    for (int k = offset; k < offset + new_size; k++)
      page->leafbody.value[k] = undo->old_value[k - offset];
    buffer_write_page(table_id, page_id, page_idx, 1);

    delete[] undo->old_value;
    delete undo;
  }
  com_log = make_log(LGSIZE, trx->last_LSN, trx_id, COMMITED);
  trx->last_LSN = com_log->LSN;
  push_log_buf(com_log, nullptr, nullptr, nullptr, 0);
  log_flush();

  lock_release(trx);

  return 0;
}

int lock_release(trx_t* trx) {
  lock_t* point;
  lock_t* lock;
  lock_t* del;
  entry_t* entry;
  int lock_mode;
  int flag;
  int leaf_idx;
  int trx_id;
  int cnt;
  page_t* leaf;

  LOCK(lock_mutex);

  point = trx->trx_next;
  while (point) {
    entry = point->sent_point;
    if (entry->head == point) entry->head = point->lock_next;
    if (entry->tail == point) entry->tail = point->lock_prev;
    if (point->lock_next) point->lock_next->lock_prev = point->lock_prev;
    if (point->lock_prev) point->lock_prev->lock_next = point->lock_next;

    BROADCAST(point->cond);

    if (!entry->head) {
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

bool deadlock_detect(int trx_id) {
  bool flag;
  int target_id;

  LOCK(trx_mutex);
  std::vector<int> visit(trx_table.size() + 2);
  visit[trx_id-last_trx_id-1] = 1;
  target_id = trx_table[trx_id-last_trx_id-1]->wait_trx_id;
  while (1) {
    if (target_id-last_trx_id-1 < 0) {
      flag = false;
      break;
    }
    if (visit[target_id-last_trx_id-1]) {
      flag = true;
      if (target_id != trx_id) flag = false;
      break;
    }
    visit[target_id-last_trx_id-1] = 1;
    target_id = trx_table[target_id-last_trx_id-1]->wait_trx_id;
  }
  UNLOCK(trx_mutex);

  return flag;
}

void append_lock(entry_t* entry, lock_t* lock, trx_t* trx) {
  lock_t* tail;

  if (!entry->head) {
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

bool lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int kindex,
                  int trx_id, bool lock_mode) {
  lock_table_t::iterator lock_it;
  page_t* page;
  entry_t* entry;
  lock_t* point;
  lock_t* new_lock;
  lock_t* comp_Slock;
  lock_t* conflict_lock;
  lock_t* impl_lock;
  trx_t* trx;
  trx_t* impl_trx;
  uint64_t bitmap;
  int page_idx;
  int impl_trx_id;
  bool other_Slock;
  bool conflict;
  bool my_SX;
  bool my_impl;
  bool no_impl;

  LOCK(lock_mutex);

  my_impl = no_impl = false;
  conflict_lock = nullptr;
  conflict = false;
  trx = trx_table[trx_id-last_trx_id-1];
  bitmap = MASK(kindex);
  new_lock = give_lock(key, bitmap, trx_id, lock_mode);

  lock_it = lock_table.find({table_id, page_id});
  if (lock_it == lock_table.end()) {
    entry = give_entry(table_id, page_id);
    new_lock->sent_point = entry;
    lock_table[{table_id, page_id}] = entry;
  } else
    entry = lock_it->second;
  new_lock->sent_point = entry;

  if (lock_mode == SHARED) {
    comp_Slock = nullptr;
    other_Slock = false;
    point = entry->head;
    while (point) {
      if (point->bitmap & bitmap) {
        if (point->owner_trx_id == trx_id) {
          delete new_lock;
          trx->wait_trx_id = 0;
          UNLOCK(lock_mutex);
          return NORMAL;
        }
        if (point->lock_mode == EXCLUSIVE) {
          conflict = true;
          conflict_lock = point;
          break;
        } else
          other_Slock = true;
      } else if ((point->owner_trx_id == trx_id) &&
                 (point->lock_mode == SHARED))
        comp_Slock = point;
      point = point->lock_next;
    }

    if (!conflict) {
      if (other_Slock) {
        if (comp_Slock) {
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
      if (impl_trx_id == trx_id)
        my_impl = true;
      else if ((impl_trx_id-last_trx_id-1 < 0) || (trx_table[impl_trx_id-last_trx_id-1]->state != ACTIVE))
        no_impl = true;
      else if (trx_table[impl_trx_id-last_trx_id-1]->trx_id <= last_trx_id) 
        no_impl = true;
      UNLOCK(trx_mutex);

      if (my_impl) {
        delete new_lock;
        trx->wait_trx_id = 0;
        UNLOCK(lock_mutex);
        return NORMAL;
      }
      if (no_impl) {
        if (comp_Slock) {
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
      impl_trx = trx_table[impl_trx_id-last_trx_id-1];
      impl_lock->sent_point = entry;
      append_lock(entry, impl_lock, impl_trx);

      conflict_lock = impl_lock;
    }

    append_lock(entry, new_lock, trx);
    point = conflict_lock;
    do {
      if ((point->bitmap & bitmap) && (point->lock_mode == EXCLUSIVE)) {
        trx->wait_trx_id = point->owner_trx_id;
        if (deadlock_detect(trx_id)) {
          UNLOCK(lock_mutex);
          return DEADLOCK;
        }
        WAIT(point->cond, lock_mutex);
        trx->wait_trx_id = 0;
        point = entry->head;
        continue;
      }
      point = point->lock_next;
    } while (point != new_lock);
    trx->wait_trx_id = 0;
    UNLOCK(lock_mutex);
    return NORMAL;
  }

  // lock_mode == X mode
  my_SX = false;
  point = entry->head;
  while (point) {
    if (point->bitmap & bitmap) {
      if (point->owner_trx_id == trx_id) {
        if (point->lock_mode == EXCLUSIVE) {
          delete new_lock;
          trx->wait_trx_id = 0;
          UNLOCK(lock_mutex);
          return NORMAL;
        } else
          my_SX = true;
      } else {
        if (!conflict) conflict_lock = point;
        conflict = true;
        break;
      }
    }
    point = point->lock_next;
  }

  if (!conflict) {
    if (my_SX) {
      append_lock(entry, new_lock, trx);
      trx->wait_trx_id = 0;
      UNLOCK(lock_mutex);
      return NORMAL;
    }

    page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
    impl_trx_id = page->leafbody.slot[kindex].trx_id;

    LOCK(trx_mutex);
    if (impl_trx_id == trx_id)
      my_impl = true;
    else if ((impl_trx_id-last_trx_id-1 < 0) || (trx_table[impl_trx_id-last_trx_id-1]->state != ACTIVE))
      no_impl = true;
    else if (trx_table[impl_trx_id-last_trx_id-1]->trx_id <= last_trx_id) 
      no_impl = true;
    UNLOCK(trx_mutex);

    if (my_impl) {
      buffer_write_page(table_id, page_id, page_idx, 0);
      delete new_lock;
      trx->wait_trx_id = 0;
      UNLOCK(lock_mutex);
      return NORMAL;
    }
    if (no_impl) {
      page->leafbody.slot[kindex].trx_id = trx_id;
      buffer_write_page(table_id, page_id, page_idx, 1);
      trx->wait_trx_id = 0;
      append_lock(entry, new_lock, trx);
      UNLOCK(lock_mutex);
      return NORMAL;
    }
    buffer_write_page(table_id, page_id, page_idx, 0);
    impl_lock = give_lock(key, bitmap, impl_trx_id, EXCLUSIVE);
    impl_trx = trx_table[impl_trx_id-last_trx_id-1];
    impl_lock->sent_point = entry;
    append_lock(entry, impl_lock, impl_trx);

    conflict_lock = impl_lock;
  }

  append_lock(entry, new_lock, trx);
  point = conflict_lock;
  do {
    if ((point->bitmap & bitmap) && (point->owner_trx_id != trx_id)) {
      trx->wait_trx_id = point->owner_trx_id;
      if (deadlock_detect(trx_id)) {
        UNLOCK(lock_mutex);
        return DEADLOCK;
      }
      WAIT(point->cond, lock_mutex);
      trx->wait_trx_id = 0;
      point = entry->head;
      continue;
    }
    point = point->lock_next;
  } while (point != new_lock);
  trx->wait_trx_id = 0;
  UNLOCK(lock_mutex);
  return NORMAL;
}