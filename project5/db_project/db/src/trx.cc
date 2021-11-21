#include "trx.h"

#include <stdlib.h>
#include <cstring>
#include <stdio.h>
#include <queue>
#include <iostream>

#define SHARED 0
#define EXCLUSIVE 1

#define RUNNING 0
#define WAITING 1
#define INIT 2

#define NORMAL 0
#define DEADLOCK 1
#define CHECK_IMPL 2

#define MASK(X) (1<<(63-(X)))
#define LOCK(X) (pthread_mutex_lock(&(X)))
#define UNLOCK(X) (pthread_mutex_unlock(&(X)))
#define WAIT(X, Y) (pthread_cond_wait(&(X), &(Y)))
#define SIGNAL(X) (pthread_cond_signal(&(X)))

lock_manager_t* lock_manager = nullptr;
trx_manager_t* trx_manager = nullptr;
pthread_mutex_t lock_mutex;
pthread_mutex_t trx_mutex;

lock_t*
give_lock(int64_t key, int trx_id, int lock_mode)
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
    lock->lock_state = INIT;
    lock->bitmap = 0;

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

int
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

    LOCK(trx_mutex);
    it = trx_manager->trx_table.find(trx_id);
    if(it == trx_manager->trx_table.end()) {
        UNLOCK(trx_mutex);
        return 0;
    }
    trx = it->second;
    UNLOCK(trx_mutex);

    for(auto log_it = trx->log_table.begin(); log_it != trx->log_table.end(); log_it++) {
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

        size = leaf_page->leafbody.slot[i].offset;
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

    return trx_id;
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
    int64_t                 key;
    uint64_t                bitmap;
    int64_t                 table_id;
    pagenum_t               page_id;
    page_t*                 leaf;

    LOCK(lock_mutex);

    point = trx->trx_next;
    while(point) {
        flag = 0;
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
            
            del = point;
            point = point->trx_next;
            trx->trx_next = point;
            free(del);

            continue;
        }

        if(lock_mode == SHARED) 
        {
            std::unordered_map<int, int> key_map;
            std::unordered_map<int, int>::iterator it;

            bitmap = point->bitmap;

            leaf = buffer_read_page(table_id, page_id, &leaf_idx, READ);
            for(int i=0; i<leaf->info.num_keys; i++) {
                if((MASK(i) & bitmap) != 0)
                    key_map[leaf->leafbody.slot[i].key] = 0;
            }

            while(lock) 
            {
                if(lock->lock_state == WAITING) 
                {
                    it = key_map.find(lock->key);
                    if(it != key_map.end()) 
                    {
                        if(lock->lock_mode == SHARED) {
                            if(it->second != 2) {
                                lock->lock_state = RUNNING;
                                trx_manager->trx_table[lock->owner_trx_id]->waiting_lock = nullptr;
                                key_map[lock->key] = 1;
                                SIGNAL(lock->cond);
                            }
                        }
                        else {
                            if(it->second == 0) {
                                lock->lock_state = RUNNING;
                                trx_manager->trx_table[lock->owner_trx_id]->waiting_lock = nullptr;
                                key_map[lock->key] = 2;
                                SIGNAL(lock->cond);
                            }
                            else key_map[lock->key] = 2;
                        }
                    }
                }
                lock->lock_next;
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
                        lock->lock_state = RUNNING;
                        trx_manager->trx_table[lock->owner_trx_id]->waiting_lock = nullptr;
                        SIGNAL(lock->cond);
                    }
                    else {
                        if(!flag) {
                            lock->lock_state = RUNNING;
                            trx_manager->trx_table[lock->owner_trx_id]->waiting_lock = nullptr;
                            SIGNAL(lock->cond);
                        }
                        break;
                    }
                }
                lock = lock->lock_next;
            }
        }

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
    trx_t*                  trx;
    int                     cur;
    int64_t                 key;

    LOCK(trx_mutex);
    key = lock_obj->key;
    point = lock_obj->lock_prev;
    while(point) {
        if(point->key == key) 
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

        point = point->lock_prev;
        while(point) {
            if(point->owner_trx_id == lock_obj->owner_trx_id) {
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
impl_to_expl(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode, int i)
{
    entry_t*                entry;
    lock_t*                 impl_lock;
    lock_t*                 lock;
    lock_t*                 point;
    trx_t*                  trx;
    page_t*                 page;
    int                     leaf_idx;
    int                     impl_trx_id;
    trx_table_t::iterator   it;
    lock_table_t::iterator  lock_it;

    page = buffer_read_page(table_id, page_id, &leaf_idx, WRITE);
    if(page->leafbody.slot[i].key != key) {
        buffer_write_page(table_id, page_id, leaf_idx, 0);
        return 0;
    }

    
    LOCK(trx_mutex);
    impl_trx_id = page->leafbody.slot[i].trx_id;
    if(impl_trx_id == trx_id) {
        buffer_write_page(table_id, page_id, leaf_idx, 0);
        if(lock_mode == SHARED) {
            LOCK(lock_mutex);

            lock_it = lock_manager->lock_table.find({table_id, page_id});
            if(lock_it == lock_manager->lock_table.end()) {
                entry = give_entry(table_id, page_id);
                lock_manager->lock_table[{table_id, page_id}] = entry;
            }
            else entry = lock_it->second;

            lock = give_lock(key, trx_id, EXCLUSIVE);
            lock->lock_state = RUNNING;
            lock->sent_point = entry;
            trx = trx_manager->trx_table[trx_id];
            trx->waiting_lock = nullptr;
            append_lock(entry, lock, trx);

            UNLOCK(lock_mutex);
        }
        return 1;
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

            if(lock_it == lock_manager->lock_table.end()) 
            {
                lock = give_lock(key, trx_id, SHARED);
                
                entry = give_entry(table_id, page_id);
                lock->lock_state = RUNNING;
                lock->sent_point = entry;
                lock_manager->lock_table[{table_id, page_id}] = entry;
            } 
            
            else 
            {
                entry = lock_it->second;
                point = entry->head;
                while(point) {
                    if((point->owner_trx_id == trx_id)
                        && (point->lock_mode == SHARED))
                    {
                        point->bitmap |= MASK(i);
                        UNLOCK(lock_mutex);
                        return 1;   
                    }
                    point = point->lock_next;
                }
                lock = give_lock(key, trx_id, SHARED);
            }

            lock->bitmap |= MASK(i);
            append_lock(entry, lock, trx);
            UNLOCK(lock_mutex);            
            return 1;
        }
        return 1;
    }

    LOCK(lock_mutex);
    lock_it = lock_manager->lock_table.find({table_id, page_id});
    if(lock_it == lock_manager->lock_table.end()) {
        entry = give_entry(table_id, page_id);
        lock_manager->lock_table[{table_id, page_id}] = entry;
    }
    else entry = lock_it->second;

    impl_lock = give_lock(key, impl_trx_id, EXCLUSIVE);
    impl_lock->lock_state = RUNNING;
    impl_lock->sent_point = entry;
    append_lock(entry, impl_lock, it->second);
    UNLOCK(trx_mutex);

    trx = trx_manager->trx_table[trx_id];
    lock = give_lock(key, trx_id, lock_mode);
    if(lock_mode == SHARED) lock->bitmap |= MASK(i);
    lock->lock_state = WAITING;
    lock->sent_point = entry;
    append_lock(entry, lock, trx);

    if(deadlock_detect(lock)) {
        UNLOCK(lock_mutex);
        return 0;
    }
    WAIT(lock->cond, lock_mutex);
    UNLOCK(lock_mutex);
    return 1;
}

int
db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t *val_size, int trx_id)
{
    lock_t*         lock;
    page_t*         header;
    page_t*         page;
    pagenum_t       root_num;
    pagenum_t       page_id;
    int             i;
    int             header_idx;
    int             leaf_idx;
    int             flag;
    uint16_t        offset;
    uint16_t        size;

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

    flag = 0;
    lock = lock_acquire(table_id, page_id, key, trx_id, SHARED, &flag, i);
    if(!lock) {
        if(flag == DEADLOCK) {
            trx_abort(trx_id);
            return 1;
        }
        if(!(impl_to_expl(table_id, page_id, key, trx_id, SHARED, i))) {
            trx_abort(trx_id);
            return 1;
        }
    }

    page = buffer_read_page(table_id, page_id, &leaf_idx, READ);
    if(page->leafbody.slot[i].key != key)
        trx_abort(trx_id);

    offset = page->leafbody.slot[i].offset-128;
    size = page->leafbody.slot[i].size;
    for(int k=offset; k<offset+size; k++)
        ret_val[k-offset] = page->leafbody.value[k];
    *val_size = size;

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
    lock_t*                 lock;
    lock_t*                 impl_lock;
    trx_t*                  trx;
    trx_t*                  impl_trx;
    entry_t*                entry;
    log_t*                  log;
    log_table_t::iterator   log_it;
    uint16_t                offset;
    uint16_t                size;
    char*                   old_value;

    header = buffer_read_page(table_id, 0, &header_idx, READ);
    root_num = header->root_num;
    if(!root_num) {
        trx_abort(trx_id);
        return 1;
    }

    // find record
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

    
    //  acquire lock
    flag = 0;
    lock = lock_acquire(table_id, page_id, key, trx_id, EXCLUSIVE, &flag, i);
    if(!lock) 
    {
        if(flag == DEADLOCK) {
            trx_abort(trx_id);
            return 1;
        }
        if(!(impl_to_expl(table_id, page_id, key, trx_id, EXCLUSIVE, i))) {
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
        log->table_id = table_id;
        log->key = key;
        log->val_size = size;
        // strncpy(log->old_value, old_value, size);
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

lock_t*
lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode, int* flag, int index)
{
    int                     conflict;
    uint64_t                mask;
    lock_t*                 point;
    entry_t*                entry;
    lock_t*                 lock;
    trx_t*                  trx;
    lock_table_t::iterator  it;

    LOCK(lock_mutex);
    
    mask = MASK(index);
    lock = give_lock(key, trx_id, lock_mode);

    it = lock_manager->lock_table.find({table_id, page_id});
    if(it == lock_manager->lock_table.end()) {
        free(lock);
        *flag = CHECK_IMPL;
        UNLOCK(lock_mutex);
        return nullptr;
    }

    entry = it->second;
    lock->sent_point = entry;

    if(lock_mode == EXCLUSIVE)
    {
        point = entry->head;
        while(point) 
        {
            if(point->lock_state == RUNNING) 
            {
                if(point->key == key) 
                {
                    if(point->owner_trx_id == trx_id) {
                        if(point->lock_mode == SHARED) {
                            free(lock);
                            *flag = DEADLOCK;
                            UNLOCK(lock_mutex);
                            return nullptr;
                        }
                        free(lock);
                        UNLOCK(lock_mutex);
                        return point;
                    }

                    if(point->lock_mode == SHARED) {
                        point = point->lock_next;
                        while(point) {
                            if((point->owner_trx_id == trx_id) 
                                && (point->lock_mode == SHARED)
                                && (point->bitmap & mask)) 
                            {
                                free(lock);
                                *flag = DEADLOCK;
                                UNLOCK(lock_mutex);
                                return nullptr;
                            }
                            point = point->lock_next;
                        }
                    }

                    trx = trx_manager->trx_table[trx_id];
                    append_lock(entry, lock, trx);
                    lock->lock_state = WAITING;
                    trx->waiting_lock = lock;

                    if(deadlock_detect(lock)) {
                        *flag = DEADLOCK;
                        UNLOCK(lock_mutex);
                        return nullptr;
                    }
                    WAIT(lock->cond, lock_mutex);
                    UNLOCK(lock_mutex);
                    return lock;
                }

                else if((point->lock_mode == SHARED) 
                    && (point->bitmap & mask))
                {
                    if(point->owner_trx_id == trx_id) {
                        free(lock);
                        *flag = DEADLOCK;
                        UNLOCK(lock_mutex);
                        return nullptr;
                    }

                    point = point->lock_next;
                    while(point) {
                        if((point->lock_mode == SHARED)  
                            && (point->owner_trx_id == trx_id) 
                            && (point->bitmap & mask)) 
                        {
                            free(lock);
                            *flag = DEADLOCK;
                            UNLOCK(lock_mutex);
                            return nullptr;
                        }
                        point = point->lock_next;
                    }

                    trx = trx_manager->trx_table[trx_id];
                    append_lock(entry, lock, trx);
                    lock->lock_state = WAITING;
                    trx->waiting_lock = lock;
                    if(deadlock_detect(lock)) {
                        *flag = DEADLOCK;
                        UNLOCK(lock_mutex);
                        return nullptr;
                    }
                    WAIT(lock->cond, lock_mutex);
                    UNLOCK(lock_mutex);
                    return lock;
                }
            }
            point = point->lock_next;
        }
    }

    else {
        point = entry->head;
        while(point) 
        {
            if((point->key == key)
                && (point->lock_mode == EXCLUSIVE)
                && (point->lock_state == RUNNING))
            {
                if(point->owner_trx_id == trx_id) {
                    free(lock);
                    UNLOCK(lock_mutex);
                    return point;
                }

                trx = trx_manager->trx_table[trx_id];
                trx->waiting_lock = lock;

                append_lock(entry, lock, trx);
                lock->bitmap |= mask;
                lock->lock_state = WAITING;
                
                if(deadlock_detect(lock)) {
                    *flag = DEADLOCK;
                    UNLOCK(lock_mutex);
                    return nullptr;
                }
                WAIT(lock->cond, lock_mutex);
                UNLOCK(lock_mutex);
                return lock;
            }
            point = point->lock_next;
        }
    }

    free(lock);
    *flag = CHECK_IMPL;
    UNLOCK(lock_mutex);
    return nullptr;
}