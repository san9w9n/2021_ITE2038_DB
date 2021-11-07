#include "lock_table.h"
#include <iostream>
#include <utility>
#include <stdlib.h>
#include <unordered_map>
#include <pthread.h>

pthread_mutex_t mutex;

struct pair_hash {
  	template <class T1, class T2>
  	std::size_t operator() (const std::pair<T1, T2> &pair) const {
	    return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
  	}
};

struct sent_point_t {
	int table_id;
	int64_t key;
};

struct lock_t {
	lock_t* prev;
	lock_t* next;
	sent_point_t sent_point;
	pthread_cond_t cond;
};

struct entry_t {
	int table_id;
	int64_t key;
	lock_t* tail;
	lock_t* head;
};

struct link_t {
	entry_t* cur;
	link_t* next;
};

typedef std::unordered_map<std::pair<int, int64_t>, link_t* , pair_hash> lock_table_t;
lock_table_t lock_table;

int init_lock_table() {
	mutex = PTHREAD_MUTEX_INITIALIZER;
  	return 0;
}

lock_t* lock_acquire(int table_id, int64_t key) {
	lock_t* 					lock;
	lock_t* 					tail;
	entry_t* 					entry;
	link_t* 					tmp;
	link_t*						link;
	bool 						flag;
	std::pair<int, int64_t> 	key_pair;

	pthread_mutex_lock(&mutex);

	lock = (lock_t*)malloc(sizeof(lock_t));
	lock->prev = lock->next = nullptr;
	lock->cond = PTHREAD_COND_INITIALIZER;
	lock->sent_point.table_id = table_id;
	lock->sent_point.key = key;

	key_pair = {table_id, key};
	lock_table_t::iterator it = lock_table.find(key_pair);
	
	if(it == lock_table.end()) {
		entry = (entry_t*)malloc(sizeof(entry_t));
		entry->table_id = table_id;
		entry->key = key;
		entry->tail = entry->head = lock;

		lock_table[key_pair] = (link_t*)malloc(sizeof(link_t));
		lock_table[key_pair]->cur = entry;
		lock_table[key_pair]->next = nullptr;
	} else {
		flag = 1;
		tmp = it->second;
		while(tmp->cur->key!=key || tmp->cur->table_id!=table_id) {
			if(!tmp->next) {
				flag = 0;
				break;
			}
			tmp = tmp->next;
		}
		if(flag) {
			tail = tmp->cur->tail;
			tail->next = lock;
			lock->prev = tail;
			tmp->cur->tail = lock;
			pthread_cond_wait(&lock->cond, &mutex);
		} else {
			entry = (entry_t*)malloc(sizeof(entry_t));
			entry->table_id = table_id;
			entry->key = key;
			entry->tail = entry->head = lock;

			link = (link_t*)malloc(sizeof(link_t));
			link->cur = entry;
			tmp->next = link;
			link->next = nullptr;
		}
	}
	pthread_mutex_unlock(&mutex);

  	return lock;
};

int lock_release(lock_t* lock_obj) {
	int 						table_id;
	int64_t 					key;
	link_t* 					prev_tmp;
	link_t* 					tmp;
	lock_t*						del;
	std::pair<int, int64_t> 	key_pair;

	pthread_mutex_lock(&mutex);

	table_id = lock_obj->sent_point.table_id;
	key = lock_obj->sent_point.key;
	key_pair = {table_id, key};

	prev_tmp = nullptr;
	tmp = lock_table[key_pair];
	if(!tmp || !tmp->cur) {
		pthread_mutex_unlock(&mutex);
		return 1;
	}

	while(tmp->cur->key!=key || tmp->cur->table_id!=table_id) {
		prev_tmp = tmp;
		tmp = tmp->next;
		if(!tmp) {
			pthread_mutex_unlock(&mutex);
			return 1;
		}
	}
	if(tmp->cur->head == tmp->cur->tail) {
		free(tmp->cur->head);
		if(prev_tmp) {
			prev_tmp->next = tmp->next;
			free(tmp->cur);
			free(tmp);
		} else {
			lock_table[key_pair] = tmp->next;
			free(tmp->cur);
			free(tmp);
			if(!lock_table[key_pair]) lock_table.erase(key_pair);
		}
	} else {
		del = tmp->cur->head;
		tmp->cur->head = tmp->cur->head->next;
		tmp->cur->head->prev = nullptr;
		pthread_cond_signal(&tmp->cur->head->cond);
		free(del);
	}

	pthread_mutex_unlock(&mutex);
  	return 0;
}