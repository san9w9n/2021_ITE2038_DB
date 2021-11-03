#include "lock_table.h"
#include <iostream>
#include <utility>
#include <stdlib.h>
#include <unordered_map>
#include <pthread.h>

pthread_mutex_t mutex;
typedef struct link_t link_t;

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
	pthread_mutex_lock(&mutex);

	lock_t* lock = (lock_t*)malloc(sizeof(lock_t));
	lock->prev = lock->next = nullptr;
	lock->cond = PTHREAD_COND_INITIALIZER;
	std::pair<int, int64_t> key_pair = {table_id, key};
	lock_table_t::iterator it = lock_table.find(key_pair);
	lock->sent_point.table_id = table_id;
	lock->sent_point.key = key;

	if(it == lock_table.end()) {
		entry_t* entry = (entry_t*)malloc(sizeof(entry_t));
		entry->table_id = table_id;
		entry->key = key;
		entry->tail = entry->head = lock;

		lock_table[key_pair] = (link_t*)malloc(sizeof(link_t));
		lock_table[key_pair]->cur = entry;
		lock_table[key_pair]->next = nullptr;
	} else {
		bool flag = 1;
		link_t* tmp = it->second;
		while(tmp->cur->key!=key || tmp->cur->table_id!=table_id) {
			if(!tmp->next) {
				flag = 0;
				break;
			}
			tmp = tmp->next;
		}
		if(flag) { // 같은 레코드에 접근 중.
			lock_t* tail = tmp->cur->tail;
			tail->next = lock;
			lock->prev = tail;
			tmp->cur->tail = lock;
		} else { // 같은 레코드가 존재하지 않음.
			entry_t* entry = (entry_t*)malloc(sizeof(entry_t));
			entry->table_id = table_id;
			entry->key = key;
			entry->tail = entry->head = lock;

			link_t* link = (link_t*)malloc(sizeof(link_t));
			link->cur = entry;
			tmp->next = link;
			link->next = nullptr;
		}
		while(lock->prev) pthread_cond_wait(&lock->cond, &mutex);
	}
	pthread_mutex_unlock(&mutex);
  	return lock;
};

int lock_release(lock_t* lock_obj) {
	pthread_mutex_lock(&mutex);
	int table_id = lock_obj->sent_point.table_id;
	int64_t key = lock_obj->sent_point.key;
	std::pair<int, int64_t> key_pair = {table_id, key};

	link_t* prev_tmp = nullptr;
	link_t* tmp = lock_table[key_pair];
	while(tmp->cur->key!=key || tmp->cur->table_id!=table_id) {
		tmp = tmp->next;
		prev_tmp = tmp;
	}
	if(tmp->cur->head == tmp->cur->tail) {
		free(tmp->cur->head);
		if(prev_tmp) {
			prev_tmp->next = tmp->next;
			free(tmp->cur);
			free(tmp);
		} else {
			lock_table[{table_id, key}] = tmp->next;
			free(tmp->cur);
			free(tmp);
			if(!lock_table[{table_id, key}]) lock_table.erase({table_id, key});
		}
	} else {
		lock_t* del = tmp->cur->head;
		tmp->cur->head = tmp->cur->head->next;
		tmp->cur->head->prev = nullptr;
		pthread_cond_signal(&tmp->cur->head->cond);
		free(del);
	}
	pthread_mutex_unlock(&mutex);
  	return 0;
}
