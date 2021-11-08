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

struct lock_t {
	lock_t* prev;
	lock_t* next;
	entry_t* sent_point;
	pthread_cond_t cond;
};

struct entry_t {
	int64_t table_id;
	int64_t key;
	lock_t* tail;
	lock_t* head;
};

struct link_t {
	entry_t* cur;
	link_t* next;
};

typedef std::unordered_map<std::pair<int64_t, int64_t>, link_t* , pair_hash> lock_table_t;
lock_table_t lock_table;

int init_lock_table() {
	mutex = PTHREAD_MUTEX_INITIALIZER;
  	return 0;
}

lock_t* lock_acquire(int64_t table_id, int64_t key) {
	lock_t* 				lock;
	lock_t* 				tail;
	entry_t* 				entry;
	link_t* 				tmp;
	link_t*					link;
	bool 					flag;
	std::pair<int64_t, int64_t> key_pair;

	pthread_mutex_lock(&mutex);

	lock = (lock_t*)malloc(sizeof(lock_t));
	lock->prev = lock->next = nullptr;
	lock->cond = PTHREAD_COND_INITIALIZER;

	key_pair = {table_id, key};
	lock_table_t::iterator it = lock_table.find(key_pair);
	
	if(it == lock_table.end()) {
		entry = (entry_t*)malloc(sizeof(entry_t));
		entry->table_id = table_id;
		entry->key = key;
		entry->tail = entry->head = lock;

		lock->sent_point = entry;

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
			lock->sent_point = tmp->cur;
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

			lock->sent_point = entry;
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
	int64_t					table_id;
	int64_t 				key;
	link_t* 				prev_tmp;
	link_t* 				tmp;
	entry_t*				sent_point;
	lock_t*					del;
	std::pair<int64_t, int64_t> key_pair;

	pthread_mutex_lock(&mutex);

	sent_point = lock_obj->sent_point;
	table_id = sent_point->table_id;
	key = sent_point->key;
	key_pair = {table_id, key};

	if(sent_point->head != sent_point->tail) {
		if(sent_point->head!=lock_obj) {
			perror("WRONG LOCK_OBJ!!");
			exit(EXIT_FAILURE);
		}
		del = sent_point->head;
		sent_point->head = del->next;
		sent_point->head->prev = nullptr;
		free(del);

		pthread_cond_signal(&sent_point->head->cond);
	} else {
		prev_tmp = nullptr;
		tmp = lock_table[key_pair];
		while(tmp->cur!=sent_point) {
			prev_tmp = tmp;
			tmp = tmp->next;
			if(!tmp) {
				perror("WRONG LOGIC!!");
				exit(EXIT_FAILURE);
			}
		}
		free(sent_point->head);
		if(prev_tmp) {
			prev_tmp->next = tmp->next;
			free(sent_point);
			free(tmp);
		} else {
			lock_table[key_pair] = tmp->next;
			if(!tmp->next) lock_table.erase(key_pair);
			free(sent_point);
			free(tmp);
		}
	}
	pthread_mutex_unlock(&mutex);
  	return 0;
}