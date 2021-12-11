#ifndef __BUFFER_H__
#define __BUFFER_H__

#include "log.h"
#include "file.h"

#define READ 0
#define WRITE 1
#define LOCKED 0
#define UNLOCKED 1

typedef struct frame_t {
  page_t* page;
  int64_t table_id;
  pagenum_t page_num;
  pthread_mutex_t page_mutex;
  int nextLRU;
  int prevLRU;
  int8_t is_dirty;
  int8_t is_buf;
  bool state;
} frame_t;

typedef struct buffer_pool_t {
  frame_t* frames;
  int num_frames;
  int num_bufs;
  int firstLRU;
  int lastLRU;
} buffer_pool_t;

void buffer_flush();
page_t* buffer_read_page_without_latch(int page_idx);
int buf_hashFunction(int64_t table_id, pagenum_t pagenum);
int find_empty_frame(int64_t table_id, pagenum_t pagenum);
int hit_idx(int64_t table_id, pagenum_t pagenum);
int isValid(int64_t table_id);
void append_LRU(int idx);
void delete_LRU(int idx);
void delete_append_LRU(int idx);
int give_idx();
int init_buffer(int num_buf);
int64_t file_open_via_buffer(char* pathname);
pagenum_t buffer_alloc_page(int64_t table_id);
void buffer_free_page(int64_t table_id, pagenum_t pagenum, int32_t idx);
page_t* buffer_read_page(int64_t table_id, pagenum_t pagenum, int* idx, bool mode);
void buffer_write_page(int64_t table_id, pagenum_t pagenum, int32_t idx, bool success);
int shutdown_buffer();

#endif