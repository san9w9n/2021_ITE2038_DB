#ifndef __BUFFER_H__
#define __BUFFER_H__
#include "file.h"

typedef struct __attribute__((__packed__)) frame_t {
    page_t* page;
    int64_t table_id;
    pagenum_t page_num;
    char is_dirty;
    int is_pinned;
    int nextLRU;
    int prevLRU;
    char is_ref;
    char is_buf;
} frame_t;

typedef struct buffer_pool_t {
    frame_t* frames;
    int num_frames;
    int num_bufs;
    int firstLRU;
    int lastLRU;
} buffer_pool_t;

int isValid(int64_t table_id);
void append_LRU(int idx);
void delete_LRU(int idx);
void delete_append_LRU(int idx);
int find_empty_frame();
void manage_pin(int32_t idx, int32_t flag);
int hit_idx(int64_t table_id, pagenum_t pagenum);
int give_idx();
int init_buffer(int num_buf);
int64_t file_open_via_bufer(char *pathname);
pagenum_t buffer_alloc_page(int64_t table_id);
void buffer_free_page(int64_t table_id, pagenum_t pagenum, int32_t idx);
int buffer_get_idx(int64_t table_id, pagenum_t pagenum);
page_t* buffer_read_page(int64_t table_id, pagenum_t pagenum, int32_t idx);
void buffer_write_page(int64_t table_id, pagenum_t pagenum, int32_t idx);
int shutdown_buffer();

#endif