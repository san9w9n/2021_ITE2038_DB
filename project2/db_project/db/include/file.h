#ifndef __FILE_H__
#define __FILE_H__
#include <stdint.h>

typedef uint64_t pagenum_t;

typedef struct page_t {
    char Reserved[4096];
} page_t;

typedef struct headerPg_t {
    pagenum_t free_num;
    uint64_t num_pages;
    char Reserved[4080];
} headerPg_t;

typedef struct freePg_t {
    pagenum_t nextfree_num;
    char Reserved[4088];
} freePg_t;

int file_open_database_file(const char* path);
pagenum_t file_alloc_page(int fd);
void file_free_page(int fd, pagenum_t pagenum);
void file_read_page(int fd, pagenum_t pagenum, page_t* dest);
void file_write_page(int fd, pagenum_t pagenum, const page_t* src);
void file_close_database_file();

#endif