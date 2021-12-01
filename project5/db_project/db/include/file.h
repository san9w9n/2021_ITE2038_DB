#ifndef __FILE_H__
#define __FILE_H__

#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "pthread.h"
#include <unordered_map>
#include <vector>

#define MAX_TABLES 20
#define PGSIZE 4096
typedef uint64_t pagenum_t;

typedef struct Table {
    int fd;
    char* filename;
} Table;

typedef struct __attribute__((__packed__)) pageInfo_t {
    uint32_t isLeaf;
    uint32_t num_keys;
} pageInfo_t;

typedef struct __attribute__((__packed__)) slot_t {
    int64_t key;
    uint16_t size;
    uint16_t offset;
    int trx_id;
} slot_t;

typedef struct __attribute__((__packed__)) branch_t {
    int64_t key;
    pagenum_t pagenum;
} branch_t;

typedef struct __attribute__((__packed__)) leafbody_t {
    union {
        slot_t slot[64];
        char value[3968];
    };
} leafbody_t; 

typedef struct __attribute__((__packed__)) page_t {
    union {
        pagenum_t parent_num;
        pagenum_t nextfree_num;
    };
    union {
      pageInfo_t info;
      uint64_t num_pages;
    };
    pagenum_t root_num;
    char Reserved[88];
    uint64_t freespace;
    union {
        uint64_t Rsibling;
        uint64_t leftmost;
    };
    union {
        leafbody_t leafbody;
        branch_t branch[248];
    };
} page_t;

// API
int64_t file_open_table_file(const char* path);
pagenum_t file_alloc_page(int64_t table_id);
void file_free_page(int64_t table_id, pagenum_t pagenum);
void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest);
void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src);
void file_close_table_files();
int isValid_table_id(int64_t table_id);

#endif