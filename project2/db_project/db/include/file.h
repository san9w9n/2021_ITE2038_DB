#ifndef __FILE_H__
#define __FILE_H__
#include <stdint.h>

#define MAX_TABLES 20

typedef uint64_t pagenum_t;

typedef struct __attribute__((__packed__)) Table {
    int fd;
    char* filename;
} Table;

typedef struct __attribute__((__packed__)) pageheader_t {
    pagenum_t parent_num;
    uint32_t isLeaf;
    uint32_t num_keys;
} pageheader_t; // 16

typedef struct __attribute__((__packed__)) slot_t {
    int64_t key;
    uint16_t size;
    uint16_t offset;
} slot_t; // 12

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
    pageheader_t info;
    char Reserved[96];
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

typedef struct __attribute__((__packed__)) headerPg_t {
    pagenum_t free_num;
    uint64_t num_pages;
    pagenum_t root_num;
    char Reserved[4072];
} headerPg_t;

typedef struct __attribute__((__packed__)) freePg_t {
    pagenum_t nextfree_num;
    char Reserved[4088];
} freePg_t;


// API
int64_t file_open_table_file(const char* path);
pagenum_t file_alloc_page(int64_t table_id);
void file_free_page(int64_t table_id, pagenum_t pagenum);
void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest);
void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src);
void file_close_table_files();
pagenum_t get_rootnum(int64_t table_id);
void set_rootnum(int64_t table_id, pagenum_t root_num);
int isValid_table_id(int64_t table_id);

#endif