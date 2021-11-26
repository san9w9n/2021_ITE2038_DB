#include "file.h"

#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#pragma GCC optimize("O3")
#pragma GCC optimize("Ofast")
#pragma GCC optimize("unroll-loops")

#define READ 0
#define WRITE 1
#define PGSIZE 4096
#define FILENUMS 100
#define PGOFFSET(X) ((X)<<12)

int table_nums = 0;
Table* table;

void make_free_pages(int fd, pagenum_t next, uint64_t lp, page_t* headerPg);
ssize_t header_page_rdwr(int fd, int write_else_read, page_t* headerPg);
ssize_t free_page_rdwr(int fd, page_t* freePg, pagenum_t pagenum, int write_else_read);
int find_duplicate_file(const char* path);

int find_duplicate_file(const char* path) {
    if(!table) return 0;
    for(int i=0; i<table_nums; i++) {
        if(!strcmp(table[i].filename, path)) return 1;
    }
    return 0;
}

int isValid_table_id(int64_t table_id) {
    if(!table || table_id<0 || table_id>=MAX_TABLES) return 0;
    if(table[table_id].fd<0 || !table[table_id].filename) return 0;
    return 1;
}

int64_t file_open_table_file(const char* path) {
    ssize_t check;
    int64_t table_id;
    if(table_nums>=MAX_TABLES || find_duplicate_file(path)) return -1;
    int fd = open(path, O_RDWR|O_SYNC|O_CREAT, 0777);
    if(fd<0) return -1;

    if(!table) {
        table = (Table*)malloc(sizeof(Table)*MAX_TABLES+1);
        if(!table) {
            // perror("ALLOCATED FAILED!!\n");
            exit(EXIT_FAILURE);
        }
        table[0].filename = (char*)malloc(strlen(path)+1);
        if(!table[0].filename) {
            // perror("ALLOCATED FAILED!!\n");
            exit(EXIT_FAILURE);
        }
        for(int i=0; i<MAX_TABLES; i++) table[0].fd = -1;
        table_nums = 0;
        table[0].fd = fd;
        strcpy(table[0].filename, path);
    } else {
        table[table_nums].filename = (char*)malloc(strlen(path)+1);
        table[table_nums].fd = fd;
        strcpy(table[table_nums].filename, path);
    }
    table_id = table_nums++;

    page_t* headerPg = (page_t*)malloc(sizeof(page_t));
    check = header_page_rdwr(fd, READ, headerPg);

    if(check!=PGSIZE || !headerPg->num_pages) {
        pagenum_t nextfree;
        headerPg->num_pages = 1;
        headerPg->nextfree_num = nextfree = 1;
        headerPg->root_num = 0;
        uint64_t loop = 2560;

        make_free_pages(fd, nextfree, loop, headerPg);
        header_page_rdwr(fd, WRITE, headerPg);
    }
    free(headerPg);

    return table_id;
}

ssize_t header_page_rdwr(int fd, int write_else_read, page_t* headerPg) {
    ssize_t ret_flag = -1;
    if(!headerPg) {
        // perror("NO HEADER PAGE!!\n");
        exit(EXIT_FAILURE);
    }

    if(write_else_read) { 
        ret_flag = pwrite(fd, headerPg, PGSIZE, 0);
        if(ret_flag!=PGSIZE) {
            // perror("HEADER WRITE FAILED!!\n");
            exit(EXIT_FAILURE);
        }
        fsync(fd);
    } else {
        ret_flag = pread(fd, headerPg, PGSIZE, 0);
        if(ret_flag<0) {
            // perror("HEADER READ FAILED!!\n");
            exit(EXIT_FAILURE);
        }
    }
    if(ret_flag==-1) {
        // perror("HEADER READ/WRITE FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    return ret_flag;
}

void make_free_pages(int fd, pagenum_t next, uint64_t lp, page_t* headerPg) {
    if(!headerPg || !headerPg->num_pages) {
        // perror("NO HEADER PAGE!!\n");
        exit(EXIT_FAILURE);
    }
    
    pagenum_t nextfree = next;
    page_t* freePg = (page_t*)malloc(sizeof(page_t));
    if(!freePg) {
        // perror("ALLOCATE FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    uint64_t loop = lp;
    while(headerPg->num_pages<loop-1) {
        freePg->nextfree_num = nextfree+1;
        free_page_rdwr(fd, freePg, nextfree, WRITE);
        headerPg->num_pages++; nextfree++;
    }
    freePg->nextfree_num = 0;
    free_page_rdwr(fd, freePg, nextfree, WRITE);
    headerPg->num_pages++;

    free(freePg);
}

ssize_t free_page_rdwr(int fd, page_t* freePg, pagenum_t pagenum, int write_else_read) {
    ssize_t ret_flag = -1;
    if(!freePg) {
        // perror("NULL ACCESSED!!\n");
        exit(EXIT_FAILURE);
    }

    if(write_else_read) { 
        ret_flag = pwrite(fd, freePg, PGSIZE, PGOFFSET(pagenum));
        if(ret_flag!=PGSIZE) {
            // perror("FREE_PAGE WRITE FAILED!!\n");
            exit(EXIT_FAILURE);
        }
        fsync(fd);
    } else {
        ret_flag = pread(fd, freePg, PGSIZE, PGOFFSET(pagenum));
        if(ret_flag<0) {
            // perror("PREE_PAGE READ FAILED!!\n");
            exit(EXIT_FAILURE);
        }
    }
    if(ret_flag==-1) {
        // perror("FREE PAGE READ/WRITE FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    return ret_flag;
}

pagenum_t file_alloc_page(int64_t table_id) {
    pagenum_t ret_page = 0;
    int fd;
    fd = table[table_id].fd;
    page_t* headerPg = (page_t*)malloc(sizeof(page_t));

    if(header_page_rdwr(fd, READ, headerPg)!=PGSIZE) {
        // perror("HEADER READ FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    if(!headerPg->num_pages) {
        // perror("NO HEADER PAGE!!\n");
        exit(EXIT_FAILURE);
    }
    
    if(!headerPg->nextfree_num) {
        pagenum_t nextfree;
        nextfree = ret_page = headerPg->num_pages;
        headerPg->nextfree_num = (headerPg->num_pages==1) ? 0 : nextfree+1;
        uint64_t loop = headerPg->num_pages <= (UINT64_MAX/2) ? 2*(headerPg->num_pages) : UINT64_MAX;
        make_free_pages(fd, nextfree, loop, headerPg);
    } else {
        page_t* freePg = (page_t*)malloc(sizeof(page_t));
        ret_page = headerPg->nextfree_num;
        if(free_page_rdwr(fd, freePg, headerPg->nextfree_num, READ)!=PGSIZE) {
            // perror("FREE PAGE READ FAILED!!\n");
            exit(EXIT_FAILURE);
        }
        headerPg->nextfree_num = freePg->nextfree_num;

        free(freePg);
    }
    if(!ret_page) { 
        // perror("FILE ALLOC PAGE FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    
    header_page_rdwr(fd, WRITE, headerPg);
    free(headerPg);
    return ret_page;
}

void file_free_page(int64_t table_id, pagenum_t pagenum) {
    int fd;
    fd = table[table_id].fd;

    page_t* headerPg = (page_t*)malloc(sizeof(page_t));
    page_t* freePg = (page_t*)malloc(sizeof(page_t));
    if(!freePg) {
        // perror("ALLOCATE FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    if(header_page_rdwr(fd, READ, headerPg)!=PGSIZE) {
        // perror("HEADER READ FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    if(!headerPg->num_pages) {
        // perror("NO HEADER PAGE!!\n");
        exit(EXIT_FAILURE);
    }

    freePg->nextfree_num = headerPg->nextfree_num;
    headerPg->nextfree_num = pagenum;
    free_page_rdwr(fd, freePg, pagenum, WRITE);
    header_page_rdwr(fd, WRITE, headerPg);

    free(freePg);
    free(headerPg);
}

void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest) {
    int fd;
    fd = table[table_id].fd;
    if(!dest) {
        // perror("NULL ACCESSED!!(AT FILE_READ_PAGE)\n");
        exit(EXIT_FAILURE);
    }
    if(pread(fd, dest, PGSIZE, PGOFFSET(pagenum))<0) {
        // perror("FILE READ FAILED!!\n");
        exit(EXIT_FAILURE);
    }
}

void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src) {
    int fd;
    fd = table[table_id].fd;
    if(!src) {
        // perror("NULL ACCESSED!!(AT FILE_WRITE_PAGE)\n");
        exit(EXIT_FAILURE);
    }
    if(pwrite(fd, src, PGSIZE, PGOFFSET(pagenum))!=PGSIZE) {
        // perror("FILE WRITE FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    fsync(fd);
}

void file_close_table_files() {
    for(int i=0; i<table_nums; i++) {
        free(table[i].filename);
        close(table[i].fd);
    }
    free(table);
    table = NULL;
    table_nums = 0;
}