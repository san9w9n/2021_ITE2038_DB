#include "file.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#define READ 0
#define WRITE 1
#define PGSIZE 4096
#define FILENUMS 100

#define PGNUM(X) ((X)/PGSIZE)
#define PGOFFSET(X) ((X)*PGSIZE)

void make_free_pages(int fd, pagenum_t next, uint64_t lp, headerPg_t* headerPg);
ssize_t header_page_rdwr(int fd, int write_else_read, headerPg_t* headerPg);
ssize_t free_page_rdwr(int fd, freePg_t* freePg, pagenum_t pagenum, int write_else_read);

int openedfiles = 0;
int fd_array[FILENUMS];


int file_open_database_file(const char* path) {
    ssize_t check;
    int fd = open(path, O_RDWR|O_SYNC|O_CREAT, 0777);
    if(fd<0) {
        perror("FILE OPEN FAILED!!\n");
        exit(EXIT_FAILURE);
    }

    headerPg_t* headerPg = (headerPg_t*)malloc(sizeof(headerPg_t));
    check = header_page_rdwr(fd, READ, headerPg);

    if(check!=PGSIZE || !headerPg->num_pages) {
        pagenum_t nextfree;
        headerPg->num_pages = 1;
        headerPg->free_num = nextfree = 1;
        uint64_t loop = 2560;

        make_free_pages(fd, nextfree, loop, headerPg);
        header_page_rdwr(fd, WRITE, headerPg);
    }
    free(headerPg);

    fd_array[openedfiles++] = fd;
    return fd;
}

ssize_t header_page_rdwr(int fd, int write_else_read, headerPg_t* headerPg) {
    ssize_t ret_flag = -1;
    if(!headerPg) {
        perror("NO HEADER PAGE!!\n");
        exit(EXIT_FAILURE);
    }

    if(write_else_read) { 
        ret_flag = pwrite(fd, headerPg, PGSIZE, 0);
        if(ret_flag!=PGSIZE) {
            perror("HEADER WRITE FAILED!!\n");
            exit(EXIT_FAILURE);
        }
    } else {
        ret_flag = pread(fd, headerPg, PGSIZE, 0);
        if(ret_flag<0) {
            perror("HEADER_READ FAILED!!\n");
            exit(EXIT_FAILURE);
        }
    }
    if(ret_flag==-1) {
        perror("HEADER READ/WRITE FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    return ret_flag;
}

void make_free_pages(int fd, pagenum_t next, uint64_t lp, headerPg_t* headerPg) {
    if(!headerPg || !headerPg->num_pages) {
        perror("NO HEADER PAGE!!\n");
        exit(EXIT_FAILURE);
    }
    
    pagenum_t nextfree = next;
    freePg_t* freePg = (freePg_t*)malloc(sizeof(freePg_t));
    if(!freePg) {
        perror("ALLOCATE FAILED!!\n");
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

ssize_t free_page_rdwr(int fd, freePg_t* freePg, pagenum_t pagenum, int write_else_read) {
    ssize_t ret_flag = -1;
    if(!freePg) {
        perror("NULL ACCESSED!!\n");
        exit(EXIT_FAILURE);
    }

    if(write_else_read) { 
        ret_flag = pwrite(fd, freePg, PGSIZE, PGOFFSET(pagenum));
        if(ret_flag!=PGSIZE) {
            perror("FREE_PAGE WRITE FAILED!!\n");
            exit(EXIT_FAILURE);
        }
    } else {
        ret_flag = pread(fd, freePg, PGSIZE, PGOFFSET(pagenum));
        if(ret_flag<0) {
            perror("PREE_PAGE READ FAILED!!\n");
            exit(EXIT_FAILURE);
        }
    }
    if(ret_flag==-1) {
        perror("FREE_PAGE READ/WRITE FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    return ret_flag;
}

pagenum_t file_alloc_page(int fd) {
    pagenum_t ret_page = 0;
    if(fd<0) {
        perror("FILE NOT OPENED!!\n");
        exit(EXIT_FAILURE);
    }
    
    headerPg_t* headerPg = (headerPg_t*)malloc(sizeof(headerPg_t));

    header_page_rdwr(fd, READ, headerPg);
    if(!headerPg->num_pages) {
        perror("NO HEADER PAGE!!\n");
        exit(EXIT_FAILURE);
    }
    
    if(!headerPg->free_num) {
        pagenum_t nextfree;
        nextfree = ret_page = headerPg->num_pages;
        headerPg->free_num = (headerPg->num_pages==1) ? 0 : nextfree+1;
        uint64_t loop = headerPg->num_pages <= (UINT64_MAX/2) ? 2*(headerPg->num_pages) : UINT64_MAX;
        make_free_pages(fd, nextfree, loop, headerPg);
    } else {
        freePg_t* freePg = (freePg_t*)malloc(sizeof(freePg_t));
        ret_page = headerPg->free_num;
        free_page_rdwr(fd, freePg, headerPg->free_num, READ);
        headerPg->free_num = freePg->nextfree_num;

        free(freePg);
    }
    if(!ret_page) { 
        perror("FILE ALLOC PAGE FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    header_page_rdwr(fd, WRITE, headerPg);
    free(headerPg);
    return ret_page;
}

void file_free_page(int fd, pagenum_t pagenum) {
    if(fd<0) {
        perror("FILE NOT OPENED!!\n");
        exit(EXIT_FAILURE);
    }
    headerPg_t* headerPg = (headerPg_t*)malloc(sizeof(headerPg_t));
    freePg_t* freePg = (freePg_t*)malloc(sizeof(freePg_t));
    if(!freePg) {
        perror("ALLOCATE FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    header_page_rdwr(fd, READ, headerPg);
    if(!headerPg->num_pages) {
        perror("NO HEADER PAGE!!\n");
        exit(EXIT_FAILURE);
    }

    freePg->nextfree_num = headerPg->free_num;
    headerPg->free_num = pagenum;
    free_page_rdwr(fd, freePg, pagenum, WRITE);
    header_page_rdwr(fd, WRITE, headerPg);

    free(freePg);
    free(headerPg);
}

void file_read_page(int fd, pagenum_t pagenum, page_t* dest) {
    if(fd<0) {
        perror("FILE NOT OPENED!!\n");
        exit(EXIT_FAILURE);
    }
    if(!dest) {
        perror("NULL ACCESSED!!(AT FILE_READ_PAGE)\n");
        exit(EXIT_FAILURE);
    }
    if(pagenum) {
        if(pread(fd, dest, PGSIZE, PGOFFSET(pagenum))<0) {
            perror("FILE READ FAILED!!\n");
            exit(EXIT_FAILURE);
        }
    }
}

void file_write_page(int fd, pagenum_t pagenum, const page_t* src) {
    if(fd<0) {
        perror("FILE NOT OPENED!!\n");
        exit(EXIT_FAILURE);
    }
    if(!src) {
        perror("NULL ACCESSED!!(AT FILE_WRITE_PAGE)\n");
        exit(EXIT_FAILURE);
    }
    if(pagenum) {
        if(pwrite(fd, src, PGSIZE, PGOFFSET(pagenum))!=PGSIZE) {
            perror("FILE WRITE FAILED!!\n");
            exit(EXIT_FAILURE);
        }
    }
}

void file_close_database_file() {
    for(int i=0; i<openedfiles; i++) {
        close(fd_array[i]);
    }
    openedfiles = 0;
}