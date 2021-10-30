#include "buffer.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

buffer_pool_t* buffer = NULL;

int buf_hashFunction(int64_t table_id, pagenum_t pagenum) {
    int ret = (pagenum+table_id)<<2;
    return ret % buffer->num_bufs;
}

int find_empty_frame(int64_t table_id, pagenum_t pagenum) {
    int i, tablesize;
    tablesize = buffer->num_bufs;
    i = buf_hashFunction(table_id, pagenum);
    while(buffer->frames[i].is_buf) {
        i = (i+1)%tablesize;
    }
    append_LRU(i);
    return i;
}

int hit_idx(int64_t table_id, pagenum_t pagenum) {
    int i, hashValue, tablesize;
    tablesize = buffer->num_bufs;
    hashValue = buf_hashFunction(table_id, pagenum);
    i = hashValue;
    while(1) {
        if(buffer->frames[i].is_buf && buffer->frames[i].table_id==table_id && buffer->frames[i].page_num==pagenum) {
            delete_append_LRU(i);
            return i;
        }
        i = (i+1)%tablesize;
        if(i==hashValue) break;
    }
    return -1;
}

int isValid(int64_t table_id) {
    if(!buffer || !isValid_table_id(table_id)) return 0;
    return 1;
}

void append_LRU(int idx) {
    if(!buffer->num_frames) {
        buffer->firstLRU = buffer->lastLRU = idx;
        buffer->frames[idx].prevLRU = buffer->frames[idx].nextLRU = -1;
        buffer->num_frames++;
        return;
    }
    if(buffer->lastLRU>=0) buffer->frames[buffer->lastLRU].nextLRU = idx;
    buffer->frames[idx].prevLRU = buffer->lastLRU;
    buffer->frames[idx].nextLRU = -1;
    buffer->lastLRU = idx;
    buffer->num_frames++;
}

void delete_LRU(int idx) {
    if(buffer->firstLRU==idx) buffer->firstLRU = buffer->frames[idx].nextLRU;
    if(buffer->lastLRU==idx) buffer->lastLRU = buffer->frames[idx].prevLRU;
    if(buffer->frames[idx].prevLRU>=0) 
        buffer->frames[buffer->frames[idx].prevLRU].nextLRU = buffer->frames[idx].nextLRU;
    if(buffer->frames[idx].nextLRU>=0) 
        buffer->frames[buffer->frames[idx].nextLRU].prevLRU = buffer->frames[idx].prevLRU;
    buffer->frames[idx].nextLRU = buffer->frames[idx].prevLRU = -1;
    buffer->num_frames--;
}

void delete_append_LRU(int idx) {
    if(buffer->lastLRU==idx) return;
    delete_LRU(idx);
    append_LRU(idx);
}

void manage_pin(int32_t idx, int32_t flag) {
    if(flag) buffer->frames[idx].is_pinned++;
    else {
        if(buffer->frames[idx].is_pinned>0) 
            buffer->frames[idx].is_pinned--;
    }
}

int give_idx() {
    int ret_idx = -1, i = buffer->firstLRU;
    while(ret_idx<0) {
        if(!buffer->frames[i].is_pinned) {
            if(buffer->frames[i].is_dirty)
                file_write_page(buffer->frames[i].table_id, buffer->frames[i].page_num, buffer->frames[i].page);
            memset(buffer->frames[i].page, 0x00, PGSIZE);
            ret_idx = i;
        }
        i = buffer->frames[i].nextLRU;
        if(i<0) break;
    }
    if(i<0 && ret_idx<0) {
        perror("ALL PINNED!!\n");
        exit(EXIT_FAILURE);
    }
    delete_append_LRU(ret_idx);
    return ret_idx;
}

int init_buffer(int num_buf) {
    if(!buffer) {
        buffer = (buffer_pool_t*)malloc(sizeof(buffer_pool_t));
        if(!buffer) {
            perror("MALLOC FAILED!!\n");
            exit(EXIT_FAILURE);
        }
        if(num_buf<5) num_buf = 5;
        buffer->frames = (frame_t*)malloc(sizeof(frame_t)*num_buf);
        if(!buffer->frames) {
            perror("MALLOC FAILED!!\n");
            exit(EXIT_FAILURE);
        }
        for(int i=0; i<num_buf; i++) {
            buffer->frames[i].page = (page_t*)malloc(PGSIZE);
            if(!buffer->frames[i].page) {
                perror("MALLOC FAILED!!\n");
                exit(EXIT_FAILURE);
            }
            memset(buffer->frames[i].page, 0x00, PGSIZE);
            buffer->frames[i].page_num = 0;
            buffer->frames[i].nextLRU = buffer->frames[i].prevLRU = -1;
            buffer->frames[i].table_id = buffer->frames[i].is_pinned 
                = buffer->frames[i].is_dirty = buffer->frames[i].is_buf = 0;
        }
        buffer->num_frames = 0;
        buffer->num_bufs = num_buf;
        buffer->firstLRU = buffer->lastLRU = -1;
        return 0;
    }
    return 1;
}

int64_t file_open_via_buffer(char *pathname) {
    int64_t table_id;
    table_id = file_open_table_file(pathname);
    if(table_id < 0) return -1;
    return table_id;
}

pagenum_t buffer_alloc_page(int64_t table_id) {
    int hit, new_idx;
    pagenum_t new_pagenum = 0;
    
    hit = hit_idx(table_id, 0);

    // case 1: header page is in buffer.
    if(hit>=0) {
        buffer->frames[hit].is_pinned++;
        
        if(!buffer->frames[hit].page->nextfree_num) {
            if(buffer->frames[hit].is_dirty) file_write_page(table_id, 0, buffer->frames[hit].page);
            buffer->frames[hit].is_dirty = 0;
            new_pagenum = file_alloc_page(table_id); // file size become twice

            if(buffer->num_frames < buffer->num_bufs) new_idx = find_empty_frame(table_id, new_pagenum);
            else new_idx = give_idx();
            buffer->frames[new_idx].is_buf = 1;
            buffer->frames[new_idx].is_dirty = 0;
            buffer->frames[new_idx].is_pinned = 0;
            buffer->frames[new_idx].table_id = table_id;

            file_read_page(table_id, 0, buffer->frames[hit].page);
        } else {
            new_pagenum = buffer->frames[hit].page->nextfree_num;

            if(buffer->num_frames < buffer->num_bufs) new_idx = find_empty_frame(table_id, new_pagenum);
            else new_idx = give_idx();
            buffer->frames[new_idx].is_buf = 1;
            buffer->frames[new_idx].is_dirty = 0;
            buffer->frames[new_idx].is_pinned = 0;
            buffer->frames[new_idx].table_id = table_id;

            file_read_page(table_id, new_pagenum, buffer->frames[new_idx].page);
            buffer->frames[hit].page->nextfree_num = buffer->frames[new_idx].page->nextfree_num;
            buffer->frames[hit].is_dirty = 1;
        }
        buffer->frames[new_idx].page_num = new_pagenum;
        buffer->frames[hit].is_pinned--;

        return new_pagenum;
    }

    // case 2: header page is not in buffer.
    new_pagenum = file_alloc_page(table_id);
    if(buffer->num_frames<buffer->num_bufs) hit = find_empty_frame(table_id, new_pagenum);
    else hit = give_idx();
    buffer->frames[hit].is_pinned = 1;
    buffer->frames[hit].is_dirty = 0;
    buffer->frames[hit].is_buf = 1;
    buffer->frames[hit].page_num = 0;
    buffer->frames[hit].table_id = table_id;
    file_read_page(table_id, 0, buffer->frames[hit].page);
    
    if(buffer->num_frames < buffer->num_bufs) new_idx = find_empty_frame(table_id, new_pagenum);
    else new_idx = give_idx();
    buffer->frames[new_idx].is_buf = 1;
    buffer->frames[new_idx].is_dirty = 0;
    buffer->frames[new_idx].is_pinned = 0;
    buffer->frames[new_idx].table_id = table_id;
    buffer->frames[new_idx].page_num = new_pagenum;

    buffer->frames[hit].is_pinned--;
    return new_pagenum;    
}

void buffer_free_page(int64_t table_id, pagenum_t pagenum, int32_t idx) {
    int hit, new_idx;
    if(buffer->frames[idx].table_id!=table_id || buffer->frames[idx].page_num!=pagenum) {
        perror("BUFFER FREE PAGE FAILED!! : LOGIC IS WRONG!!\n");
        exit(EXIT_FAILURE);
    }
    memset(buffer->frames[idx].page, 0x00, PGSIZE);
    buffer->frames[idx].is_buf = buffer->frames[idx].is_dirty 
        = buffer->frames[idx].is_pinned = 0;
    buffer->frames[idx].table_id = 0;
    buffer->frames[idx].page_num = 0;
    delete_LRU(idx);

    hit = hit_idx(table_id, 0);
    // case 1: header page is in buffer.
    if(hit>=0) {
        buffer->frames[idx].page->nextfree_num = buffer->frames[hit].page->nextfree_num;
        file_write_page(table_id, pagenum, buffer->frames[idx].page);
        buffer->frames[hit].page->nextfree_num = pagenum;
        buffer->frames[hit].is_dirty = 1;
        return;
    }

    // case 2: header page is not in buffer.
    file_free_page(table_id, pagenum);
    hit = find_empty_frame(table_id, 0);
    buffer->frames[hit].is_dirty = 0;
    buffer->frames[hit].is_buf = 1;
    buffer->frames[hit].page_num = 0;
    buffer->frames[hit].is_pinned = 0;
    buffer->frames[hit].table_id = table_id;
    file_read_page(table_id, 0, buffer->frames[hit].page);
}

int buffer_get_idx(int64_t table_id, pagenum_t pagenum) {
    int hit;
    hit = hit_idx(table_id, pagenum);
    if(hit>=0) {
        buffer->frames[hit].is_buf = 1;
        return hit;
    }
    if(buffer->num_frames<buffer->num_bufs) hit = find_empty_frame(table_id, pagenum);
    else hit = give_idx();

    file_read_page(table_id, pagenum, buffer->frames[hit].page);
    buffer->frames[hit].is_buf = 1;
    buffer->frames[hit].is_pinned = buffer->frames[hit].is_dirty = 0;
    buffer->frames[hit].table_id = table_id;
    buffer->frames[hit].page_num = pagenum;
    return hit;
}

page_t* buffer_read_page(int64_t table_id, pagenum_t pagenum, int32_t idx) {
    if(buffer->frames[idx].table_id!=table_id || buffer->frames[idx].page_num!=pagenum) {
        perror("BUFFER READ PAGE FAILED!! : LOGIC IS WRONG!!\n");
        exit(EXIT_FAILURE);
    }
    return buffer->frames[idx].page;
}

void buffer_write_page(int64_t table_id, pagenum_t pagenum, int32_t idx) {
    if(buffer->frames[idx].table_id!=table_id || buffer->frames[idx].page_num!=pagenum) {
        perror("BUFFER WRITE PAGE FAILED!! : LOGIC IS WRONG!!\n");
        exit(EXIT_FAILURE);
    }
    buffer->frames[idx].is_dirty = 1;
}

int shutdown_buffer() {
    for(int i=0; i<buffer->num_bufs; i++) {
        if(buffer->frames[i].is_buf && buffer->frames[i].is_dirty) {
            file_write_page( buffer->frames[i].table_id, buffer->frames[i].page_num, buffer->frames[i].page);
        }
        free(buffer->frames[i].page);
        buffer->frames[i].page = NULL;
    }
    free(buffer->frames);
    buffer->frames = NULL;
    free(buffer);
    buffer = NULL;
    file_close_table_files();
    return 0;
}