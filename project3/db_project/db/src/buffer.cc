#include "buffer.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

buffer_pool_t* buffer = NULL;

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

int find_empty_frame() {
    int i=0;
    for(i=0; i<buffer->num_bufs; i++) {
        if(!buffer->frames[i].is_buf) break;
    }
    if(i==buffer->num_bufs) {
        perror("FIND EMPTY FRAME FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    append_LRU(i);
    return i;
}

void manage_pin(int32_t idx, int32_t flag) {
    if(flag) buffer->frames[idx].is_pinned++;
    else {
        if(buffer->frames[idx].is_pinned>0) 
            buffer->frames[idx].is_pinned--;
    }
}

int hit_idx(int64_t table_id, pagenum_t pagenum) {
    int32_t point;
    point = buffer->lastLRU;
    while(point>=0) {
        if(buffer->frames[point].table_id == table_id 
            && buffer->frames[point].page_num == pagenum) {
            delete_append_LRU(point);
            return point;
        }
        point = buffer->frames[point].prevLRU;
    }
    return -1;
}

int give_idx() {
    int ret_idx = -1, pinned_cnt = 0, CLK = buffer->firstLRU;
    while(ret_idx<0) {
        if(buffer->frames[CLK].is_pinned) {
            pinned_cnt++;
            if(pinned_cnt>=buffer->num_bufs) {
                perror("ALL PINNED!!\n");
                exit(EXIT_FAILURE);
            }
        } else {
            if(!buffer->frames[CLK].is_ref) {
                if(buffer->frames[CLK].is_dirty) 
                    file_write_page(buffer->frames[CLK].table_id, buffer->frames[CLK].page_num, buffer->frames[CLK].page);
                memset(buffer->frames[CLK].page, 0x00, PGSIZE);
                ret_idx = CLK;
            } 
            else buffer->frames[CLK].is_ref = 0;
        }
        CLK = buffer->frames[CLK].nextLRU;
        if(CLK<0) {
            CLK = buffer->firstLRU;
            pinned_cnt = 0;
        }
    }
    delete_append_LRU(ret_idx);
    return ret_idx;
}

int init_buffer(int num_buf) {
    //buffer생성
    if(!buffer) {
        buffer = (buffer_pool_t*)malloc(sizeof(buffer_pool_t));
        if(!buffer) {
            perror("MALLOC FAILED!!\n");
            exit(EXIT_FAILURE);
        }
        buffer->frames = (frame_t*)malloc(sizeof(frame_t)*num_buf);
        if(!buffer->frames) {
            perror("MALLOC FAILED!!\n");
            exit(EXIT_FAILURE);
        }
        for(int i=0; i<num_buf; i++) {
            buffer->frames[i].page = (page_t*)malloc(sizeof(page_t));
            if(!buffer->frames[i].page) {
                perror("MALLOC FAILED!!\n");
                exit(EXIT_FAILURE);
            }
            memset(buffer->frames[i].page, 0x00, PGSIZE);
            buffer->frames[i].page_num = 0;
            buffer->frames[i].nextLRU = buffer->frames[i].prevLRU = -1;
            buffer->frames[i].table_id = buffer->frames[i].is_ref = buffer->frames[i].is_pinned 
                = buffer->frames[i].is_dirty = buffer->frames[i].is_buf = 0;
        }
        buffer->num_frames = 0;
        buffer->num_bufs = num_buf;
        buffer->firstLRU = buffer->lastLRU = -1;
        return 0;
    }
    return 1;
}

int64_t file_open_via_bufer(char *pathname) {
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
        if(buffer->num_frames < buffer->num_bufs) new_idx = find_empty_frame();
        else new_idx = give_idx();
        buffer->frames[new_idx].is_buf = buffer->frames[new_idx].is_ref = 1;
        buffer->frames[new_idx].is_dirty = 0;
        buffer->frames[new_idx].is_pinned = 0;
        buffer->frames[new_idx].table_id = table_id;

        if(!buffer->frames[hit].page->nextfree_num) {
            if(buffer->frames[hit].is_dirty) file_write_page(table_id, 0, buffer->frames[hit].page);
            buffer->frames[hit].is_dirty = 0;
            new_pagenum = file_alloc_page(table_id); // file size become twice
            file_read_page(table_id, 0, buffer->frames[hit].page);
        } else {
            new_pagenum = buffer->frames[hit].page->nextfree_num;
            file_read_page(table_id, new_pagenum, buffer->frames[new_idx].page);
            buffer->frames[hit].page->nextfree_num = buffer->frames[new_idx].page->nextfree_num;
            buffer->frames[hit].is_dirty = 1;
        }
        buffer->frames[new_idx].page_num = new_pagenum;
        buffer->frames[hit].is_pinned--;
        buffer->frames[hit].is_ref = 1;

        return new_pagenum;
    }

    // case 2: header page is not in buffer.
    new_pagenum = file_alloc_page(table_id);
    if(buffer->num_frames<buffer->num_bufs) hit = find_empty_frame();
    else hit = give_idx();
    buffer->frames[hit].is_pinned = 1;
    buffer->frames[hit].is_dirty = 0;
    buffer->frames[hit].is_buf = buffer->frames[hit].is_ref = 1;
    buffer->frames[hit].page_num = 0;
    buffer->frames[hit].table_id = table_id;
    file_read_page(table_id, 0, buffer->frames[hit].page);
    
    if(buffer->num_frames < buffer->num_bufs) new_idx = find_empty_frame();
    else new_idx = give_idx();
    buffer->frames[new_idx].is_buf = buffer->frames[new_idx].is_ref = 1;
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
        = buffer->frames[idx].is_pinned = buffer->frames[idx].is_ref = 0;
    buffer->frames[idx].table_id = 0;
    buffer->frames[idx].page_num = 0;
    delete_LRU(idx);

    hit = hit_idx(table_id, 0);
    // case 1: header page is in buffer.
    if(hit>=0) {
        buffer->frames[hit].page->nextfree_num = pagenum;
        buffer->frames[hit].is_dirty = buffer->frames[hit].is_ref = 1;
        return;
    }
    // case 2: header page is not in buffer.
    file_free_page(table_id, pagenum);
    hit = find_empty_frame();
    buffer->frames[hit].is_dirty = 0;
    buffer->frames[hit].is_buf = buffer->frames[hit].is_ref = 1;
    buffer->frames[hit].page_num = 0;
    buffer->frames[hit].is_pinned = 0;
    buffer->frames[hit].table_id = table_id;
    file_read_page(table_id, 0, buffer->frames[hit].page);
}

int buffer_get_idx(int64_t table_id, pagenum_t pagenum) {
    int hit;
    hit = hit_idx(table_id, pagenum);
    if(hit>=0) {
        buffer->frames[hit].is_buf = buffer->frames[hit].is_ref = 1;
        return hit;
    }
    if(buffer->num_frames<buffer->num_bufs) hit = find_empty_frame();
    else hit = give_idx();

    file_read_page(table_id, pagenum, buffer->frames[hit].page);
    buffer->frames[hit].is_buf = buffer->frames[hit].is_ref = 1;
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