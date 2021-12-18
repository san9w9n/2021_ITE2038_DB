#include "buffer.h"

frame_t* frames;
int num_frames;
int num_bufs;
int firstLRU;
int lastLRU;
pthread_mutex_t buf_mutex;

int buf_hashFunction(int64_t table_id, pagenum_t pagenum) {
  int ret = std::hash<int64_t>()(table_id) ^ std::hash<int64_t>()(pagenum);
  return ret % num_bufs;
}

page_t* buffer_read_page_without_latch(int page_idx) { return frames[page_idx].page; }

int find_empty_frame(int64_t table_id, pagenum_t pagenum) {
  int i, tablesize;
  tablesize = num_bufs;
  i = buf_hashFunction(table_id, pagenum);
  while (frames[i].is_buf)
    i = (i + 1) % tablesize;
  append_LRU(i);
  return i;
}

int hit_idx(int64_t table_id, pagenum_t pagenum) {
  int i, hashValue, tablesize;
  tablesize = num_bufs;
  hashValue = buf_hashFunction(table_id, pagenum);
  i = hashValue;
  while (1) {
    if (frames[i].is_buf && frames[i].table_id == table_id &&
        frames[i].page_num == pagenum) {
      delete_append_LRU(i);
      return i;
    }
    i = (i + 1) % tablesize;
    if (i == hashValue) break;
  }
  return -1;
}

int isValid(int64_t table_id) {
  if (!frames || !isValid_table_id(table_id)) return 0;
  return 1;
}

void append_LRU(int idx) {
  if (!num_frames) {
    firstLRU = lastLRU = idx;
    frames[idx].prevLRU = frames[idx].nextLRU = -1;
    num_frames++;
    return;
  }
  if (lastLRU >= 0) frames[lastLRU].nextLRU = idx;
  frames[idx].prevLRU = lastLRU;
  frames[idx].nextLRU = -1;
  lastLRU = idx;
  num_frames++;
}

void delete_LRU(int idx) {
  if (firstLRU == idx) firstLRU = frames[idx].nextLRU;
  if (lastLRU == idx) lastLRU = frames[idx].prevLRU;
  if (frames[idx].prevLRU >= 0)
    frames[frames[idx].prevLRU].nextLRU = frames[idx].nextLRU;
  if (frames[idx].nextLRU >= 0)
    frames[frames[idx].nextLRU].prevLRU = frames[idx].prevLRU;
  frames[idx].nextLRU = frames[idx].prevLRU = -1;
  num_frames--;
}

void delete_append_LRU(int idx) {
  if (lastLRU == idx) return;
  delete_LRU(idx);
  append_LRU(idx);
}

int give_idx() {
  int ret_idx = -1;
  int i = firstLRU;

  while (ret_idx < 0) {
    if (frames[i].state == UNLOCKED) {
      if (frames[i].is_dirty) {
        log_flush();
        file_write_page(frames[i].table_id, frames[i].page_num, frames[i].page);
      }
      memset(frames[i].page, 0x00, PGSIZE);
      ret_idx = i;
    }
    i = frames[i].nextLRU;
    if (i < 0)
      i = firstLRU;
  }
  delete_append_LRU(ret_idx);
  return ret_idx;
}

int init_buffer(int num_buf) {
  if (!frames) {
    buf_mutex = PTHREAD_MUTEX_INITIALIZER;
    if (num_buf < 20) num_buf = 20;
    frames = (frame_t*)malloc(sizeof(frame_t) * num_buf);
    for (int i = 0; i < num_buf; i++) {
      frames[i].page = (page_t*)malloc(PGSIZE);
      memset(frames[i].page, 0x00, PGSIZE);
      frames[i].page_num = 0;
      frames[i].nextLRU = frames[i].prevLRU = -1;
      frames[i].table_id = frames[i].is_dirty = frames[i].is_buf = 0;
      frames[i].page_mutex = PTHREAD_MUTEX_INITIALIZER;
      frames[i].state = UNLOCKED;
    }
    num_frames = 0;
    num_bufs = num_buf;
    firstLRU = lastLRU = -1;
    return 0;
  }
  return 1;
}

int64_t file_open_via_buffer(char* pathname) {
  int64_t table_id;
  table_id = file_open_table_file(pathname);
  if (table_id < 0) return -1;
  return table_id;
}

pagenum_t buffer_alloc_page(int64_t table_id) {
  int hit, new_idx;
  pagenum_t new_pagenum = 0;

  LOCK(buf_mutex);
  hit = hit_idx(table_id, 0);
  if (hit >= 0) {
    frames[hit].state = LOCKED;
    LOCK(frames[hit].page_mutex);

    if (!frames[hit].page->nextfree_num) {
      if (frames[hit].is_dirty) file_write_page(table_id, 0, frames[hit].page);
      frames[hit].is_dirty = 0;
      new_pagenum = file_alloc_page(table_id);  // file size become twice

      if (num_frames < num_bufs)
        new_idx = find_empty_frame(table_id, new_pagenum);
      else
        new_idx = give_idx();

      frames[new_idx].state = LOCKED;
      LOCK(frames[new_idx].page_mutex);
      UNLOCK(buf_mutex);

      frames[new_idx].is_buf = 1;
      frames[new_idx].is_dirty = 0;
      frames[new_idx].table_id = table_id;

      file_read_page(table_id, 0, frames[hit].page);
    }

    // subcase 2 : File has a freepage.
    else {
      new_pagenum = frames[hit].page->nextfree_num;
      if (num_frames < num_bufs)
        new_idx = find_empty_frame(table_id, new_pagenum);
      else
        new_idx = give_idx();

      frames[new_idx].state = LOCKED;
      LOCK(frames[new_idx].page_mutex);
      UNLOCK(buf_mutex);

      frames[new_idx].is_buf = 1;
      frames[new_idx].is_dirty = 0;
      frames[new_idx].table_id = table_id;

      file_read_page(table_id, new_pagenum, frames[new_idx].page);
      frames[hit].page->nextfree_num = frames[new_idx].page->nextfree_num;
      frames[hit].is_dirty = 1;
    }

    frames[new_idx].page_num = new_pagenum;
    frames[new_idx].page->LSN = 0;
    frames[hit].state = UNLOCKED;
    UNLOCK(frames[hit].page_mutex);

    frames[new_idx].state = UNLOCKED;
    UNLOCK(frames[new_idx].page_mutex);

    return new_pagenum;
  }

  // case 2: header page is not in buffer.
  new_pagenum = file_alloc_page(table_id);
  if (num_frames < num_bufs)
    hit = find_empty_frame(table_id, new_pagenum);
  else
    hit = give_idx();
  frames[hit].state = LOCKED;
  LOCK(frames[hit].page_mutex);

  frames[hit].is_dirty = 0;
  frames[hit].is_buf = 1;
  frames[hit].page_num = 0;
  frames[hit].table_id = table_id;
  file_read_page(table_id, 0, frames[hit].page);

  if (num_frames < num_bufs)
    new_idx = find_empty_frame(table_id, new_pagenum);
  else
    new_idx = give_idx();

  frames[new_idx].state = LOCKED;
  LOCK(frames[new_idx].page_mutex);
  UNLOCK(buf_mutex);

  frames[new_idx].is_buf = 1;
  frames[new_idx].is_dirty = 0;
  frames[new_idx].table_id = table_id;
  frames[new_idx].page_num = new_pagenum;
  frames[new_idx].page->LSN = 0;

  UNLOCK(frames[hit].page_mutex);
  UNLOCK(frames[new_idx].page_mutex);

  return new_pagenum;
}

void buffer_free_page(int64_t table_id, pagenum_t pagenum, int32_t idx) {
  int hit;
  LOCK(buf_mutex);
  memset(frames[idx].page, 0x00, PGSIZE);
  frames[idx].is_buf = frames[idx].is_dirty = 0;
  frames[idx].table_id = 0;
  frames[idx].page_num = 0;
  delete_LRU(idx);

  hit = hit_idx(table_id, 0);
  // case 1: header page is in buffer.
  if (hit >= 0) {
    frames[hit].state = LOCKED;
    LOCK(frames[hit].page_mutex);
    UNLOCK(buf_mutex);
    frames[idx].page->nextfree_num = frames[hit].page->nextfree_num;
    file_write_page(table_id, pagenum, frames[idx].page);
    frames[hit].page->nextfree_num = pagenum;
    frames[hit].is_dirty = 1;
    UNLOCK(frames[hit].page_mutex);
    UNLOCK(frames[idx].page_mutex);
    frames[idx].state = UNLOCKED;
    frames[hit].state = UNLOCKED;
    return;
  }

  // case 2: header page is not in buffer.
  UNLOCK(frames[idx].page_mutex);
  frames[idx].state = UNLOCKED;

  file_free_page(table_id, pagenum);
  hit = find_empty_frame(table_id, 0);
  frames[hit].state = LOCKED;
  LOCK(frames[hit].page_mutex);
  UNLOCK(buf_mutex);
  frames[hit].is_dirty = 0;
  frames[hit].is_buf = 1;
  frames[hit].page_num = 0;
  frames[hit].table_id = table_id;
  file_read_page(table_id, 0, frames[hit].page);
  UNLOCK(frames[hit].page_mutex);
  frames[hit].state = UNLOCKED;
}

page_t* buffer_read_page(int64_t table_id, pagenum_t pagenum, int* idx,
                         bool mode) {
  int hit;
  page_t* ret;
  int flag = 0;

RETRY:
  LOCK(buf_mutex);
  hit = hit_idx(table_id, pagenum);
  if (hit >= 0) {
    if(pthread_mutex_trylock(&frames[hit].page_mutex)) {
      UNLOCK(buf_mutex);
      goto RETRY;
    }
    frames[hit].state = LOCKED;
    UNLOCK(buf_mutex);
    *idx = hit;
    frames[hit].is_buf = 1;
    ret = frames[hit].page;
    if (mode == READ) {
      frames[hit].state = UNLOCKED;
      UNLOCK(frames[hit].page_mutex);
    }
    return ret;
  }
  if (num_frames < num_bufs)
    hit = find_empty_frame(table_id, pagenum);
  else {
    hit = give_idx();
  }
  if(pthread_mutex_trylock(&frames[hit].page_mutex)) {
    UNLOCK(buf_mutex);
    goto RETRY;
  }
  frames[hit].state = LOCKED;
  UNLOCK(buf_mutex);
  *idx = hit;
  file_read_page(table_id, pagenum, frames[hit].page);
  frames[hit].is_buf = 1;
  frames[hit].is_dirty = 0;
  frames[hit].table_id = table_id;
  frames[hit].page_num = pagenum;
  ret = frames[hit].page;
  if (mode == READ) {
    frames[hit].state = UNLOCKED;
    UNLOCK(frames[hit].page_mutex);
  }
  return ret;
}

void buffer_write_page(int64_t table_id, pagenum_t pagenum, int32_t idx,
                       bool success) {
  if (success) frames[idx].is_dirty = 1;
  frames[idx].state = UNLOCKED;
  UNLOCK(frames[idx].page_mutex);
}

void buffer_flush()
{
  for (int i = 0; i < num_bufs; i++) {
    if (frames[i].is_buf && frames[i].is_dirty) 
      file_write_page(frames[i].table_id, frames[i].page_num, frames[i].page);
    frames[i].page_num = 0;
    frames[i].nextLRU = frames[i].prevLRU = -1;
    frames[i].table_id = frames[i].is_dirty = frames[i].is_buf = 0;
    frames[i].page_mutex = PTHREAD_MUTEX_INITIALIZER;
    frames[i].state = UNLOCKED;
  }
  buf_mutex = PTHREAD_MUTEX_INITIALIZER;
  num_frames = 0;
  firstLRU = lastLRU = -1;
}

int shutdown_buffer() {
  for (int i = 0; i < num_bufs; i++) {
    if (frames[i].is_buf && frames[i].is_dirty) {
      log_flush();
      file_write_page(frames[i].table_id, frames[i].page_num, frames[i].page);
    }
    free(frames[i].page);
    frames[i].page = NULL;
  }
  free(frames);
  frames = NULL;
  file_close_table_files();
  return shutdown_log();
}