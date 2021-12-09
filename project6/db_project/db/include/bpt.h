#ifndef __BPT_H__
#define __BPT_H__
#include "trx.h"

// API
int64_t open_table(char *pathname);
int shutdown_db();
int db_insert(int64_t table_id, int64_t key, char * value, uint16_t val_size);
int db_delete(int64_t table_id, int64_t key);
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t *val_size, int trx_id);
int db_update(int64_t table_id, int64_t key, char* values, uint16_t new_val_size, uint16_t* old_val_size, int trx_id);

// Insert
int cut(int length);
void valueCopy(char* src, page_t* dest, int16_t size, int16_t offset);
pagenum_t find_leaf(int64_t table_id, pagenum_t root_num, int64_t key);
int insert_into_internal(int64_t table_id, uint32_t index, pagenum_t parent_num, page_t *parent, int32_t parent_idx, pagenum_t l_num, page_t* l, int32_t l_idx, pagenum_t r_num, page_t* r, int32_t r_idx, int64_t key);
int insert_into_internal_after_splitting(int64_t table_id, uint32_t index, pagenum_t parent_num, page_t *parent, int32_t parent_idx, pagenum_t r_num, int64_t key);
int insert_into_parent(int64_t table_id, pagenum_t l_num, page_t* l, int32_t l_idx, pagenum_t r_num, page_t* r, int32_t r_idx, int64_t key);
int insert_into_leaf(int64_t table_id, uint32_t index, pagenum_t leaf_num, page_t* leaf, int leaf_idx, int64_t key, char * value, uint16_t val_size);
int insert_into_leaf_after_splitting(int64_t table_id, uint32_t index, pagenum_t leaf_num, page_t* leaf, int leaf_idx, int64_t key, char* value, uint16_t val_size);
int start_new_tree(int64_t table_id, int64_t key, char * value, uint16_t val_size);

// Delete
int get_my_index(int64_t table_id, pagenum_t pagenum, page_t* page);
void compact_value(int64_t table_id, page_t* leaf, int32_t leaf_idx);
void delete_leaf(int64_t table_id, uint32_t index, pagenum_t leaf_num, page_t* leaf, int32_t leaf_idx, int64_t key);
void delete_internal(int64_t table_id, uint32_t index, pagenum_t page_num, page_t* page, int32_t page_idx, int64_t key);
int adjust_root(int64_t table_id, pagenum_t root_num, page_t* root, int32_t root_idx, int64_t key);
int coalesce_leaf(int64_t table_id, int my_index, pagenum_t parent_num, page_t* parent, int32_t parent_idx, pagenum_t sibling_num, page_t* sibling, int32_t sibling_idx, pagenum_t leaf_num, page_t* leaf, int32_t leaf_idx);
int redistribute_leaf(int64_t table_id, pagenum_t parent_num, page_t* parent, int32_t parent_idx, pagenum_t sibling_num, page_t* sibling, int32_t sibling_idx, pagenum_t leaf_num, page_t* leaf, int32_t leaf_idx, int my_index);
int coalesce_internal(int64_t table_id, int my_index, pagenum_t parent_num, page_t* parent, int32_t parent_idx, pagenum_t sibling_num, page_t* sibling, int32_t sibling_idx, pagenum_t page_num, page_t* page, int32_t page_idx);
int redistribute_internal(int64_t table_id, pagenum_t parent_num, page_t* parent, int32_t parent_idx, pagenum_t sibling_num, page_t* sibling, int32_t sibling_idx, pagenum_t page_num, page_t* page, int32_t page_idx, int my_index);
int delete_entry(int64_t table_id, pagenum_t page_num, page_t* page, int32_t page_idx, int64_t key);

#endif