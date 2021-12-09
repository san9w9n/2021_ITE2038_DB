#include <stdlib.h>

#include "bpt.h"

#define INITIAL_FREE 3968
#define THRESHOLD 2500
#define MAX_ORDER 249
#define PGSIZE 4096

int64_t open_table(char *pathname) {
    return file_open_via_buffer(pathname);
}

int shutdown_db() {
    return shutdown_trx();
}

int cut(int length) {
    if(length%2==0) return length/2;
    return length/2+1;
}

void valueCopy(char* src, page_t* dest, int16_t size, int16_t offset) {
    offset-=128;
    for(int i=0, j=offset; i<size; i++, j++) {
        dest->leafbody.value[j] = src[i];
    }
}

pagenum_t find_leaf(int64_t table_id, pagenum_t root_num, int64_t key) {
    pagenum_t ret_num = root_num;
    int32_t root_idx, next_idx;
    page_t* root;
    root = buffer_read_page(table_id, root_num, &root_idx, READ);

    while(!root->info.isLeaf) {
        if(key<root->branch[0].key) ret_num = root->leftmost;
        else {
            uint32_t i=0;
            for(i=0; i<root->info.num_keys-1; i++) {
                if(key<root->branch[i+1].key) break;
            }
            ret_num = root->branch[i].pagenum;
        }
        root = buffer_read_page(table_id, ret_num, &next_idx, READ);
    }

    return ret_num;
}

int 
db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t *val_size, int trx_id)
{
  trx_t*        trx;
  page_t*       header;
  page_t*       page;
  pagenum_t     page_id;
  int           header_idx;
  int           page_idx;
  int           flag;
  int           key_index;
  uint16_t      size;
  uint16_t      offset;

  if(!isValid(table_id))
    return 1;
  if(!(trx = give_trx(trx_id)))
    return 1;

  header = buffer_read_page(table_id, 0, &header_idx, READ);
  page_id = header->root_num;
  if(!page_id) return 1;

  page = buffer_read_page(table_id, page_id, &page_idx, READ);

  while(!page->info.isLeaf) {
    if(key<page->branch[0].key) page_id = page->leftmost;
    else {
      uint32_t i=0;
      for(i=0; i<page->info.num_keys-1; i++) 
        if(key<page->branch[i+1].key) break;
      page_id = page->branch[i].pagenum;
    }
    page = buffer_read_page(table_id, page_id, &page_idx, READ);
  }
  
  for(key_index=0; key_index<page->info.num_keys; key_index++) {
    if(page->leafbody.slot[key_index].key == key) break;
  }
  if(key_index == page->info.num_keys) return 1;

  flag = lock_acquire(table_id, page_id, key, key_index, trx_id, SHARED);
  if(flag == DEADLOCK) {
    trx_abort(trx_id);
    return 1;
  }

  page = buffer_read_page(table_id, page_id, &page_idx, READ);
  offset = page->leafbody.slot[key_index].offset-128;
  size = page->leafbody.slot[key_index].size;
  for(int i=offset, j=0; i<offset+size; j++, i++)
    ret_val[j] = page->leafbody.value[i];
  *val_size = size;

  return 0;
}

int
db_update(int64_t table_id, int64_t key, char* values, uint16_t new_val_size, uint16_t* old_val_size, int trx_id)
{
  page_t*                 header;
  page_t*                 page;
  int                     header_idx;
  int                     page_idx;
  int                     key_index;
  int                     flag;
  pagenum_t               page_id;
  trx_t*                  trx;
  trx_t*                  impl_trx;
  entry_t*                entry;
  undo_t*                 undo;
  uint16_t                offset;
  uint16_t                size;
  char*                   old_value;
	main_log_t*							main_log;
	update_log_t*						update_log_t;
	char*										old_img;
	char*										new_img;


  if(!isValid(table_id))
    return 1;
  if(!(trx = give_trx(trx_id)))
    return 1;

  header = buffer_read_page(table_id, 0, &header_idx, READ);
  page_id = header->root_num;
  if(!page_id) return 1;

  page = buffer_read_page(table_id, page_id, &page_idx, READ);

  while(!page->info.isLeaf) {
    if(key<page->branch[0].key) page_id = page->leftmost;
    else {
      uint32_t i=0;
      for(i=0; i<page->info.num_keys-1; i++) 
        if(key<page->branch[i+1].key) break;
      page_id = page->branch[i].pagenum;
    }
    page = buffer_read_page(table_id, page_id, &page_idx, READ);
  }

  for(key_index=0; key_index<page->info.num_keys; key_index++) {
    if(page->leafbody.slot[key_index].key == key) break;
  }
  if(key_index == page->info.num_keys) return 1;
  

  flag = lock_acquire(table_id, page_id, key, key_index, trx_id, EXCLUSIVE);
  if(flag == DEADLOCK) {
    trx_abort(trx_id);
    return 1;
  }

  page = buffer_read_page(table_id, page_id, &page_idx, WRITE);
  offset = page->leafbody.slot[key_index].offset-128;
  size = page->leafbody.slot[key_index].size;
  *old_val_size = size;
  old_value = new char[size + 1];
  for(int i=offset, j=0; i<offset+size; j++,i++)
    old_value[j] = page->leafbody.value[i];

	main_log = make_main_log(trx_id, UPDATE, MAINLOG + UPDATELOG + 2*size, trx->last_LSN);
	update_log_t = make_update_log(table_id, page_id, size, offset + 128);
	page->LSN = main_log->LSN;
	old_img = new char[size + 2]();
	new_img = new char[size + 2]();
	for(int i=0; i<size; i++) {
		old_img[i] = page->leafbody.value[i+offset];
		new_img[i] = values[i];
	}
	trx->last_LSN = main_log->LSN;
	push_update_stack(trx_id, main_log->LSN);
	push_log_to_buffer(main_log, update_log_t, old_img, new_img, 0);

  for(int i=offset, j=0; i<offset+new_val_size; j++, i++)
    page->leafbody.value[i] = values[j];
	
  page->leafbody.slot[key_index].size = new_val_size;
  buffer_write_page(table_id, page_id, page_idx, 1);

  trx = give_trx(trx_id);
	undo = new undo_t;
	undo->old_value = new char[size+1];
	undo->table_id = table_id;
	undo->key = key;
	undo->val_size = size;
	for(int k=0; k<size; k++) 
		undo->old_value[k] = old_value[k];
	trx->undo_stack.push(undo);
	
  delete[] old_value;
  
  return 0;
}

int insert_into_internal(int64_t table_id, uint32_t index, pagenum_t parent_num, page_t *parent, int32_t parent_idx, pagenum_t l_num, page_t* l, int32_t l_idx, pagenum_t r_num, page_t* r, int32_t r_idx, int64_t key) {
    for(uint32_t i=parent->info.num_keys-1; i>=index; i--) {
        parent->branch[i+1].key = parent->branch[i].key;
        parent->branch[i+1].pagenum = parent->branch[i].pagenum;

        if(i==0) break;
    }
    parent->branch[index].key = key;
    parent->branch[index].pagenum = r_num;
    parent->info.num_keys++;

    l->parent_num = r->parent_num = parent_num;
    
    buffer_write_page(table_id, l_num, l_idx, 1);
    buffer_write_page(table_id, r_num, r_idx, 1);
    buffer_write_page(table_id, parent_num, parent_idx, 1);

    return 0;
}

int insert_into_internal_after_splitting(int64_t table_id, uint32_t index, pagenum_t parent_num, page_t *parent, int32_t parent_idx, pagenum_t r_num, int64_t key) {
    uint32_t num_keys;
    branch_t* tmp;
    pagenum_t new_parent_num;
    int64_t kprime;
    page_t *new_parent, *child;
    int32_t new_parent_idx, child_idx;

    tmp=(branch_t*)malloc(sizeof(branch_t) * 252);
    num_keys = parent->info.num_keys + 1;
    
    int split = cut(MAX_ORDER)-1, ret=1;
    for(uint32_t i=0, j=0; i<num_keys; i++, j++) {
        if(j==index) j++;
        tmp[j].key = parent->branch[i].key;
        tmp[j].pagenum = parent->branch[i].pagenum;
    }
    tmp[index].key = key;
    tmp[index].pagenum = r_num;

    new_parent_num = buffer_alloc_page(table_id);
    new_parent = buffer_read_page(table_id, new_parent_num, &new_parent_idx, WRITE);    
    new_parent->parent_num = parent->parent_num;
    new_parent->info.isLeaf = parent->info.isLeaf;
    new_parent->info.num_keys = parent->info.num_keys = 0;

    for(int i=0; i<split; i++) {
        parent->branch[i].key = tmp[i].key;
        parent->branch[i].pagenum = tmp[i].pagenum;
        parent->info.num_keys++;
    }
    new_parent->leftmost = tmp[split].pagenum;

    child = buffer_read_page(table_id, new_parent->leftmost, &child_idx, WRITE);
    child->parent_num = new_parent_num;    
    buffer_write_page(table_id, new_parent->leftmost, child_idx, 1);

    for(uint32_t i=split+1, j=0; i<num_keys; i++, j++) {
        new_parent->branch[j].key = tmp[i].key;
        new_parent->branch[j].pagenum = tmp[i].pagenum;

        child = buffer_read_page(table_id, tmp[i].pagenum, &child_idx, WRITE);
        child->parent_num = new_parent_num;
        buffer_write_page(table_id, tmp[i].pagenum, child_idx, 1);
        new_parent->info.num_keys++;
    }

    kprime = tmp[split].key;

    free(tmp);

    return insert_into_parent(table_id, parent_num, parent, parent_idx, new_parent_num, new_parent, new_parent_idx, kprime);
}

int insert_into_parent(int64_t table_id, pagenum_t l_num, page_t* l, int32_t l_idx, pagenum_t r_num, page_t* r, int32_t r_idx, int64_t key) {
    int32_t parent_idx;
    page_t* parent;
    pagenum_t parent_num;

    parent_num =  l->parent_num;
    if(!parent_num) {
        page_t *new_root, *header;
        pagenum_t new_root_num;
        int32_t new_root_idx, header_idx;
        new_root_num = buffer_alloc_page(table_id);
        new_root = buffer_read_page(table_id, new_root_num, &new_root_idx, WRITE);

        header = buffer_read_page(table_id, 0, &header_idx, WRITE);
        header->root_num = new_root_num;
        buffer_write_page(table_id, 0, header_idx, 1);
        
        l->parent_num = r->parent_num = new_root_num;
        new_root->parent_num = 0;
        new_root->info.isLeaf = 0;
        new_root->info.num_keys = 1;
        new_root->leftmost = l_num;
        new_root->branch[0].key = key;
        new_root->branch[0].pagenum = r_num;
        
        buffer_write_page(table_id, l_num, l_idx, 1);
        buffer_write_page(table_id, r_num, r_idx, 1);
        buffer_write_page(table_id, new_root_num, new_root_idx, 1);

        return 0;
    }
    parent = buffer_read_page(table_id, parent_num, &parent_idx, WRITE);

    uint32_t i;
    for(i=0; i<parent->info.num_keys; i++) {
        if(parent->branch[i].key>=key) break;
    }

    if(parent->info.num_keys < MAX_ORDER - 1) return insert_into_internal(table_id, i, parent_num, parent, parent_idx, l_num, l, l_idx, r_num, r, r_idx, key);

    buffer_write_page(table_id, l_num, l_idx, 1);
    buffer_write_page(table_id, r_num, r_idx, 1);
    return insert_into_internal_after_splitting(table_id, i, parent_num, parent, parent_idx, r_num, key);
}

int insert_into_leaf(int64_t table_id, uint32_t index, pagenum_t leaf_num, page_t* leaf, int leaf_idx, int64_t key, char * value, uint16_t val_size) {
    if(leaf->info.num_keys>0) {
        for(uint32_t i=leaf->info.num_keys-1; i>=index; i--) {
            leaf->leafbody.slot[i+1].key = leaf->leafbody.slot[i].key;
            leaf->leafbody.slot[i+1].offset = leaf->leafbody.slot[i].offset;
            leaf->leafbody.slot[i+1].size = leaf->leafbody.slot[i].size;
            leaf->leafbody.slot[i+1].trx_id = leaf->leafbody.slot[i].trx_id;

            if(i==0) break;
        }
    }
    leaf->info.num_keys++;
    leaf->freespace -= 16+val_size;
    leaf->leafbody.slot[index].key = key;
    leaf->leafbody.slot[index].offset = 128+(16*leaf->info.num_keys)+leaf->freespace;
    leaf->leafbody.slot[index].size = val_size;
    leaf->leafbody.slot[index].trx_id = 0;
    
    valueCopy(value, leaf, val_size, leaf->leafbody.slot[index].offset);
    buffer_write_page(table_id, leaf_num, leaf_idx, 1);
    return 0;
}

int insert_into_leaf_after_splitting(int64_t table_id, uint32_t index, pagenum_t leaf_num, page_t* leaf, int leaf_idx, int64_t key, char* value, uint16_t val_size) {
    uint32_t totalspace = 0, offset = PGSIZE, split=0, num_keys = leaf->info.num_keys;
    page_t *new_leaf, *old_leaf;
    int ret = 1, flag = 0;
    pagenum_t new_leaf_num;
    int32_t new_leaf_idx;

    for(split=0; split<leaf->info.num_keys; split++) {
        if(split==index) {
            totalspace+=16+val_size;
            flag = 1;
            if(totalspace>=INITIAL_FREE/2) {
                flag = 0;
                break;
            }
        } 
        totalspace+=16+leaf->leafbody.slot[split].size;
        if(totalspace>=INITIAL_FREE/2) {
            break;
        }
    }
    new_leaf_num = buffer_alloc_page(table_id);
    new_leaf = buffer_read_page(table_id, new_leaf_num, &new_leaf_idx, WRITE);
    
    new_leaf->parent_num = leaf->parent_num;
    new_leaf->info.isLeaf = leaf->info.isLeaf;
    new_leaf->info.num_keys = 0;
    new_leaf->freespace = INITIAL_FREE;
    new_leaf->Rsibling = leaf->Rsibling;

    old_leaf = (page_t*)malloc(sizeof(page_t));
    old_leaf->parent_num = leaf->parent_num;
    old_leaf->info.isLeaf = leaf->info.isLeaf;
    old_leaf->info.num_keys = 0;
    old_leaf->freespace = INITIAL_FREE;
    old_leaf->Rsibling = new_leaf_num;

    if(!flag) {
        uint32_t i=split, j=0;
        for(i=split, j=0; i<num_keys; i++, j++) {
            if(i==index) {
                new_leaf->leafbody.slot[j].key = key;
                new_leaf->leafbody.slot[j].trx_id = 0;

                if(j==0) new_leaf->leafbody.slot[j].offset = PGSIZE-val_size;
                else new_leaf->leafbody.slot[j].offset = new_leaf->leafbody.slot[j-1].offset-val_size;
    
                new_leaf->leafbody.slot[j].size = val_size;
                valueCopy(value, new_leaf, val_size, new_leaf->leafbody.slot[j].offset);
                new_leaf->freespace -= 16+val_size;
                new_leaf->info.num_keys++;
                j++;
            }
            new_leaf->leafbody.slot[j].key = leaf->leafbody.slot[i].key;

            if(j==0) new_leaf->leafbody.slot[j].offset = PGSIZE - leaf->leafbody.slot[i].size;
            else new_leaf->leafbody.slot[j].offset = new_leaf->leafbody.slot[j-1].offset - leaf->leafbody.slot[i].size;
            new_leaf->leafbody.slot[j].trx_id = leaf->leafbody.slot[i].trx_id;
            new_leaf->leafbody.slot[j].size = leaf->leafbody.slot[i].size;
            for(int k=0; k<leaf->leafbody.slot[i].size; k++) {
                new_leaf->leafbody.value[new_leaf->leafbody.slot[j].offset-128+k]
                    = leaf->leafbody.value[leaf->leafbody.slot[i].offset-128+k];
            }
            new_leaf->freespace -= 16+leaf->leafbody.slot[i].size;
            new_leaf->info.num_keys++;
        }
        if(index==num_keys) {
            new_leaf->leafbody.slot[j].key = key;
            new_leaf->leafbody.slot[j].trx_id = 0;
            if(j==0) new_leaf->leafbody.slot[j].offset = PGSIZE-val_size;
            else new_leaf->leafbody.slot[j].offset = new_leaf->leafbody.slot[j-1].offset-val_size;

            new_leaf->leafbody.slot[j].size = val_size;
            valueCopy(value, new_leaf, val_size, new_leaf->leafbody.slot[j].offset);
            new_leaf->freespace -= 16+val_size;
            new_leaf->info.num_keys++;
        }
        for(i=0; i<split; i++) {
            old_leaf->leafbody.slot[i].key = leaf->leafbody.slot[i].key;
            old_leaf->leafbody.slot[i].size = leaf->leafbody.slot[i].size;
            old_leaf->leafbody.slot[i].trx_id = leaf->leafbody.slot[i].trx_id;

            if(i==0) old_leaf->leafbody.slot[i].offset = PGSIZE - old_leaf->leafbody.slot[i].size;
            else old_leaf->leafbody.slot[i].offset = old_leaf->leafbody.slot[i-1].offset - old_leaf->leafbody.slot[i].size;

            for(int j=0; j<leaf->leafbody.slot[i].size; j++) {
                old_leaf->leafbody.value[old_leaf->leafbody.slot[i].offset-128+j]
                    = leaf->leafbody.value[leaf->leafbody.slot[i].offset-128+j];
            }
            old_leaf->freespace -= old_leaf->leafbody.slot[i].size+16;
            old_leaf->info.num_keys++;
        }
    } else {
        for(uint32_t i=split, j=0; i<num_keys; i++, j++) {
            new_leaf->leafbody.slot[j].key = leaf->leafbody.slot[i].key;
            new_leaf->leafbody.slot[j].trx_id = leaf->leafbody.slot[i].trx_id;

            if(j==0) new_leaf->leafbody.slot[j].offset = PGSIZE - leaf->leafbody.slot[i].size;
            else new_leaf->leafbody.slot[j].offset = new_leaf->leafbody.slot[j-1].offset - leaf->leafbody.slot[i].size;

            new_leaf->leafbody.slot[j].size = leaf->leafbody.slot[i].size;
            for(int k=0; k<leaf->leafbody.slot[i].size; k++) {
                new_leaf->leafbody.value[new_leaf->leafbody.slot[j].offset-128+k]
                    = leaf->leafbody.value[leaf->leafbody.slot[i].offset-128+k];
            }
            new_leaf->freespace -= 16+leaf->leafbody.slot[i].size;
            new_leaf->info.num_keys++;
        }
        for(uint32_t i=0, j=0; i<split; i++, j++) {
            if(i==index) {
                old_leaf->leafbody.slot[j].key = key;
                old_leaf->leafbody.slot[j].size = val_size;
                old_leaf->leafbody.slot[j].trx_id = 0;

                if(j==0) old_leaf->leafbody.slot[j].offset = PGSIZE - val_size;
                else old_leaf->leafbody.slot[j].offset = old_leaf->leafbody.slot[j-1].offset - val_size;

                valueCopy(value, old_leaf, val_size, old_leaf->leafbody.slot[j].offset);
                old_leaf->freespace -= val_size+16;
                old_leaf->info.num_keys++;
                j++;
            }
            old_leaf->leafbody.slot[j].key = leaf->leafbody.slot[i].key;
            old_leaf->leafbody.slot[j].size = leaf->leafbody.slot[i].size;
            old_leaf->leafbody.slot[j].trx_id = leaf->leafbody.slot[i].trx_id;

            if(j==0) old_leaf->leafbody.slot[j].offset = PGSIZE - old_leaf->leafbody.slot[j].size;
            else old_leaf->leafbody.slot[j].offset = old_leaf->leafbody.slot[j-1].offset - old_leaf->leafbody.slot[j].size;

            for(int k=0; k<old_leaf->leafbody.slot[j].size; k++) {
                old_leaf->leafbody.value[old_leaf->leafbody.slot[j].offset-128+k]
                    = leaf->leafbody.value[leaf->leafbody.slot[i].offset-128+k];
            }
            old_leaf->freespace -= old_leaf->leafbody.slot[j].size+16;
            old_leaf->info.num_keys++;
        }
        if(index==split) {
            old_leaf->leafbody.slot[index].key = key;
            old_leaf->leafbody.slot[index].trx_id = 0;

            if(index==0) old_leaf->leafbody.slot[index].offset = PGSIZE - val_size;
            else old_leaf->leafbody.slot[index].offset = old_leaf->leafbody.slot[index-1].offset - val_size;

            old_leaf->leafbody.slot[index].size = val_size;
            valueCopy(value, old_leaf, val_size, old_leaf->leafbody.slot[index].offset);
            old_leaf->freespace -= 16+val_size;
            old_leaf->info.num_keys++;
        }
    }
    
    leaf->info.isLeaf = old_leaf->info.isLeaf;
    leaf->info.num_keys = old_leaf->info.num_keys;
    leaf->parent_num = old_leaf->parent_num;
    leaf->Rsibling = old_leaf->Rsibling;
    for(int i=0; i<3968; i++) leaf->leafbody.value[i] = old_leaf->leafbody.value[i];
    leaf->freespace = old_leaf->freespace;
    
    free(old_leaf);
    return insert_into_parent(table_id, leaf_num, leaf, leaf_idx, new_leaf_num, new_leaf, new_leaf_idx, new_leaf->leafbody.slot[0].key);
}

int start_new_tree(int64_t table_id, int64_t key, char * value, uint16_t val_size) {
    pagenum_t new_root_num;
    int32_t root_idx, header_idx;
    page_t *new_root, *header;

    new_root_num = buffer_alloc_page(table_id);
    new_root = buffer_read_page(table_id, new_root_num, &root_idx, WRITE);
    
    header = buffer_read_page(table_id, 0, &header_idx, WRITE);
    header->root_num = new_root_num;
    buffer_write_page(table_id, 0, header_idx, 1);

    new_root->parent_num = 0;
    new_root->info.isLeaf = 1;
    new_root->info.num_keys = 1;
    new_root->freespace = INITIAL_FREE-(16+val_size);
    new_root->Rsibling = 0;
    new_root->leafbody.slot[0].key = key;
    new_root->leafbody.slot[0].offset = PGSIZE-val_size;
    new_root->leafbody.slot[0].size = val_size;
    new_root->leafbody.slot[0].trx_id = 0;
    valueCopy(value, new_root, val_size, new_root->leafbody.slot[0].offset);

    buffer_write_page(table_id, new_root_num, root_idx, 1);
    return 0;
}

int db_insert(int64_t table_id, int64_t key, char * value, uint16_t val_size) {
    int ret_num = 1;
    pagenum_t root_num, leaf_num;
    int32_t leaf_idx, header_idx;
    page_t *leaf, *header;
    
    if(!isValid(table_id)) return 1;
    
    header = buffer_read_page(table_id, 0, &header_idx, READ);
    root_num = header->root_num;
    if(!root_num) return start_new_tree(table_id, key, value, val_size);

    leaf_num = find_leaf(table_id, root_num, key);
    leaf = buffer_read_page(table_id, leaf_num, &leaf_idx, WRITE);
    
    uint32_t i;
    for(i=0; i<leaf->info.num_keys; i++) {
        if(leaf->leafbody.slot[i].key == key) {
            buffer_write_page(table_id, leaf_num, leaf_idx, 0);
            return 1;
        }
        if(leaf->leafbody.slot[i].key > key) break;
    }
    if(leaf->freespace>=16+val_size) return insert_into_leaf(table_id, i, leaf_num, leaf, leaf_idx, key, value, val_size);
    return insert_into_leaf_after_splitting(table_id, i, leaf_num, leaf, leaf_idx, key, value, val_size);
}


// 삭제 시작
int get_my_index(int64_t table_id, pagenum_t pagenum, page_t* page) {
    page_t* parent;
    int i;
    int32_t parent_idx;

    parent = buffer_read_page(table_id, page->parent_num, &parent_idx, READ);
    if(parent->leftmost==pagenum) {
        return -1;
    }

    for(i=0; i<parent->info.num_keys; i++) {
        if(parent->branch[i].pagenum == pagenum) {
            return i;
        }
    }
    return -2;
}

void compact_value(int64_t table_id, page_t* leaf, int32_t leaf_idx) {
    page_t* tmp = (page_t*)malloc(sizeof(page_t));

    for(int i=0; i<leaf->info.num_keys; i++) {
        tmp->leafbody.slot[i].offset = (i==0) ? PGSIZE : tmp->leafbody.slot[i-1].offset;
        tmp->leafbody.slot[i].offset -= leaf->leafbody.slot[i].size;
        for(int j=0; j<leaf->leafbody.slot[i].size; j++) {
            tmp->leafbody.value[tmp->leafbody.slot[i].offset-128+j]
                = leaf->leafbody.value[leaf->leafbody.slot[i].offset-128+j];
        }
    }
    for(int i=0; i<leaf->info.num_keys; i++) {
        leaf->leafbody.slot[i].offset = tmp->leafbody.slot[i].offset;
    }
    
    uint16_t startpoint = (16*leaf->info.num_keys) + leaf->freespace;
    for(uint16_t i=startpoint; i<3968; i++) leaf->leafbody.value[i] = tmp->leafbody.value[i];

    free(tmp);
}


void delete_leaf(int64_t table_id, uint32_t index, pagenum_t leaf_num, page_t* leaf, int32_t leaf_idx, int64_t key) {
    leaf->freespace += leaf->leafbody.slot[index].size+16;
    for(uint32_t i=index; i<leaf->info.num_keys-1; i++) {
        leaf->leafbody.slot[i].key = leaf->leafbody.slot[i+1].key;
        leaf->leafbody.slot[i].size = leaf->leafbody.slot[i+1].size;
        leaf->leafbody.slot[i].offset = leaf->leafbody.slot[i+1].offset;
        leaf->leafbody.slot[i].trx_id = leaf->leafbody.slot[i+1].trx_id;
    }
    leaf->info.num_keys--;
    compact_value(table_id, leaf, leaf_idx);
}

void delete_internal(int64_t table_id, uint32_t index, pagenum_t page_num, page_t* page, int32_t page_idx, int64_t key) {
    for(uint32_t i=index; i<page->info.num_keys-1; i++) {
        page->branch[i].key = page->branch[i+1].key;
        page->branch[i].pagenum = page->branch[i+1].pagenum;
    }
    page->info.num_keys--;
}

int adjust_root(int64_t table_id, pagenum_t root_num, page_t* root, int32_t root_idx, int64_t key) {
    uint32_t index;

    if(root->info.isLeaf) {
        for(index=0; index<root->info.num_keys; index++) {
            if(root->leafbody.slot[index].key == key) break;
        }
        if(index==root->info.num_keys) {

            return 1;
        }
        delete_leaf(table_id, index, root_num, root, root_idx, key);
    } else {
        for(index=0; index<root->info.num_keys; index++) {
            if(root->branch[index].key == key) break;
        }
        if(index==root->info.num_keys) {
            return 1;
        }
        delete_internal(table_id, index, root_num, root, root_idx, key);
    }

    if(!root->info.num_keys) {
        page_t *header, *new_root;
        int header_idx, new_root_idx;

        header = buffer_read_page(table_id, 0, &header_idx, WRITE);
        
        if(root->info.isLeaf) {
            header->root_num = 0;
        } else {
            header->root_num = root->leftmost;

            new_root = buffer_read_page(table_id, root->leftmost, &new_root_idx, WRITE);
            new_root->parent_num = 0;
            buffer_write_page(table_id, root->leftmost, new_root_idx, 1);
        }
        buffer_write_page(table_id, 0, header_idx, 1);
        buffer_free_page(table_id, root_num, root_idx);
    } else {
        buffer_write_page(table_id, root_num, root_idx, 1);
    }

    return 0;
}

int coalesce_leaf(int64_t table_id, int my_index, pagenum_t parent_num, page_t* parent, int32_t parent_idx, pagenum_t sibling_num, page_t* sibling, int32_t sibling_idx, pagenum_t leaf_num, page_t* leaf, int32_t leaf_idx) {
    int k_prime_index;
    uint16_t siboff, leafoff;
    for(uint32_t i=sibling->info.num_keys, j=0; j<leaf->info.num_keys; i++,j++) {
        sibling->info.num_keys++;
        sibling->freespace-= 16+leaf->leafbody.slot[j].size;

        sibling->leafbody.slot[i].key = leaf->leafbody.slot[j].key;
        sibling->leafbody.slot[i].size = leaf->leafbody.slot[j].size;
        sibling->leafbody.slot[i].trx_id = leaf->leafbody.slot[j].trx_id;
        sibling->leafbody.slot[i].offset = 128+(16*sibling->info.num_keys)+sibling->freespace;
        
        siboff = sibling->leafbody.slot[i].offset-128;
        leafoff = leaf->leafbody.slot[j].offset-128;
        for(uint16_t k=0; k<sibling->leafbody.slot[i].size; k++)
            sibling->leafbody.value[siboff+k] = leaf->leafbody.value[leafoff+k];
    }
    sibling->Rsibling = leaf->Rsibling;
    buffer_free_page(table_id, leaf_num, leaf_idx);
    
    buffer_write_page(table_id, sibling_num, sibling_idx, 1);

    k_prime_index = (my_index==-1) ? 0 : my_index;
    return delete_entry(table_id, parent_num, parent, parent_idx, parent->branch[k_prime_index].key);
}

int redistribute_leaf(int64_t table_id, pagenum_t parent_num, page_t* parent, int32_t parent_idx, pagenum_t sibling_num, page_t* sibling, int32_t sibling_idx, pagenum_t leaf_num, page_t* leaf, int32_t leaf_idx, int my_index) {
    if(my_index==-1) {
        uint32_t num_keys;
        uint16_t leafoff, siboff;
        uint64_t tmp_freespace = leaf->freespace, movenums = 0;

        for(uint32_t i=0; i<sibling->info.num_keys; i++) {
            tmp_freespace -= sibling->leafbody.slot[i].size;
            movenums++;
            if(tmp_freespace<THRESHOLD) break;
        }
        num_keys = leaf->info.num_keys;
        for(uint32_t i=num_keys, j=0; j<movenums; i++, j++) {
            leaf->freespace -= 16+sibling->leafbody.slot[j].size;
            sibling->freespace += 16+sibling->leafbody.slot[j].size;
            leaf->info.num_keys++; sibling->info.num_keys--;

            leaf->leafbody.slot[i].key = sibling->leafbody.slot[j].key;
            leaf->leafbody.slot[i].size = sibling->leafbody.slot[j].size;
            leaf->leafbody.slot[i].trx_id = sibling->leafbody.slot[j].trx_id;
            leaf->leafbody.slot[i].offset = 128 + (16*leaf->info.num_keys) + leaf->freespace;

            siboff = sibling->leafbody.slot[j].offset-128;
            leafoff = leaf->leafbody.slot[i].offset-128;
            for(int k=0; k<sibling->leafbody.slot[j].size; k++)
                leaf->leafbody.value[leafoff+k] = sibling->leafbody.value[siboff+k];
        }
        for(int i=movenums; i<movenums+sibling->info.num_keys; i++) {
            sibling->leafbody.slot[i-movenums].key = sibling->leafbody.slot[i].key;
            sibling->leafbody.slot[i-movenums].size = sibling->leafbody.slot[i].size;
            sibling->leafbody.slot[i-movenums].offset = sibling->leafbody.slot[i].offset;
            sibling->leafbody.slot[i-movenums].trx_id = sibling->leafbody.slot[i].trx_id;
        }
        compact_value(table_id, sibling, sibling_idx);
        parent->branch[0].key = sibling->leafbody.slot[0].key;
    } else {
        uint32_t i = sibling->info.num_keys-1, movenums = 0;
        uint64_t tmp_freespace = leaf->freespace;
        uint16_t leafoff, siboff;

        for(i=sibling->info.num_keys-1; ; i--) {
            tmp_freespace -= sibling->leafbody.slot[i].size;
            movenums++;
            if(tmp_freespace<THRESHOLD) break;

            if(i==0) break;
        }

        for(uint32_t l=leaf->info.num_keys-1; ; l--) {
            leaf->leafbody.slot[l+movenums].key = leaf->leafbody.slot[l].key;
            leaf->leafbody.slot[l+movenums].size = leaf->leafbody.slot[l].size;
            leaf->leafbody.slot[l+movenums].offset = leaf->leafbody.slot[l].offset;
            leaf->leafbody.slot[l+movenums].trx_id = leaf->leafbody.slot[l].trx_id;

            if(l==0) break; 
        }

        for(uint32_t l=0, s=i; l<movenums; l++, s++) {
            leaf->freespace -= 16+sibling->leafbody.slot[s].size;
            sibling->freespace += 16+sibling->leafbody.slot[s].size;
            leaf->info.num_keys++; sibling->info.num_keys--;

            leaf->leafbody.slot[l].key = sibling->leafbody.slot[s].key;
            leaf->leafbody.slot[l].size = sibling->leafbody.slot[s].size;
            leaf->leafbody.slot[l].trx_id = sibling->leafbody.slot[s].trx_id;
            leaf->leafbody.slot[l].offset = 128 + (16*leaf->info.num_keys) + leaf->freespace;

            leafoff = leaf->leafbody.slot[l].offset-128;
            siboff = sibling->leafbody.slot[s].offset-128;
            for(int j=0; j<leaf->leafbody.slot[l].size; j++) {
                leaf->leafbody.value[leafoff+j]
                    = sibling->leafbody.value[siboff+j];
            }
        }
        compact_value(table_id, sibling, sibling_idx);
        parent->branch[my_index].key = leaf->leafbody.slot[0].key;
    }

    buffer_write_page(table_id, parent_num, parent_idx, 1);
    buffer_write_page(table_id, leaf_num, leaf_idx, 1);
    buffer_write_page(table_id, sibling_num, sibling_idx, 1);

    return 0;
}

int coalesce_internal(int64_t table_id, int my_index, pagenum_t parent_num, page_t* parent, int32_t parent_idx, pagenum_t sibling_num, page_t* sibling, int32_t sibling_idx, pagenum_t page_num, page_t* page, int32_t page_idx) {
    int64_t k_prime;
    page_t* child;
    int32_t child_idx;
    
    if(my_index==-1) k_prime = parent->branch[0].key;
    else k_prime = parent->branch[my_index].key;

    sibling->branch[sibling->info.num_keys].key = k_prime;
    sibling->branch[sibling->info.num_keys].pagenum = page->leftmost;
    sibling->info.num_keys++;

    child = buffer_read_page(table_id, page->leftmost, &child_idx, WRITE);
    child->parent_num = sibling_num;
    buffer_write_page(table_id, page->leftmost, child_idx, 1);

    int i=sibling->info.num_keys;
    int num_keys = page->info.num_keys;
    for(int j=0; j<num_keys; j++, i++) {
        sibling->branch[i].key= page->branch[j].key;
        sibling->branch[i].pagenum = page->branch[j].pagenum;
        sibling->info.num_keys++;

        child = buffer_read_page(table_id, sibling->branch[i].pagenum, &child_idx, WRITE);
        child->parent_num = sibling_num;
        buffer_write_page(table_id, sibling->branch[i].pagenum, child_idx, 1);
    }

    buffer_free_page(table_id, page_num, page_idx);
    buffer_write_page(table_id, sibling_num, sibling_idx, 1);

    return delete_entry(table_id, parent_num, parent, parent_idx, k_prime);
}

int redistribute_internal(int64_t table_id, pagenum_t parent_num, page_t* parent, int32_t parent_idx, pagenum_t sibling_num, page_t* sibling, int32_t sibling_idx, pagenum_t page_num, page_t* page, int32_t page_idx, int my_index) {
    page_t *child;
    int32_t child_idx;

    if(my_index==-1) {    
        page->branch[page->info.num_keys].key = parent->branch[0].key;
        page->branch[page->info.num_keys].pagenum = sibling->leftmost;

        child = buffer_read_page(table_id, sibling->leftmost, &child_idx, WRITE);
        child->parent_num = page_num;
        buffer_write_page(table_id, sibling->leftmost, child_idx, 1);

        parent->branch[0].key = sibling->branch[0].key;
        sibling->leftmost = sibling->branch[0].pagenum;
        for(int i=0; i<sibling->info.num_keys-1; i++) {
            sibling->branch[i].key = sibling->branch[i+1].key;
            sibling->branch[i].pagenum = sibling->branch[i+1].pagenum;
        }
        page->info.num_keys++;
        sibling->info.num_keys--;
    } else {
        for(uint32_t i=page->info.num_keys; i>0; i--) {
            page->branch[i].key = page->branch[i-1].key;
            page->branch[i].pagenum = page->branch[i-1].pagenum;

            if(i==1) break;
        }
        page->branch[0].key = parent->branch[my_index].key;
        page->branch[0].pagenum = page->leftmost;
        page->leftmost = sibling->branch[sibling->info.num_keys-1].pagenum;

        child = buffer_read_page(table_id, page->branch[0].pagenum, &child_idx, WRITE);
        child->parent_num = page_num;
        buffer_write_page(table_id, page->branch[0].pagenum, child_idx, 1);
        
        child = buffer_read_page(table_id, page->leftmost, &child_idx, WRITE);
        child->parent_num = page_num;
        buffer_write_page(table_id, page->leftmost, child_idx, 1);

        parent->branch[my_index].key = sibling->branch[sibling->info.num_keys-1].key;
        page->info.num_keys++;
        sibling->info.num_keys--;
    }

    buffer_write_page(table_id, sibling_num, sibling_idx, 1);
    buffer_write_page(table_id, page_num, page_idx, 1);
    buffer_write_page(table_id, parent_num, parent_idx, 1);

    return 0;
}

int delete_entry(int64_t table_id, pagenum_t page_num, page_t* page, int32_t page_idx, int64_t key) {
    page_t *parent, *sibling;
    pagenum_t sibling_num;
    int32_t sibling_idx, parent_idx;;

    if(!page->parent_num) return adjust_root(table_id, page_num, page, page_idx, key);

    if(page->info.isLeaf) {
        uint32_t index=0;
        for(index=0; index<page->info.num_keys; index++) {
            if(page->leafbody.slot[index].key == key) break;
        }
        if(index==page->info.num_keys) {
            buffer_write_page(table_id, page_num, page_idx, 0);
            return 1;
        }
        delete_leaf(table_id, index, page_num, page, page_idx, key);

        if(page->freespace<THRESHOLD) {
            buffer_write_page(table_id, page_num, page_idx, 1);
            return 0;
        }

        int my_index = get_my_index(table_id, page_num, page);
        if(my_index==-2) {
            buffer_write_page(table_id, page_num, page_idx, 0);
            return 1;
        }

        parent = buffer_read_page(table_id, page->parent_num, &parent_idx, WRITE);

        if(my_index==-1) sibling_num = parent->branch[0].pagenum;
        else if(my_index==0) sibling_num = parent->leftmost;
        else sibling_num = parent->branch[my_index-1].pagenum;

        sibling = buffer_read_page(table_id, sibling_num, &sibling_idx, WRITE);
        
        if(sibling->freespace>=INITIAL_FREE-page->freespace) {
            if(my_index==-1) return coalesce_leaf(table_id, my_index, page->parent_num, parent, parent_idx, page_num, page, page_idx, sibling_num, sibling, sibling_idx);
            return coalesce_leaf(table_id, my_index, page->parent_num, parent, parent_idx, sibling_num, sibling, sibling_idx, page_num, page, page_idx);
        }
        return redistribute_leaf(table_id, page->parent_num, parent, parent_idx ,sibling_num, sibling, sibling_idx, page_num, page, page_idx, my_index);
    } else {
        int min_keys = cut(MAX_ORDER) - 1, capacity = MAX_ORDER-1, my_index;
        uint32_t index = 0;
        for(index=0; index<page->info.num_keys; index++) {
            if(page->branch[index].key == key) break;
        }
        if(index==page->info.num_keys) {
            buffer_write_page(table_id, page_num, page_idx, 0);
            return 1;
        }
        delete_internal(table_id, index, page_num, page, page_idx, key);

        if(page->info.num_keys >= min_keys) {
            buffer_write_page(table_id, page_num, page_idx, 1);
            return 0;
        }
        my_index = get_my_index(table_id, page_num, page);
        parent = buffer_read_page(table_id, page->parent_num, &parent_idx, WRITE);

        if(my_index==-1) sibling_num = parent->branch[0].pagenum;
        else if(my_index==0) sibling_num = parent->leftmost;
        else sibling_num = parent->branch[my_index-1].pagenum;
        
        sibling = buffer_read_page(table_id, sibling_num, &sibling_idx, WRITE);

        if(sibling->info.num_keys+page->info.num_keys < capacity) {
            if(my_index==-1) return coalesce_internal(table_id, my_index, page->parent_num, parent, parent_idx, page_num, page, page_idx, sibling_num, sibling, sibling_idx);
            return coalesce_internal(table_id, my_index, page->parent_num, parent, parent_idx, sibling_num, sibling, sibling_idx, page_num, page, page_idx);
        }
        return redistribute_internal(table_id, page->parent_num, parent, parent_idx, sibling_num, sibling, sibling_idx, page_num, page, page_idx, my_index); //redistribution
    }
    return 1;
}

int db_delete(int64_t table_id, int64_t key) {
    page_t *leaf, *header;
    pagenum_t root_num, leaf_num;
    int32_t leaf_idx, header_idx;
    uint32_t i;

    if(!isValid(table_id)) return 1;
    
    header = buffer_read_page(table_id, 0, &header_idx, READ);
    root_num = header->root_num;
    if(!root_num) return 1;

    leaf_num = find_leaf(table_id, root_num, key);
    leaf = buffer_read_page(table_id, leaf_num, &leaf_idx, WRITE);
    
    return delete_entry(table_id, leaf_num, leaf, leaf_idx, key);
}