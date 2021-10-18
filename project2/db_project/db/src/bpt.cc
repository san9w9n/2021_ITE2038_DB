#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "bpt.h"

#define INITIAL_FREE 3968
#define THRESHOLD 2500
#define MAX_ORDER 249
#define PGSIZE 4096

int64_t open_table(char *pathname) {
    return file_open_table_file(pathname);
}

int cut(int length) {
    if(length%2==0) return length/2;
    return length/2+1;
}

void valueCopy(char* src, page_t* dest, int16_t size, int16_t offset) {
    offset-=128;
    if(offset<0) return;

    for(int i=offset; i<offset+size; i++) {
        dest->leafbody.value[i] = src[i-offset];
    }
}

pagenum_t find_leaf(int64_t table_id, pagenum_t root_num, int64_t key) {
    pagenum_t ret_num = root_num;
    page_t* root = (page_t*)malloc(sizeof(page_t));
    file_read_page(table_id, root_num, root);

    while(!root->info.isLeaf) {
        if(key<root->branch[0].key) ret_num = root->leftmost;
        else {
            uint32_t i=0;
            for(i=0; i<root->info.num_keys-1; i++) {
                if(key<root->branch[i+1].key) break;
            }
            ret_num = root->branch[i].pagenum;
        }
        file_read_page(table_id, ret_num, root);
    }
    free(root);
    return ret_num;
}

int db_find(int64_t table_id, int64_t key, char * ret_val, uint16_t * val_size) {
    page_t *leaf;
    pagenum_t leaf_num, root_num;
    if(!isValid_table_id(table_id)) return 1;

    root_num = get_rootnum(table_id);
    if(root_num==0) return 1;

    leaf_num = find_leaf(table_id, root_num, key);
    leaf = (page_t*)malloc(sizeof(page_t));
    file_read_page(table_id, leaf_num,  leaf);

    uint32_t i=0;
    for(i=0; i<leaf->info.num_keys; i++) {
        if(leaf->leafbody.slot[i].key == key) break;
    }
    if(i==leaf->info.num_keys) return 1;

    *val_size = leaf->leafbody.slot[i].size;
    for(uint16_t c=0; c<leaf->leafbody.slot[i].size; c++) {
        ret_val[c] = leaf->leafbody.value[leaf->leafbody.slot[i].offset-128+c];
    }

    free(leaf);
    return 0;
}

int insert_into_internal(int64_t table_id, uint32_t index, pagenum_t parent_num, page_t *parent, pagenum_t l_num, page_t* l, pagenum_t r_num, page_t* r, int64_t key) {

    for(uint32_t i=parent->info.num_keys-1; i>=index; i--) {
        parent->branch[i+1].key = parent->branch[i].key;
        parent->branch[i+1].pagenum = parent->branch[i].pagenum;

        if(i==0) break;
    }
    parent->branch[index].key = key;
    parent->branch[index].pagenum = r_num;
    parent->info.num_keys++;

    l->info.parent_num = r->info.parent_num = parent_num;
    file_write_page(table_id, parent_num, parent);
    file_write_page(table_id, r_num, r);
    file_write_page(table_id, l_num, l);
    free(parent);
    free(r);
    free(l);

    return 0;
}

int insert_into_internal_after_splitting(int64_t table_id, uint32_t index, pagenum_t parent_num, page_t *parent, pagenum_t r_num, int64_t key) {
    uint32_t num_keys;
    branch_t* tmp;
    pagenum_t new_parent_num;
    int64_t kprime;
    page_t *new_parent;

    tmp=(branch_t*)malloc(sizeof(branch_t) * 252);
    if(!tmp) {
        perror("MALLOC FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    num_keys = parent->info.num_keys + 1;
    
    int split = cut(MAX_ORDER)-1, ret=1;
    for(uint32_t i=0, j=0; i<num_keys; i++, j++) {
        if(j==index) j++;
        tmp[j].key = parent->branch[i].key;
        tmp[j].pagenum = parent->branch[i].pagenum;
    }
    tmp[index].key = key;
    tmp[index].pagenum = r_num;

    new_parent = (page_t*)malloc(sizeof(page_t));
    if(!new_parent) {
        perror("MALLOC FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    new_parent_num = file_alloc_page(table_id);
    new_parent->info.parent_num = parent->info.parent_num;
    new_parent->info.isLeaf = parent->info.isLeaf;
    new_parent->info.num_keys = parent->info.num_keys = 0;

    for(int i=0; i<split; i++) {
        parent->branch[i].key = tmp[i].key;
        parent->branch[i].pagenum = tmp[i].pagenum;
        parent->info.num_keys++;
    }

    page_t* child = (page_t*)malloc(sizeof(page_t));
    new_parent->leftmost = tmp[split].pagenum;
    file_read_page(table_id, new_parent->leftmost, child);
    child->info.parent_num = new_parent_num;
    file_write_page(table_id, new_parent->leftmost, child);

    for(uint32_t i=split+1, j=0; i<num_keys; i++, j++) {
        new_parent->branch[j].key = tmp[i].key;
        new_parent->branch[j].pagenum = tmp[i].pagenum;

        file_read_page(table_id, tmp[i].pagenum, child);
        child->info.parent_num = new_parent_num;
        file_write_page(table_id, tmp[i].pagenum, child);

        new_parent->info.num_keys++;
    }

    kprime = tmp[split].key;

    free(child);
    free(tmp);
    return insert_into_parent(table_id, parent_num, parent, new_parent_num, new_parent, kprime);
}

int insert_into_parent(int64_t table_id, pagenum_t l_num, page_t* l, pagenum_t r_num, page_t* r, int64_t key) {
    
    if(l->info.parent_num==0) {
        page_t *new_root;
        pagenum_t new_root_num;

        new_root = (page_t*)malloc(sizeof(page_t));
        if(!new_root) {
            perror("MALLOC FAILED!!\n");
            exit(EXIT_FAILURE);
        }
        new_root_num = file_alloc_page(table_id);
        l->info.parent_num = r->info.parent_num = new_root_num;
        new_root->info.parent_num = 0;
        new_root->info.isLeaf = 0;
        new_root->info.num_keys = 1;
        new_root->leftmost = l_num;
        new_root->branch[0].key = key;
        new_root->branch[0].pagenum = r_num;
        file_write_page(table_id, new_root_num, new_root);
        free(new_root);

        set_rootnum(table_id, new_root_num);

        l->info.parent_num = r->info.parent_num = new_root_num;
        file_write_page(table_id, r_num, r);
        file_write_page(table_id, l_num, l);
        free(r);
        free(l);
        return 0;
    }

    page_t* parent = (page_t*)malloc(sizeof(page_t));
    file_read_page(table_id, l->info.parent_num, parent);

    uint32_t i;
    for(i=0; i<parent->info.num_keys; i++) {
        if(parent->branch[i].key>=key) break;
    }
    pagenum_t parent_num = l->info.parent_num;

    if(parent->info.num_keys < MAX_ORDER - 1) return insert_into_internal(table_id, i, parent_num, parent, l_num, l, r_num, r, key);
    file_write_page(table_id, r_num, r);
    file_write_page(table_id, l_num, l);
    free(r);
    free(l);

    return insert_into_internal_after_splitting(table_id, i, parent_num, parent, r_num, key);
}

int insert_into_leaf(int64_t table_id, uint32_t index, pagenum_t leaf_num, page_t* leaf, int64_t key, char * value, uint16_t val_size) {

    if(leaf->info.num_keys>0) {
        for(uint32_t i=leaf->info.num_keys-1; i>=index; i--) {
            leaf->leafbody.slot[i+1].key = leaf->leafbody.slot[i].key;
            leaf->leafbody.slot[i+1].offset = leaf->leafbody.slot[i].offset;
            leaf->leafbody.slot[i+1].size = leaf->leafbody.slot[i].size;

            if(i==0) break;
        }
    }
    leaf->info.num_keys++;
    leaf->freespace -= 12+val_size;
    leaf->leafbody.slot[index].key = key;
    leaf->leafbody.slot[index].offset = 128+(12*leaf->info.num_keys)+leaf->freespace;
    leaf->leafbody.slot[index].size = val_size;
    
    valueCopy(value, leaf, val_size, leaf->leafbody.slot[index].offset);
    file_write_page(table_id, leaf_num, leaf);

    free(leaf);
    return 0;
}

int insert_into_leaf_after_splitting(int64_t table_id, uint32_t index, pagenum_t leaf_num, page_t* leaf, int64_t key, char* value, uint16_t val_size) {
    uint32_t totalspace = 0, offset = PGSIZE, split=0, num_keys = leaf->info.num_keys;
    page_t *new_leaf, *old_leaf;
    int ret = 1, flag = 0;
    pagenum_t new_leaf_num;

    for(split=0; split<leaf->info.num_keys; split++) {
        if(split==index) {
            totalspace+=12+val_size;
            flag = 1;
            if(totalspace>=INITIAL_FREE/2) {
                flag = 0;
                break;
            }
        } 
        totalspace+=12+leaf->leafbody.slot[split].size;
        if(totalspace>=INITIAL_FREE/2) {
            break;
        }
    }

    new_leaf = (page_t*)malloc(sizeof(page_t));
    if(!new_leaf) {
        perror("MALLOC FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    new_leaf_num = file_alloc_page(table_id);
    new_leaf->info.parent_num = leaf->info.parent_num;
    new_leaf->info.isLeaf = leaf->info.isLeaf;
    new_leaf->info.num_keys = 0;
    new_leaf->freespace = INITIAL_FREE;
    new_leaf->Rsibling = leaf->Rsibling;

    old_leaf = (page_t*)malloc(sizeof(page_t));
    if(!old_leaf) {
        perror("MALLOC FAILED!!\n");
        exit(EXIT_FAILURE);
    }
    old_leaf->info.parent_num = leaf->info.parent_num;
    old_leaf->info.isLeaf = leaf->info.isLeaf;
    old_leaf->info.num_keys = 0;
    old_leaf->freespace = INITIAL_FREE;
    old_leaf->Rsibling = new_leaf_num;

    if(!flag) {
        uint32_t i=split, j=0;
        for(i=split, j=0; i<num_keys; i++, j++) {
            if(i==index) {
                new_leaf->leafbody.slot[j].key = key;

                if(j==0) new_leaf->leafbody.slot[j].offset = PGSIZE-val_size;
                else new_leaf->leafbody.slot[j].offset = new_leaf->leafbody.slot[j-1].offset-val_size;
    
                new_leaf->leafbody.slot[j].size = val_size;
                valueCopy(value, new_leaf, val_size, new_leaf->leafbody.slot[j].offset);
                new_leaf->freespace -= 12+val_size;
                new_leaf->info.num_keys++;
                j++;
            }
            new_leaf->leafbody.slot[j].key = leaf->leafbody.slot[i].key;

            if(j==0) new_leaf->leafbody.slot[j].offset = PGSIZE - leaf->leafbody.slot[i].size;
            else new_leaf->leafbody.slot[j].offset = new_leaf->leafbody.slot[j-1].offset - leaf->leafbody.slot[i].size;

            new_leaf->leafbody.slot[j].size = leaf->leafbody.slot[i].size;
            for(int k=0; k<leaf->leafbody.slot[i].size; k++) {
                new_leaf->leafbody.value[new_leaf->leafbody.slot[j].offset-128+k]
                    = leaf->leafbody.value[leaf->leafbody.slot[i].offset-128+k];
            }
            new_leaf->freespace -= 12+leaf->leafbody.slot[i].size;
            new_leaf->info.num_keys++;
        }
        if(index==num_keys) {
            new_leaf->leafbody.slot[j].key = key;

            if(j==0) new_leaf->leafbody.slot[j].offset = PGSIZE-val_size;
            else new_leaf->leafbody.slot[j].offset = new_leaf->leafbody.slot[j-1].offset-val_size;

            new_leaf->leafbody.slot[j].size = val_size;
            valueCopy(value, new_leaf, val_size, new_leaf->leafbody.slot[j].offset);
            new_leaf->freespace -= 12+val_size;
            new_leaf->info.num_keys++;
        }
        for(i=0; i<split; i++) {
            old_leaf->leafbody.slot[i].key = leaf->leafbody.slot[i].key;
            old_leaf->leafbody.slot[i].size = leaf->leafbody.slot[i].size;

            if(i==0) old_leaf->leafbody.slot[i].offset = PGSIZE - old_leaf->leafbody.slot[i].size;
            else old_leaf->leafbody.slot[i].offset = old_leaf->leafbody.slot[i-1].offset - old_leaf->leafbody.slot[i].size;

            for(int j=0; j<leaf->leafbody.slot[i].size; j++) {
                old_leaf->leafbody.value[old_leaf->leafbody.slot[i].offset-128+j]
                    = leaf->leafbody.value[leaf->leafbody.slot[i].offset-128+j];
            }
            old_leaf->freespace -= old_leaf->leafbody.slot[i].size+12;
            old_leaf->info.num_keys++;
        }
    } else {
        for(uint32_t i=split, j=0; i<num_keys; i++, j++) {
            new_leaf->leafbody.slot[j].key = leaf->leafbody.slot[i].key;

            if(j==0) new_leaf->leafbody.slot[j].offset = PGSIZE - leaf->leafbody.slot[i].size;
            else new_leaf->leafbody.slot[j].offset = new_leaf->leafbody.slot[j-1].offset - leaf->leafbody.slot[i].size;

            new_leaf->leafbody.slot[j].size = leaf->leafbody.slot[i].size;
            for(int k=0; k<leaf->leafbody.slot[i].size; k++) {
                new_leaf->leafbody.value[new_leaf->leafbody.slot[j].offset-128+k]
                    = leaf->leafbody.value[leaf->leafbody.slot[i].offset-128+k];
            }
            new_leaf->freespace -= 12+leaf->leafbody.slot[i].size;
            new_leaf->info.num_keys++;
        }
        for(uint32_t i=0, j=0; i<split; i++, j++) {
            if(i==index) {
                old_leaf->leafbody.slot[j].key = key;
                old_leaf->leafbody.slot[j].size = val_size;

                if(j==0) old_leaf->leafbody.slot[j].offset = PGSIZE - val_size;
                else old_leaf->leafbody.slot[j].offset = old_leaf->leafbody.slot[j-1].offset - val_size;

                valueCopy(value, old_leaf, val_size, old_leaf->leafbody.slot[j].offset);
                old_leaf->freespace -= val_size+12;
                old_leaf->info.num_keys++;
                j++;
            }
            old_leaf->leafbody.slot[j].key = leaf->leafbody.slot[i].key;
            old_leaf->leafbody.slot[j].size = leaf->leafbody.slot[i].size;

            if(j==0) old_leaf->leafbody.slot[j].offset = PGSIZE - old_leaf->leafbody.slot[j].size;
            else old_leaf->leafbody.slot[j].offset = old_leaf->leafbody.slot[j-1].offset - old_leaf->leafbody.slot[j].size;

            for(int k=0; k<old_leaf->leafbody.slot[j].size; k++) {
                old_leaf->leafbody.value[old_leaf->leafbody.slot[j].offset-128+k]
                    = leaf->leafbody.value[leaf->leafbody.slot[i].offset-128+k];
            }
            old_leaf->freespace -= old_leaf->leafbody.slot[j].size+12;
            old_leaf->info.num_keys++;
        }
        if(index==split) {
            old_leaf->leafbody.slot[index].key = key;

            if(index==0) old_leaf->leafbody.slot[index].offset = PGSIZE - val_size;
            else old_leaf->leafbody.slot[index].offset = old_leaf->leafbody.slot[index-1].offset - val_size;

            old_leaf->leafbody.slot[index].size = val_size;
            valueCopy(value, old_leaf, val_size, old_leaf->leafbody.slot[index].offset);
            old_leaf->freespace -= 12+val_size;
            old_leaf->info.num_keys++;
        }
    }
    free(leaf);
    return insert_into_parent(table_id, leaf_num, old_leaf, new_leaf_num, new_leaf, new_leaf->leafbody.slot[0].key);
}

int start_new_tree(int64_t table_id, int64_t key, char * value, uint16_t val_size) {
    pagenum_t new_root_num;
    page_t *new_root;
    
    new_root = (page_t*)malloc(sizeof(page_t));
    if(!new_root) {
        perror("MALLOC FAILED!\n");
        exit(EXIT_FAILURE);
    }
    new_root->info.parent_num = 0;
    new_root->info.isLeaf = 1;
    new_root->info.num_keys = 1;
    new_root->freespace = INITIAL_FREE-(12+val_size);
    new_root->Rsibling = 0;
    new_root->leafbody.slot[0].key = key;
    new_root->leafbody.slot[0].offset = PGSIZE-val_size;
    new_root->leafbody.slot[0].size = val_size;
    valueCopy(value, new_root, val_size, new_root->leafbody.slot[0].offset);

    new_root_num = file_alloc_page(table_id);
    file_write_page(table_id, new_root_num, new_root);
    free(new_root);

    set_rootnum(table_id, new_root_num);
    return 0;
}

int db_insert(int64_t table_id, int64_t key, char * value, uint16_t val_size) {
    pagenum_t root_num, leaf_num;
    page_t *leaf;
    
    if(!isValid_table_id(table_id)) return 1;

    root_num = get_rootnum(table_id);

    if(!root_num) return start_new_tree(table_id, key, value, val_size);

    leaf_num = find_leaf(table_id, root_num, key);
    leaf = (page_t*)malloc(sizeof(page_t));
    file_read_page(table_id, leaf_num, leaf);

    uint32_t i;
    for(i=0; i<leaf->info.num_keys; i++) {
        if(leaf->leafbody.slot[i].key == key) {
            free(leaf);
            return 1;
        }
        if(leaf->leafbody.slot[i].key > key) break;
    }

    if(leaf->freespace>=12+val_size) return insert_into_leaf(table_id, i, leaf_num, leaf, key, value, val_size);
    return insert_into_leaf_after_splitting(table_id, i, leaf_num, leaf, key, value, val_size);
}


// 삭제 시작
int get_my_index(int64_t table_id, pagenum_t pagenum, page_t* page) {
    page_t* parent;
    int i;
    pagenum_t sibling = 0;
    parent = (page_t*)malloc(sizeof(page_t));
    file_read_page(table_id, page->info.parent_num, parent);
    
    if(parent->leftmost==pagenum) {
        free(parent);
        return -1;
    }
    for(i=0; i<parent->info.num_keys; i++) {
        if(parent->branch[i].pagenum == pagenum) {
            free(parent);
            return i;
        }
    }
    free(parent);
    return -2;
}

void compact_value(int64_t table_id, page_t* leaf) {
    page_t* tmp = (page_t*)malloc(sizeof(page_t));
    if(!tmp) {
        perror("MALLOC FAILED!!\n");
        exit(EXIT_FAILURE);
    }

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
    
    uint16_t startpoint = (12*leaf->info.num_keys) + leaf->freespace;
    for(uint16_t i=startpoint; i<3968; i++) leaf->leafbody.value[i] = tmp->leafbody.value[i];

    free(tmp);
}


void delete_leaf(int64_t table_id, uint32_t index, pagenum_t leaf_num, page_t* leaf, int64_t key) {
    leaf->freespace += leaf->leafbody.slot[index].size+12;
    for(uint32_t i=index; i<leaf->info.num_keys-1; i++) {
        leaf->leafbody.slot[i].key = leaf->leafbody.slot[i+1].key;
        leaf->leafbody.slot[i].size = leaf->leafbody.slot[i+1].size;
        leaf->leafbody.slot[i].offset = leaf->leafbody.slot[i+1].offset;
    }
    leaf->info.num_keys--;
    compact_value(table_id, leaf);
}

void delete_internal(int64_t table_id, uint32_t index, pagenum_t page_num, page_t* page, int64_t key) {
    for(uint32_t i=index; i<page->info.num_keys-1; i++) {
        page->branch[i].key = page->branch[i+1].key;
        page->branch[i].pagenum = page->branch[i+1].pagenum;
    }
    page->info.num_keys--;
}

int adjust_root(int64_t table_id, pagenum_t root_num, page_t* root, int64_t key) {
    uint32_t index;

    if(root->info.isLeaf) {
        for(index=0; index<root->info.num_keys; index++) {
            if(root->leafbody.slot[index].key == key) break;
        }
        if(index==root->info.num_keys) {
            free(root);
            return 1;
        }
        delete_leaf(table_id, index, root_num, root, key);
    } else {
        for(index=0; index<root->info.num_keys; index++) {
            if(root->branch[index].key == key) break;
        }
        if(index==root->info.num_keys) {
            free(root);
            return 1;
        }
        delete_internal(table_id, index, root_num, root, key);
    }

    if(!root->info.num_keys) {
        if(root->info.isLeaf) {
            set_rootnum(table_id, 0);
        } else {
            set_rootnum(table_id, root->leftmost);

            page_t* new_root = (page_t*)malloc(sizeof(page_t));
            file_read_page(table_id, root->leftmost, new_root);
            new_root->info.parent_num = 0;
            file_write_page(table_id, root->leftmost, new_root);
            free(new_root);
        }
        file_free_page(table_id, root_num);
    } else file_write_page(table_id, root_num, root);

    free(root);
    return 0;
}

int coalesce_leaf(int64_t table_id, int my_index, pagenum_t parent_num, page_t* parent, pagenum_t sibling_num, page_t* sibling, pagenum_t leaf_num, page_t* leaf) {
    int k_prime_index;
    for(uint32_t i=sibling->info.num_keys, j=0; j<leaf->info.num_keys; i++,j++) {
        sibling->info.num_keys++;
        sibling->freespace-= 12+leaf->leafbody.slot[j].size;

        sibling->leafbody.slot[i].key = leaf->leafbody.slot[j].key;
        sibling->leafbody.slot[i].size = leaf->leafbody.slot[j].size;
        sibling->leafbody.slot[i].offset = 128+(12*sibling->info.num_keys)+sibling->freespace;
        
        for(uint16_t k=0; k<sibling->leafbody.slot[i].size; k++) {
            sibling->leafbody.value[sibling->leafbody.slot[i].offset-128+k]
                = leaf->leafbody.value[leaf->leafbody.slot[j].offset-128+k];
        }
    }
    sibling->Rsibling = leaf->Rsibling;
    file_free_page(table_id, leaf_num);
    file_write_page(table_id, sibling_num, sibling);

    free(sibling);
    free(leaf);

    k_prime_index = (my_index==-1) ? 0 : my_index;
    return delete_entry(table_id, parent_num, parent, parent->branch[k_prime_index].key);
}

int redistribute_leaf(int64_t table_id, pagenum_t parent_num, page_t* parent, pagenum_t sibling_num, page_t* sibling, pagenum_t leaf_num, page_t* leaf, int my_index) {

    if(my_index==-1) {
        uint32_t num_keys;
        uint64_t tmp_freespace = leaf->freespace, movenums = 0;
        for(uint32_t i=0; i<sibling->info.num_keys; i++) {
            tmp_freespace -= sibling->leafbody.slot[i].size;
            movenums++;
            if(tmp_freespace<THRESHOLD) break;
        }
        num_keys = leaf->info.num_keys;
        for(uint32_t i=num_keys, j=0; j<movenums; i++, j++) {
            leaf->freespace -= 12+sibling->leafbody.slot[j].size;
            sibling->freespace += 12+sibling->leafbody.slot[j].size;
            leaf->info.num_keys++; sibling->info.num_keys--;

            leaf->leafbody.slot[i].key = sibling->leafbody.slot[j].key;
            leaf->leafbody.slot[i].size = sibling->leafbody.slot[j].size;
            leaf->leafbody.slot[i].offset = 128 + (12*leaf->info.num_keys) + leaf->freespace;

            for(int k=0; k<sibling->leafbody.slot[j].size; k++) {
                leaf->leafbody.value[leaf->leafbody.slot[i].offset-128+k]
                    = sibling->leafbody.value[sibling->leafbody.slot[j].offset-128+k];
            }
        }
        for(int i=movenums; i<movenums+sibling->info.num_keys; i++) {
            sibling->leafbody.slot[i-movenums].key = sibling->leafbody.slot[i].key;
            sibling->leafbody.slot[i-movenums].size = sibling->leafbody.slot[i].size;
            sibling->leafbody.slot[i-movenums].offset = sibling->leafbody.slot[i].offset;
        }
        compact_value(table_id, sibling);
        parent->branch[0].key = sibling->leafbody.slot[0].key;
    } else {
        uint32_t i = sibling->info.num_keys-1, movenums = 0;
        uint64_t tmp_freespace = leaf->freespace;
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

            if(l==0) break; 
        }

        for(uint32_t l=0, s=i; l<movenums; l++, s++) {
            leaf->freespace -= 12+sibling->leafbody.slot[s].size;
            sibling->freespace += 12+sibling->leafbody.slot[s].size;
            leaf->info.num_keys++; sibling->info.num_keys--;

            leaf->leafbody.slot[l].key = sibling->leafbody.slot[s].key;
            leaf->leafbody.slot[l].size = sibling->leafbody.slot[s].size;
            leaf->leafbody.slot[l].offset = 128 + (12*leaf->info.num_keys) + leaf->freespace;

            for(int j=0; j<leaf->leafbody.slot[l].size; j++) {
                leaf->leafbody.value[leaf->leafbody.slot[l].offset-128+j]
                    = sibling->leafbody.value[sibling->leafbody.slot[s].offset-128+j];
            }
        }
        compact_value(table_id, sibling);
        parent->branch[my_index].key = leaf->leafbody.slot[0].key;
    }
    file_write_page(table_id, parent_num, parent);
    file_write_page(table_id, sibling_num, sibling);
    file_write_page(table_id, leaf_num, leaf);

    free(parent);
    free(sibling);
    free(leaf);
    return 0;
}

int coalesce_internal(int64_t table_id, int my_index, pagenum_t parent_num, page_t* parent, pagenum_t sibling_num, page_t* sibling, pagenum_t page_num, page_t* page) {
    int64_t k_prime;
    
    if(my_index==-1) k_prime = parent->branch[0].key;
    else k_prime = parent->branch[my_index].key;

    sibling->branch[sibling->info.num_keys].key = k_prime;
    sibling->branch[sibling->info.num_keys].pagenum = page->leftmost;
    sibling->info.num_keys++;

    page_t* child = (page_t*)malloc(sizeof(page_t));
    file_read_page(table_id, page->leftmost, child);
    child->info.parent_num = sibling_num;
    file_write_page(table_id, page->leftmost, child);
    
    int i=sibling->info.num_keys;
    int num_keys = page->info.num_keys;
    for(int j=0; j<num_keys; j++, i++) {
        sibling->branch[i].key= page->branch[j].key;
        sibling->branch[i].pagenum = page->branch[j].pagenum;
        sibling->info.num_keys++;
        file_read_page(table_id, sibling->branch[i].pagenum, child);
        child->info.parent_num = sibling_num;
        file_write_page(table_id, sibling->branch[i].pagenum, child);
    }

    file_free_page(table_id, page_num);
    file_write_page(table_id, sibling_num, sibling);

    free(child);
    free(page);
    free(sibling);

    return delete_entry(table_id, parent_num, parent, k_prime);
}

int redistribute_internal(int64_t table_id, pagenum_t parent_num, page_t* parent, pagenum_t sibling_num, page_t* sibling, pagenum_t page_num, page_t* page, int my_index) {
    
    if(my_index==-1) {
        page->branch[page->info.num_keys].key = parent->branch[0].key;
        page->branch[page->info.num_keys].pagenum = sibling->leftmost;

        page_t* child = (page_t*)malloc(sizeof(page_t));
        file_read_page(table_id, sibling->leftmost, child);
        child->info.parent_num = page_num;
        file_write_page(table_id, sibling->leftmost, child);

        parent->branch[0].key = sibling->branch[0].key;
        sibling->leftmost = sibling->branch[0].pagenum;
        for(int i=0; i<sibling->info.num_keys-1; i++) {
            sibling->branch[i].key = sibling->branch[i+1].key;
            sibling->branch[i].pagenum = sibling->branch[i+1].pagenum;
        }
        page->info.num_keys++;
        sibling->info.num_keys--;
        free(child);
    } else {
        for(uint32_t i=page->info.num_keys; i>0; i--) {
            page->branch[i].key = page->branch[i-1].key;
            page->branch[i].pagenum = page->branch[i-1].pagenum;
            if(i==1) break;
        }
        page->branch[0].key = parent->branch[my_index].key;
        page->branch[0].pagenum = page->leftmost;
        page->leftmost = sibling->branch[sibling->info.num_keys-1].pagenum;

        page_t* child = (page_t*)malloc(sizeof(page_t));
        file_read_page(table_id, page->branch[0].pagenum, child);
        child->info.parent_num = page_num;
        file_write_page(table_id, page->branch[0].pagenum, child);
        
        file_read_page(table_id, page->leftmost, child);
        child->info.parent_num = page_num;
        file_write_page(table_id, page->leftmost, child);

        parent->branch[my_index].key = sibling->branch[sibling->info.num_keys-1].key;
        page->info.num_keys++;
        sibling->info.num_keys--;
        free(child);
    }

    file_write_page(table_id, parent_num, parent);
    file_write_page(table_id, sibling_num, sibling);
    file_write_page(table_id, page_num, page);

    free(parent);
    free(sibling);
    free(page);
    
    return 0;
}

int delete_entry(int64_t table_id, pagenum_t page_num, page_t* page, int64_t key) {
    pagenum_t sibling_num;

    if(!page->info.parent_num) return adjust_root(table_id, page_num, page, key);

    if(page->info.isLeaf) {
        uint32_t index=0;
        for(index=0; index<page->info.num_keys; index++) {
            if(page->leafbody.slot[index].key == key) break;
        }
        if(index==page->info.num_keys) {
            free(page);
            return 1;
        }
        delete_leaf(table_id, index, page_num, page, key);

        if(page->freespace<THRESHOLD) {
            file_write_page(table_id, page_num, page);
            free(page);
            return 0;
        }

        int my_index = get_my_index(table_id, page_num, page);
        if(my_index==-2) {
            file_write_page(table_id, page_num, page);
            free(page);
            return 1;
        }

        page_t* parent = (page_t*)malloc(sizeof(page_t));
        file_read_page(table_id, page->info.parent_num, parent);

        page_t* sibling = (page_t*)malloc(sizeof(page_t));
        if(my_index==-1) sibling_num = parent->branch[0].pagenum;
        else if(my_index==0) sibling_num = parent->leftmost;
        else sibling_num = parent->branch[my_index-1].pagenum;
        file_read_page(table_id, sibling_num, sibling);
        
        if(sibling->freespace>=INITIAL_FREE-page->freespace) {
            if(my_index==-1) return coalesce_leaf(table_id, my_index, page->info.parent_num, parent, page_num, page, sibling_num, sibling);
            return coalesce_leaf(table_id, my_index, page->info.parent_num, parent, sibling_num, sibling, page_num, page);
        }
        
        return redistribute_leaf(table_id, page->info.parent_num, parent, sibling_num, sibling, page_num, page, my_index);
    } else {
        int min_keys = cut(MAX_ORDER) - 1, capacity = MAX_ORDER-1, my_index;
        uint32_t index = 0;
        for(index=0; index<page->info.num_keys; index++) {
            if(page->branch[index].key == key) break;
        }
        if(index==page->info.num_keys) {
            free(page);
            return 1;
        }
        delete_internal(table_id, index, page_num, page, key);

        if(page->info.num_keys >= min_keys) {
            file_write_page(table_id, page_num, page);
            free(page);
            return 0;
        }
        my_index = get_my_index(table_id, page_num, page);
        if(my_index==-2) {
            file_write_page(table_id, page_num, page);
            free(page);
            return 0;
        }
        
        page_t* parent = (page_t*)malloc(sizeof(page_t));
        file_read_page(table_id, page->info.parent_num, parent);

        page_t* sibling = (page_t*)malloc(sizeof(page_t));
        if(my_index==-1) sibling_num = parent->branch[0].pagenum;
        else if(my_index==0) sibling_num = parent->leftmost;
        else sibling_num = parent->branch[my_index-1].pagenum;
        file_read_page(table_id, sibling_num, sibling);

        if(sibling->info.num_keys+page->info.num_keys < capacity) {
            if(my_index==-1) return coalesce_internal(table_id, my_index, page->info.parent_num, parent, page_num, page, sibling_num, sibling);
            return coalesce_internal(table_id, my_index, page->info.parent_num, parent, sibling_num, sibling, page_num, page);
        }
        return redistribute_internal(table_id, page->info.parent_num, parent, sibling_num, sibling, page_num, page, my_index); //redistribution
    }
    return 1;
}

int db_delete(int64_t table_id, int64_t key) {
    page_t *leaf;
    pagenum_t root_num, leaf_num;
    uint32_t i;
    if(!isValid_table_id(table_id)) return 1;
    root_num = get_rootnum(table_id);
    if(!root_num) return 1;

    leaf_num = find_leaf(table_id, root_num, key);
    leaf = (page_t*)malloc(sizeof(page_t));
    file_read_page(table_id, leaf_num, leaf);
    
    return delete_entry(table_id, leaf_num, leaf, key);
}

int init_db() {
    return 0;
}

int shutdown_db() {
    file_close_table_files();
    return 0;
}