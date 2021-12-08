#include "file.h"
#include <gtest/gtest.h>
#include <stdio.h>

TEST(FileInitTest, HandlesInitialization) {
    int64_t table_id;
    off_t fsize;
    FILE *f;

    table_id = file_open_table_file("init_test.db");
    ASSERT_TRUE(table_id >= 0);
    
    f = fopen("init_test.db", "r");
    fseek(f, 0, SEEK_END);
    fsize = ftell(f);
    fclose(f);
    off_t num_pages = 2560;
    EXPECT_EQ(num_pages, fsize / 4096);
    file_close_table_files();
    int is_removed = remove("init_test.db");
    ASSERT_EQ(is_removed, 0);
}

class FileTest : public ::testing::Test {
    protected:
        FileTest() { table_id = file_open_table_file(pathname.c_str()); }

        ~FileTest() {
            if(table_id >= 0) {
                file_close_table_files();
                remove(pathname.c_str());
            }
        }
    int64_t table_id;
    std::string pathname = "test1.db";
};

TEST_F(FileTest, HandlesPageAllocation) {
    pagenum_t allocated_page;
    int freepages = 100;
    pagenum_t freepage_list[100];

    ASSERT_TRUE(table_id>=0);
    allocated_page = file_alloc_page(table_id);
    ASSERT_TRUE(allocated_page > 0);
    
    for(int i=0; i<freepages; i++) {
        freepage_list[i] = file_alloc_page(table_id);
        ASSERT_TRUE(freepage_list[i] > 0);
    }
    for(int i=0; i<freepages; i++) {
        file_free_page(table_id, freepage_list[i]);
    }
    for(int i=freepages-1; i>=0; i--) {
        EXPECT_EQ(file_alloc_page(table_id), freepage_list[i]);
    }
}

TEST_F(FileTest, CheckReadWriteOperation) {
    page_t *dest, *src;
    pagenum_t src_num;
    ASSERT_TRUE(table_id >= 0);
    src_num = file_alloc_page(table_id);
    ASSERT_TRUE(src_num>0);
    src = (page_t*)malloc(sizeof(page_t));
    ASSERT_TRUE(src!=NULL);

    src->info.isLeaf = 1;
    src->parent_num = 10;
    src->info.num_keys = 1000;
    src->freespace = 1000;
    src->Rsibling = 4;
    for(int i=0; i<3968; i++) {
        src->leafbody.value[i] = 'B';
    }

    file_write_page(table_id, src_num, src);

    dest = (page_t*)malloc(sizeof(page_t));
    ASSERT_TRUE(dest!=NULL);
    
    file_read_page(table_id, src_num, dest);

    EXPECT_EQ(src->info.isLeaf, dest->info.isLeaf);
    EXPECT_EQ(src->parent_num, dest->parent_num);
    EXPECT_EQ(src->info.num_keys, dest->info.num_keys);
    EXPECT_EQ(src->freespace, dest->freespace);
    EXPECT_EQ(src->Rsibling, dest->Rsibling);

    for(int i=0; i<3968; i++) {
        EXPECT_EQ(src->leafbody.value[i], dest->leafbody.value[i]);
    }

    free(dest); dest = NULL;
    free(src); src = NULL;
}

TEST_F(FileTest, SizeBecomeTwice) {
    FILE *f;
    off_t firstsize, twicesize;
    long loop;

    ASSERT_TRUE(table_id >= 0);
    
    f = fopen("test1.db", "r");
    fseek(f, 0, SEEK_END);
    firstsize = ftell(f)/4096;
    fclose(f);

    ASSERT_TRUE(firstsize>=2560);

    loop = firstsize;
    while(loop--) {
        ASSERT_NE(file_alloc_page(table_id), 0);
    }
    f = fopen("test1.db", "r");
    fseek(f, 0, SEEK_END);
    twicesize = ftell(f)/4096;
    fclose(f);
    ASSERT_EQ(twicesize, firstsize*2);
}