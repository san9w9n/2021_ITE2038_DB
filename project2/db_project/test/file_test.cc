#include "file.h"
#include <gtest/gtest.h>
#include <string>

TEST(FileInitTest, HandlesInitialization) {
    int fd;
    off_t fsize;

    fd = file_open_database_file("init_test.db");
    ASSERT_TRUE(fd >= 0);
    fsize = lseek(fd, 0, SEEK_END);
    off_t num_pages = 2560;
    EXPECT_EQ(num_pages, fsize / 4096)
      << "The initial number of pages does not match the requirement: "
      << num_pages;
    file_close_database_file();
    EXPECT_EQ(close(fd), -1);
    int is_removed = remove("init_test.db");
    ASSERT_EQ(is_removed, 0);
}

TEST(FileCloseTest, HandlesCloseFiles) {
    int fd[5];
    std::string path[5] = {"close_test1.db", "close_test2.db", "close_test3.db",
                    "close_test4.db", "close_test5.db"}; 
    for(int i=0; i<5; i++) {
        fd[i] = file_open_database_file(path[i].c_str());
        EXPECT_TRUE(fd[i]>=0);
    }

    file_close_database_file();
    for(int i=0; i<5; i++) {
        EXPECT_TRUE(close(fd[i])<0);
        EXPECT_EQ(remove(path[i].c_str()), 0);
    }
}

class FileTest : public ::testing::Test {
    protected:
        FileTest() { fd = file_open_database_file(pathname.c_str()); }

        ~FileTest() {
            if(fd >= 0) {
                file_close_database_file();
                remove(pathname.c_str());
            }
        }
    int fd;
    std::string pathname = "test1.db";
};

TEST_F(FileTest, HandlesPageAllocation) {
    pagenum_t allocated_page;
    int freepages = 100;
    pagenum_t freepage_list[100];

    ASSERT_TRUE(fd>=0);
    allocated_page = file_alloc_page(fd);
    ASSERT_NE(allocated_page, 0);
    
    for(int i=0; i<freepages; i++) {
        freepage_list[i] = file_alloc_page(fd);
        ASSERT_NE(freepage_list[i], 0);
    }
    for(int i=0; i<freepages; i++) {
        file_free_page(fd, freepage_list[i]);
    }
    for(int i=freepages-1; i>=0; i--) {
        EXPECT_EQ(file_alloc_page(fd), freepage_list[i]);
    }
}

TEST_F(FileTest, CheckReadWriteOperation) {
    page_t *dest, *src;
    pagenum_t src_num;
    ASSERT_TRUE(fd >= 0);
    src = (page_t*)malloc(sizeof(page_t));
    ASSERT_TRUE(src!=NULL);
    for(int i=0; i<1000; i++) src->Reserved[i] = 'D';
    for(int i=1000; i<2000; i++) src->Reserved[i] = 'B';
    for(int i=2000; i<3000; i++) src->Reserved[i] = 'M';
    for(int i=3000; i<4096; i++) src->Reserved[i] = 'S';
    src_num = file_alloc_page(fd);
    ASSERT_TRUE(src_num > 0);
    file_write_page(fd, src_num, src);
    free(src);
    src = NULL;

    dest = (page_t*)malloc(sizeof(page_t));
    ASSERT_TRUE(dest!=NULL);
    file_read_page(fd, src_num, dest);
    for(int i=0; i<1000; i++) ASSERT_EQ(dest->Reserved[i],'D');
    for(int i=1000; i<2000; i++) ASSERT_EQ(dest->Reserved[i],'B');
    for(int i=2000; i<3000; i++) ASSERT_EQ(dest->Reserved[i],'M');
    for(int i=3000; i<4096; i++) ASSERT_EQ(dest->Reserved[i],'S');
    free(dest);
    dest = NULL;
}

TEST_F(FileTest, SizeBecomeTwice) {
    off_t firstsize, twicesize;
    ASSERT_TRUE(fd >= 0);
    firstsize = lseek(fd, 0, SEEK_END)/4096;
    ASSERT_TRUE(firstsize>=2560);

    long loop = firstsize;
    while(loop--) {
        ASSERT_NE(file_alloc_page(fd), 0);
    }
    twicesize = lseek(fd, 0, SEEK_END)/4096;
    ASSERT_EQ(twicesize, firstsize*2);

    firstsize = twicesize;
    loop = firstsize;
    while(loop--) {
        ASSERT_NE(file_alloc_page(fd), 0);
    }
    twicesize = lseek(fd, 0, SEEK_END)/4096;
    ASSERT_EQ(twicesize, firstsize*2);
}