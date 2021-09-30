#include "file.h"
#include <gtest/gtest.h>
#include <string>

TEST(FileInitTest, HandlesInitialization) {
    int fd;
    off_t filesize;

    fd = file_open_database_file("init_test.db");
    ASSERT_TRUE(fd >= 0);
    filesize = lseek(fd, 0, SEEK_END);
    off_t num_pages = 2560;
    EXPECT_EQ(num_pages, filesize / 4096)
      << "The initial number of pages does not match the requirement: "
      << num_pages;
    
    EXPECT_EQ(file_alloc_page(fd), 1);

    file_close_database_file();
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
    std::string pathname = "Filetest.db";
};

TEST_F(FileTest, HandlesPageAllocation) {
    pagenum_t allocated_page;
    const int freepages = 100;
    pagenum_t freepage_list[freepages];

    ASSERT_TRUE(fd>=0);
    allocated_page = file_alloc_page(fd);
    
    for(int i=0; i<freepages; i++) {
        freepage_list[i] = file_alloc_page(fd);
        ASSERT_TRUE(freepage_list[i]>0);
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
    src_num = file_alloc_page(fd);
    ASSERT_TRUE(src_num > 0);

    src = (page_t*)malloc(sizeof(page_t));
    ASSERT_TRUE(src!=NULL);
    for(int i=0; i<1000; i++) src->Reserved[i] = 'D';
    for(int i=1000; i<2000; i++) src->Reserved[i] = 'B';
    for(int i=2000; i<3000; i++) src->Reserved[i] = 'M';
    for(int i=3000; i<4096; i++) src->Reserved[i] = 'S';
    
    file_write_page(fd, src_num, src);

    dest = (page_t*)malloc(sizeof(page_t));
    ASSERT_TRUE(dest!=NULL);
    file_read_page(fd, src_num, dest);
    for(int i=0; i<4096; i++) EXPECT_EQ(src->Reserved[i], dest->Reserved[i]);

    free(dest);
    dest = NULL;
    free(src);
    src = NULL;
}

TEST_F(FileTest, Sizebecometwice) {
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