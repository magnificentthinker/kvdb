#include "util/LRUCache.h"

#include <gtest/gtest.h>

#include <thread>
#include <iostream>
#include <cassert>
using namespace kvdb::cache;

TEST(LRUTest, EmptyList)
{
    LRU_cache<int> list(10);

    EXPECT_FALSE(list.Contains(10));
}

TEST(LRUTest, InsertList)
{
    LRU_cache<int> list(10);
    list.Updata(10);

    EXPECT_TRUE(list.Contains(10));
}

TEST(LRUTest, OutSizeList)
{
    LRU_cache<int> list(2);
    list.Updata(1);
    list.Updata(2);
    list.Updata(3);

    EXPECT_FALSE(list.Contains(1));
    EXPECT_TRUE(list.Contains(2));
    EXPECT_TRUE(list.Contains(3));
}

TEST(LRUTest, UpdataList)
{
    LRU_cache<int> list(2);
    list.Updata(1);
    list.Updata(2);
    list.Updata(1);
    list.Updata(3);

    EXPECT_FALSE(list.Contains(2));
    EXPECT_TRUE(list.Contains(1));
    EXPECT_TRUE(list.Contains(3));
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}