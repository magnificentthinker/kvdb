#include <gtest/gtest.h>
#include "util/LRUCache.h"

namespace kvdb
{
    namespace cache
    {

        // 测试 Insert 和 Get 方法
        TEST(LRUCacheTest, InsertAndGet)
        {
            LRUCache<int, int> cache(10);
            cache.Insert(2, 1);

            EXPECT_EQ(cache.Get(2), 1);
        }

        // 测试 LRU 策略，插入超过容量后旧数据被淘汰
        TEST(LRUCacheTest, LRUPolicy)
        {
            LRUCache<int, int> cache(3);
            cache.Insert(1, 10);
            cache.Insert(2, 20);
            cache.Insert(3, 30);
            cache.Insert(4, 40); // 插入后 1 应该被淘汰

            EXPECT_EQ(cache.Get(2), 20);
            EXPECT_EQ(cache.Get(3), 30);
            EXPECT_EQ(cache.Get(4), 40);
            EXPECT_FALSE(cache.Contains(1));
        }

        // 测试更新值的功能
        TEST(LRUCacheTest, UpdateValue)
        {
            LRUCache<int, int> cache(3);
            cache.Insert(1, 10);
            cache.Insert(2, 20);

            cache.Insert(1, 100); // 更新键为 1 的值

            EXPECT_EQ(cache.Get(1), 100);
            EXPECT_EQ(cache.Get(2), 20);
        }

        // 测试 Get 方法移动节点到前端的功能
        TEST(LRUCacheTest, MoveNodeToFrontOnGet)
        {
            LRUCache<int, int> cache(3);
            cache.Insert(1, 10);
            cache.Insert(2, 20);
            cache.Insert(3, 30);
            cache.Get(1);
            cache.Insert(4, 40);

            EXPECT_EQ(cache.Get(1), 10);
            EXPECT_FALSE(cache.Contains(2));
        }

    }
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
