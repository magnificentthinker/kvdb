#include <gtest/gtest.h>
#include "db/table.h"
#include <iostream>
using StringTable = kvdb::Table<std::string, int>;
using IntTable = kvdb::Table<int, std::string>;

TEST(TableTest, EmptyTable)
{
    StringTable table(2); // 缓存容量为 2

    ASSERT_EQ(table.Get("key1"), nullptr);
    ASSERT_EQ(table.Get("key2"), nullptr);

    // 测试未命中时从 MemTable 获取（假设 MemTable 已正确实现）
    // 这里需要 Mock MemTable 或假设其行为，暂假设插入后 MemTable 已存储
    // 实际测试需结合 MemTable 实现补充
}

TEST(TableTest, RemoveTable)
{
    StringTable table(2); // 缓存容量为 2
    table.Insert("key1", 1);
    ASSERT_EQ(*table.Get("key1"), 1);
    table.Remove("key1");
    ASSERT_EQ(table.Get("key1"), nullptr);

    // 测试未命中时从 MemTable 获取（假设 MemTable 已正确实现）
    // 这里需要 Mock MemTable 或假设其行为，暂假设插入后 MemTable 已存储
    // 实际测试需结合 MemTable 实现补充
}

TEST(TableTest, BasicInsertAndGet)
{
    StringTable table(2); // 缓存容量为 2

    // 插入键值对
    table.Insert("key1", 100);
    table.Insert("key2", 200);

    // 缓存命中
    ASSERT_EQ(*table.Get("key1"), 100);
    ASSERT_EQ(*table.Get("key2"), 200);

    // 测试未命中时从 MemTable 获取（假设 MemTable 已正确实现）
    // 这里需要 Mock MemTable 或假设其行为，暂假设插入后 MemTable 已存储
    // 实际测试需结合 MemTable 实现补充
}

TEST(TableTest, LRUCacheEviction)
{
    StringTable table(2); // 缓存容量 2

    // 插入三个键，触发 LRU 淘汰（最早插入的 "key1" 应被淘汰）
    table.Insert("key1", 100);
    table.Insert("key2", 200);
    table.Insert("key3", 300);
    table.Get("key1");
    table.Get("key2");
    table.Get("key3");
    // "key1" 被淘汰，缓存中应有 "key2" 和 "key3"
    ASSERT_TRUE(table.cache_.Contains("key2"));
    ASSERT_TRUE(table.cache_.Contains("key3"));
    ASSERT_FALSE(table.cache_.Contains("key1"));

    // 访问 "key2" 后，其变为最近使用，再次插入新键时淘汰 "key3"
    table.Get("key2");
    table.Insert("key4", 400);
    table.Get("key4");
    ASSERT_TRUE(table.cache_.Contains("key2"));
    ASSERT_TRUE(table.cache_.Contains("key4"));
    ASSERT_FALSE(table.cache_.Contains("key3"));
}

TEST(TableTest, RemoveOperation)
{
    StringTable table(2);

    table.Insert("key1", 100);
    table.Get("key1");
    table.Remove("key1");

    // 缓存中应移除 "key1"
    ASSERT_FALSE(table.cache_.Contains("key1"));

    // MemTable 应插入删除标记（需结合 MemTable 测试验证）
    // 假设 MemTable 的 Get 方法能返回删除标记
    // 这里暂验证 Table 的删除逻辑，实际需 Mock MemTable
}

TEST(TableTest, CacheAndMemTableIntegration)
{
    StringTable table(1); // 缓存容量 1

    // 插入键值对，缓存命中
    table.Insert("key1", 100);
    table.Get("key1");
    ASSERT_TRUE(table.cache_.Contains("key1"));

    // 插入相同键，假设缓存返回 true（存在且引用计数 >1）
    // 这里需明确 LRUCache::Insert 的返回逻辑，假设更新值
    table.Insert("key1", 200);
    ASSERT_TRUE(table.cache_.Contains("key1"));
    ASSERT_EQ(*table.Get("key1"), 200);

    // 模拟缓存未命中，从 MemTable 获取并插入缓存
    // 假设 MemTable 已存储 "key2"（通过 Table::Insert 插入）
    table.Insert("key2", 300); // 缓存容量 1，"key1" 被淘汰
    table.Get("key2");
    ASSERT_FALSE(table.cache_.Contains("key1"));
    ASSERT_TRUE(table.cache_.Contains("key2"));
}

TEST(TableTest, StressTestWithLargeData)
{
    const int capacity = 10000;
    IntTable table(capacity);

    // 插入大量数据，测试 LRU 性能和内存释放
    for (int i = 0; i < 2 * capacity; ++i)
    {
        table.Insert(i, "value");
        table.Get(i);
    }

    // 验证最旧的一半数据被淘汰
    for (int i = 0; i < capacity; ++i)
    {
        ASSERT_FALSE(table.cache_.Contains(i));
    }

    for (int i = capacity; i < 2 * capacity; ++i)
    {

        ASSERT_TRUE(table.cache_.Contains(i)); // 出错
    }
}

TEST(TableTest, OverloadData)
{
    const int capacity = 10000;
    StringTable table(capacity);

    // 插入大量数据，测试 LRU 性能和内存释放
    for (int i = 0; i <= 2 * capacity; ++i)
    {
        table.Insert("a", i);
    }

    ASSERT_EQ(*table.Get("a"), 2 * capacity);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
