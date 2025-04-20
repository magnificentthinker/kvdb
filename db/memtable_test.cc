#include <gtest/gtest.h>
#include "db/table.h"
#include <iostream>
#include <vector>
using StringTable = kvdb::Table<std::string, std::string>;
using IntTable = kvdb::Table<int, int>;

// TEST(TableTest, EmptyTable)
// {
//     StringTable table(2); // 缓存容量为 2

//     ASSERT_EQ(table.Get("key1"), nullptr);
//     ASSERT_EQ(table.Get("key2"), nullptr);
// }

TEST(TableTest, StressTestWithLargeData)
{
    const int capacity = 500000;
    StringTable table(capacity);

    // 插入大量数据，测试 LRU 性能和内存释放
    for (int i = 0; i < capacity; ++i)
    {
        std::string a = std::to_string(i);
        table.Insert(a, a);
    }

    for (int i = 0; i < capacity; ++i)
    {
        std::string a = std::to_string(i);
        auto it = table.Get(a);
        if (it == nullptr)
            std::cout << i << '\n';
        ASSERT_TRUE(it != nullptr);
        ASSERT_EQ(*it, a);
    }
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
