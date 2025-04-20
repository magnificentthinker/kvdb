#include <gtest/gtest.h>
#include "db/blockbuilder.h"
#include "db/blockreader.h"
#include <iostream>
#include <vector>
#include <fstream>

// TEST(TableTest, EmptyTable)
// {
//     StringTable table(2); // 缓存容量为 2

//     ASSERT_EQ(table.Get("key1"), nullptr);
//     ASSERT_EQ(table.Get("key2"), nullptr);
// }

TEST(TableTest, StressTestWithLargeData)
{
    const int capacity = 20;
    kvdb::BlockBuilder builder;

    builder.Add("0", "0");
    builder.Add("1", "1");
    // 插入大量数据，测试 LRU 性能和内存释放
    for (int i = 10; i < capacity; ++i)
    {
        std::string a = std::to_string(i);
        builder.Add(a, a);
    }

    for (int i = 2; i < 10; ++i)
    {
        std::string a = std::to_string(i);
        builder.Add(a, a);
    }
    std::ofstream dest_;
    dest_.open("wh", std::ios::binary | std::ios::trunc);
    std::string tmp = builder.Finish();
    dest_.write(tmp.data(), tmp.size());
    dest_.close();

    kvdb::BlockReader reader("wh");
    std::string value;
    reader.Get("4", &value);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
