#include "db/skiplist.h"

#include <gtest/gtest.h>

#include <thread>
#include <iostream>
#include <cassert>
using namespace kvdb;

TEST(SkipListTest, EmptyList)
{
    SkipList<int> list;
    EXPECT_FALSE(list.Contains(10));
}

TEST(SkipListTest, InsertInterage)
{
    const int N = 1000;
    SkipList<int> list;

    for (int i = 1; i < N; ++i)
        list.Insert(i);
    for (int i = 1; i < N; ++i)
        EXPECT_TRUE(list.Contains(i));
}

TEST(SkipListTest, InsertChar)
{
    SkipList<char> list;

    for (int i = 0; i < 26; ++i)
        list.Insert(i + 'a');
    for (int i = 0; i < 26; ++i)
        EXPECT_TRUE(list.Contains(i + 'a'));
}

TEST(SkipListTest, InsertString)
{
    SkipList<std::string> list;
    std::string x = "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < 25; ++i)
    {
        std::string key = x.substr(i, 1);
        list.Insert(key);
    }

    for (int i = 0; i < 25; ++i)
    {
        std::string key = x.substr(i, 1);
        EXPECT_TRUE(list.Contains(key));
    }
}

// 插入函数，用于线程执行
void insertRange(SkipList<int> &list, int start, int end)
{
    for (int i = start; i < end; ++i)
    {
        list.Insert(i);
    }
}

// 检查函数，用于线程执行
void checkRange(SkipList<int> &list, int start, int end)
{
    for (int i = start; i < end; ++i)
    {
        EXPECT_TRUE(list.Contains(i));
    }
}

// 多线程测试用例
TEST(SkipListTest, InsertInterageMultiThreaded)
{
    const int numThreads = 4;
    const int N = 1000;
    SkipList<int> list;

    std::vector<std::thread> insertThreads;
    std::vector<std::thread> checkThreads;

    // 启动插入线程
    int chunkSize = N / numThreads;

    insertThreads.emplace_back(insertRange, std::ref(list), 1, N);

    // 等待所有插入线程完成
    for (auto &thread : insertThreads)
    {
        thread.join();
    }

    // 启动检查线程
    for (int i = 0; i < numThreads; ++i)
    {
        int start = std::max(1, i * chunkSize);
        int end = (i == numThreads - 1) ? N : (i + 1) * chunkSize;
        checkThreads.emplace_back(checkRange, std::ref(list), start, end);
    }

    // 等待所有检查线程完成
    for (auto &thread : checkThreads)
    {
        thread.join();
    }
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}