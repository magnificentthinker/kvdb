#include "util/LRUCache.h"

#include <gtest/gtest.h>

#include <thread>
#include <iostream>
#include <cassert>
using namespace kvdb::cache;

TEST(HashTest, EmptyList)
{
    HashTable<std::string, int> list;
    EXPECT_TRUE(list.Find("wh") == nullptr);
}

TEST(HashTest, Insert)
{
    HashTable<std::string, int> list;
    std::string a = "wh";
    LRUNode<std::string, int> *node = new LRUNode(a, 1);
    list.Insert(node);
    EXPECT_TRUE(list.Find(a) != nullptr);
}

TEST(HashTest, Remove)
{
    HashTable<std::string, int> list;
    std::string a = "wh";
    std::string b = "hh";
    LRUNode<std::string, int> *node1 = new LRUNode(a, 1);
    LRUNode<std::string, int> *node2 = new LRUNode(b, 2);

    list.Insert(node1);
    list.Insert(node2);
    list.Remove("wh");
    EXPECT_TRUE(list.Find(a) == nullptr);
    EXPECT_TRUE(list.Find(b)->value_ == 2);
}

TEST(HashTest, ReInsert)
{
    HashTable<std::string, int> list;
    std::string a = "wh";
    LRUNode<std::string, int> *node = new LRUNode(a, 1);
    list.Insert(node);
    node = new LRUNode(a, 2);

    list.Insert(node);
    EXPECT_TRUE(list.Find(a)->value_ == 2);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}