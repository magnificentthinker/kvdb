#ifndef STORAGE_KVDB_DB_SKIPLIST_H_
#define STORAGE_KVDB_DB_SKIPLIST_H_
#include <cassert>
#include <atomic>
#include <malloc.h>
#include "util/random.h"
#include <iostream>
namespace kvdb
{

    template <typename Key>
    class SkipList
    {
    private:
        struct Node;

    public:
        SkipList();

        // insert key into skiplist
        void Insert(const Key &key);

        // if key in skiplist return true
        bool Contains(const Key &key) const;

    private:
        inline int GetMaxHeight() const { return max_height_.load(std::memory_order_relaxed); }

        const int KMaxHeight = 12;
        Node *const head_;
        Node *NewNode(const Key &key, int height);
        std::atomic<int> max_height_;

        Node *FindNodeEqual(const Key &key, Node **prev) const;
        int RandomHeight();

        Random rnd_;
    };

    template <typename Key>
    struct SkipList<Key>::Node
    {
        explicit Node(const Key &k) : key(k) {}

        Key const key;

        Node *Next(int n)
        {
            assert(n >= 0);
            // 返回第n层的next指针
            return next_[n].load(std::memory_order_acquire);
        }
        void SetNext(int n, Node *x)
        {
            assert(n >= 0);
            // 存储第n层的next
            next_[n].store(x, std::memory_order_release);
        }

        Node *NoBarrier_Next(int n)
        {
            assert(n >= 0);
            return next_[n].load(std::memory_order_relaxed);
        }
        void NoBarrier_SetNext(int n, Node *x)
        {
            assert(n >= 0);
            next_[n].store(x, std::memory_order_relaxed);
        }

    private:
        // next_是一个不定长数组，next[i] 代表第i层
        std::atomic<Node *> next_[1];
    };

    template <typename Key>
    struct SkipList<Key>::Node *SkipList<Key>::NewNode(const Key &key, int height)
    {
        char *const node_memory = (char *const)malloc(sizeof(Node) + (height - 1) * sizeof(std::atomic<Node *>));
        return new (node_memory) Node(key);
    }

    template <typename Key>
    SkipList<Key>::SkipList() : head_(NewNode(Key(), KMaxHeight)), max_height_(1), rnd_(0xdeadbeef)
    {

        for (int i = 0; i < KMaxHeight; ++i)
            head_->SetNext(i, nullptr);
    }

    template <typename Key>
    struct SkipList<Key>::Node *SkipList<Key>::FindNodeEqual(const Key &key, Node **prev) const
    {
        Node *now = head_;

        int height = GetMaxHeight();
        while (height--)
        {
            Node *next = now->Next(height);
            while (next != nullptr && next->key <= key)
            {
                now = next;
                next = now->Next(height);
            }
            if (prev != nullptr)
                prev[height] = now;
        }

        return now;
    }

    template <typename Key>
    bool SkipList<Key>::Contains(const Key &key) const
    {
        Node *now = FindNodeEqual(key, nullptr);
        return (now != nullptr && now->key == key);
    }

    template <typename Key>
    int SkipList<Key>::RandomHeight()
    {
        // Increase height with probability 1 in kBranching
        static const unsigned int kBranching = 4;
        int height = 1;
        while (height < KMaxHeight && rnd_.OneIn(kBranching))
        {
            height++;
        }
        assert(height > 0);
        assert(height <= KMaxHeight);
        return height;
    }

    template <typename Key>
    void SkipList<Key>::Insert(const Key &key)
    {

        Node *prev[KMaxHeight];
        Node *x = FindNodeEqual(key, prev);

        if (x->key == key)
            std::cout << key << '\n';
        assert(x == nullptr || x->key != key);

        int height = RandomHeight();
        if (height > GetMaxHeight())
        {
            for (int i = GetMaxHeight(); i < KMaxHeight; ++i)
                prev[i] = head_;
            max_height_.store(height, std::memory_order_relaxed);
        }

        x = NewNode(key, height);

        for (int i = 0; i < height; ++i)
        {
            x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
            prev[i]->SetNext(i, x);
        }
    }
}
#endif