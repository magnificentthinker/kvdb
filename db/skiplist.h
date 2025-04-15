#ifndef STORAGE_KVDB_DB_SKIPLIST_H_
#define STORAGE_KVDB_DB_SKIPLIST_H_
#include <cassert>
#include <atomic>
#include <malloc.h>
#include "util/random.h"
#include "util/KVNode.h"
#include <memory>
namespace kvdb
{

    template <typename K, typename V>
    class SkipList
    {
        typedef std::shared_ptr<KVnode<K, V>> kvnode;

    private:
        struct Node;

    public:
        SkipList();

        // insert key into skiplist
        void Insert(kvnode x);

        kvnode Get(const K &key);
        // if key in skiplist return true
        bool Contains(const K &key) const;

    private:
        inline int GetMaxHeight() const { return max_height_.load(std::memory_order_relaxed); }

        const int KMaxHeight = 12;
        Node *const head_;
        Node *NewNode(kvnode key, int height);
        std::atomic<int> max_height_;

        Node *FindNodeEqual(const K &key, Node **prev) const;
        int RandomHeight();

        Random rnd_;
    };

    template <typename K, typename V>
    struct SkipList<K, V>::Node
    {

        explicit Node(std::shared_ptr<KVnode<K, V>> k) : kvnode_(k) {}

        std::shared_ptr<KVnode<K, V>> kvnode_;

        inline K &key() { return kvnode_->key; }
        inline V &value() { return kvnode_->value; }
        inline KType ktype() { return kvnode_->type; }
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

    template <typename K, typename V>
    struct SkipList<K, V>::Node *SkipList<K, V>::NewNode(kvnode x, int height)
    {
        char *const node_memory = (char *const)malloc(sizeof(Node) + (height - 1) * sizeof(std::atomic<Node *>));
        return new (node_memory) Node(x);
    }

    template <typename K, typename V>
    SkipList<K, V>::SkipList() : head_(NewNode(kvnode(), KMaxHeight)), max_height_(1), rnd_(0xdeadbeef)
    {

        for (int i = 0; i < KMaxHeight; ++i)
            head_->SetNext(i, nullptr);
    }

    template <typename K, typename V>
    struct SkipList<K, V>::Node *SkipList<K, V>::FindNodeEqual(const K &key, Node **prev) const
    {
        Node *now = head_;

        int height = GetMaxHeight();
        while (height--)
        {
            Node *next = now->Next(height);
            while (next != nullptr && next->key() <= key)
            {
                now = next;
                next = now->Next(height);
            }
            if (prev != nullptr)
                prev[height] = now;
        }
        return now;
    }

    template <typename K, typename V>
    bool SkipList<K, V>::Contains(const K &key) const
    {
        Node *now = FindNodeEqual(key, nullptr);
        return (now != nullptr && now->key() == key);
    }

    template <typename K, typename V>
    int SkipList<K, V>::RandomHeight()
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

    template <typename K, typename V>
    void SkipList<K, V>::Insert(kvnode kvnode)
    {

        K &key = kvnode->key;
        Node *prev[KMaxHeight];
        Node *x = FindNodeEqual(key, prev);

        int height = RandomHeight();
        if (height > GetMaxHeight())
        {
            for (int i = GetMaxHeight(); i < KMaxHeight; ++i)
                prev[i] = head_;
            max_height_.store(height, std::memory_order_relaxed);
        }

        x = NewNode(kvnode, height);

        for (int i = 0; i < height; ++i)
        {
            x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
            prev[i]->SetNext(i, x);
        }
    }

    template <typename K, typename V>
    typename SkipList<K, V>::kvnode SkipList<K, V>::Get(const K &key)
    {
        Node *x = FindNodeEqual(key, nullptr);

        // 找到node有三种情况，找不到证明可能持久化可能不存在return nullptr
        // 找到判断type, ktype == delete 证明删除了 返回nullptr
        // ktype == value 才返回kvnode
        return (x != head_ && x != nullptr && x->key() == key && x->ktype() == KType::kTypeValue) ? x->kvnode_ : nullptr;
    }
}
#endif