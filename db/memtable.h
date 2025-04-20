#ifndef STORAGE_KVDB_DB_MEMTABLE_H_
#define STORAGE_KVDB_DB_MEMTABLE_H_
#include "util/KVNode.h"
#include "skiplist.h"
#include <memory>
#include <map>

namespace kvdb
{
    template <typename K, typename V>
    class MemTable
    {
        typedef std::shared_ptr<KVnode<K, V>> kvnode;
        using Iter = typename SkipList<K, V>::Iterator;

    private:
        SkipList<K, V> skiplist_;

        // 记录memtablei大小
        int size_;

        bool frozen_;

    public:
        MemTable() : size_(0), frozen_(false) {};
        void Insert(kvnode x);
        kvnode Get(const K &key);
        int GetSize() { return size_; }
        // 冻结该memtable 即改为只读
        void Freeze() { frozen_ = true; }
        std::unique_ptr<Iter> Iterator() { return std::make_unique<Iter>(&skiplist_); }
    };

    template <typename K, typename V>
    void MemTable<K, V>::Insert(kvnode x)
    {
        if (frozen_)
            throw std::runtime_error("在只读table内插入数据");

        size_ += sizeof(x);
        skiplist_.Insert(x);
    }

    template <typename K, typename V>
    typename MemTable<K, V>::kvnode MemTable<K, V>::Get(const K &key)
    {

        return skiplist_.Get(key);
    }
}

#endif