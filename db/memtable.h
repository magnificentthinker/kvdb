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

    private:
        // std::map<K, kvnode> skiplist_;
        SkipList<K, V> skiplist_;

    public:
        void Insert(kvnode x);
        kvnode Get(const K &key);
    };

    template <typename K, typename V>
    void MemTable<K, V>::Insert(kvnode x)
    {
        skiplist_.Insert(x);
    }

    template <typename K, typename V>
    typename MemTable<K, V>::kvnode MemTable<K, V>::Get(const K &key)
    {

        return skiplist_.Get(key);
    }
}

#endif