#ifndef STORAGE_KVDB_DB_TABLE_H_
#define STORAGE_KVDB_DB_TABLE_H_
#include "db/memtable.h"
#include "util/LRUCache.h"
#include "util/KVNode.h"
#include <memory>
namespace kvdb
{
    using namespace cache;
    template <typename K, typename V>
    class Table
    {

        typedef std::shared_ptr<KVnode<K, V>> kvnode;

    private:
        MemTable<K, V> memtable_;

        kvnode NewNode(const K &key, const V &value, KType type);

        inline V *IsKTypeValueReturnValue(const kvnode &x)
        {
            if (x->type != KType::kTypeValue)
            {
                return nullptr;
            }
            return &(x->value); // 假设 KVNode 的 value 是直接存储的值（非指针）
        }

    public:
        LRUCache<K, V> cache_;
        Table(int size) : cache_(size) {}

        void Insert(const K &key, const V &value);
        V *Get(const K &key);
        void Remove(const K &key);
    };

    template <typename K, typename V>
    typename Table<K, V>::kvnode Table<K, V>::NewNode(const K &key, const V &value, KType type)
    {
        return std::make_shared<KVnode<K, V>>(key, value, type);
    }

    template <typename K, typename V>
    void Table<K, V>::Insert(const K &key, const V &value)
    {
        // kv节点在memtable和缓存内直接被修改返回true，kv节点只在缓存或不存在返回false需要插入到memtable中
        if (!cache_.Insert(key, value))
            memtable_.Insert(NewNode(key, value, KType::kTypeValue));
    }

    template <typename K, typename V>
    V *Table<K, V>::Get(const K &key)
    {
        kvnode x = cache_.Get(key);

        if (x != nullptr)
        {
            // 在缓存中
            return IsKTypeValueReturnValue(x);
        }
        else
        {
            x = memtable_.Get(key);
            if (x != nullptr)
            {
                // 在memtable中
                // 插入到cache内
                cache_.Insert(x);
                return IsKTypeValueReturnValue(x);
            }
            else
            {
                return nullptr;
                // 在磁盘内查找
            }
        }
        throw std::runtime_error("Key not found");
    }

    template <typename K, typename V>
    void Table<K, V>::Remove(const K &key)
    {
        // 删除缓存中的key
        cache_.Remove(key);
        // Insert一个delete类型的节点进入memtable
        memtable_.Insert(NewNode(key, V(), KType::kTypeDelete));
    }
}

#endif