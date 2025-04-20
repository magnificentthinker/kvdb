#ifndef STORAGE_KVDB_DB_TABLE_H_
#define STORAGE_KVDB_DB_TABLE_H_
#include "db/memtable.h"
#include "util/LRUCache.h"
#include "util/KVNode.h"
#include "util/writablefile.h"
#include "util/varint.h"
#include "db/sstable.h"
#include <memory>

namespace kvdb
{
    using namespace cache;
    template <typename K, typename V>
    class Table
    {

        typedef std::shared_ptr<KVnode<K, V>> kvnode;
        using MemTablePtr = std::unique_ptr<MemTable<K, V>>;

    private:
        LRUCache<K, V> cache_;

        MemTablePtr memtable_;
        MemTablePtr immutable_memtable_;
        // memtabled最大容量
        const int memtable_max_size_ = 1024 * 1024;
        int version;

        Sstable<K, V> sstable_;

        kvnode NewNode(const K &key, const V &value, KType type);

        // 冻结memtable并切换新memtable
        void FreezeMemtable();
        // 持久化memtable
        void PersistenceMemtable(MemTablePtr memtable);

        inline V *IsKTypeValueReturnValue(const kvnode &x)
        {
            if (x->type != KType::kTypeValue)
            {
                return nullptr;
            }
            return &(x->value);
        }

    public:
        Table(int size) : cache_(size), memtable_(std::make_unique<MemTable<K, V>>()), version(0) {}

        void Insert(const K &key, const V &value);
        V *Get(const K &key);
        void Remove(const K &key);

        ~Table()
        {
            if (memtable_->GetSize() > 0)
            {
                PersistenceMemtable(std::move(memtable_));
                memtable_ = nullptr;
            }
        }
    };

    template <typename K, typename V>
    typename Table<K, V>::kvnode Table<K, V>::NewNode(const K &key, const V &value, KType type)
    {
        return std::make_shared<KVnode<K, V>>(key, value, type);
    }

    template <typename K, typename V>
    void Table<K, V>::Insert(const K &key, const V &value)
    {

        cache_.Remove(key);
        memtable_->Insert(NewNode(key, value, KType::kTypeValue));

        if (memtable_->GetSize() > memtable_max_size_)
        {
            // memtable超过最大容量，进行持久化
            FreezeMemtable();
        }
    }

    // 返回key的value指针，未找到返回nullptr
    template <typename K, typename V>
    V *Table<K, V>::Get(const K &key)
    {

        kvnode x = cache_.Get(key);

        if (x != nullptr)
        {
            // 在缓存中
            return IsKTypeValueReturnValue(x);
        }

        x = memtable_->Get(key);

        if (x != nullptr)
        {
            // 在memtable中
            // 插入到cache内
            cache_.Insert(x);
            return IsKTypeValueReturnValue(x);
        }

        // 如果有immutable在其中查找
        if (immutable_memtable_ != nullptr)
        {
            assert(0);
            x = immutable_memtable_->Get(key);
            if (x != nullptr)
            {

                cache_.Insert(x);
                return IsKTypeValueReturnValue(x);
            }
        }

        x = sstable_.Get(key);

        if (x != nullptr)
        {

            cache_.Insert(x);
            return IsKTypeValueReturnValue(x);
        }

        return nullptr;
    }

    template <typename K, typename V>
    void Table<K, V>::Remove(const K &key)
    {
        // 删除缓存中的key
        cache_.Remove(key);
        // Insert一个delete类型的节点进入memtable
        memtable_->Insert(NewNode(key, V(), KType::kTypeDelete));
    }

    template <typename K, typename V>
    void Table<K, V>::FreezeMemtable()
    {

        memtable_->Freeze();
        immutable_memtable_ = std::move(memtable_);
        memtable_ = std::make_unique<MemTable<K, V>>();

        PersistenceMemtable(std::move(immutable_memtable_));
        immutable_memtable_ = nullptr;
    }

    // shared length|unshared length| key | value length | value
    // length至少占一字节 length为0时也占一字节
    template <typename K, typename V>
    void Table<K, V>::PersistenceMemtable(MemTablePtr memtable)
    {

        if (!sstable_.WriterNewFile(std::move(memtable)))
            throw std::runtime_error("write file error");
    }

}

#endif