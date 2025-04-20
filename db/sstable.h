#ifndef STORAGE_KVDB_DB_SSTABLE_H_
#define STORAGE_KVDB_DB_SSTABLE_H_

#include "util/KVNode.h"
#include "db/memtable.h"
#include "util/KVNode.h"
#include "util/varint.h"
#include "db/blockbuilder.h"
#include "db/blockreader.h"

#include <memory>
#include <fstream>
#include <iostream>
namespace kvdb
{

    template <typename K, typename V>
    class Sstable
    {
    private:
        struct SstableNode;

        using kvnode = std::shared_ptr<KVnode<K, V>>;
        using ssnodeptr = SstableNode *;
        using MemTablePtr = std::unique_ptr<MemTable<K, V>>;

    private:
        int version_;
        BlockBuilder blockbuilder_;

        ssnodeptr head_;
        ssnodeptr NewNode()
        {
            return new SstableNode(std::string("build/memtable_test") + std::to_string(version_) + std::string(".sst"));
        }

        void Insert(ssnodeptr x)
        {
            if (head_ == nullptr)
            {
                head_ = x;
            }
            else
            {
                x->next = head_;
                head_ = x;
            }
        }

    public:
        Sstable() : version_(0), head_(nullptr) {};
        ~Sstable()
        {

            ssnodeptr x = head_;
            while (x != nullptr)
            {
                ssnodeptr next = x->next;

                delete x;
                x = next;
            }
        }
        bool WriterNewFile(MemTablePtr memtable)
        {
            ssnodeptr x = NewNode();
            auto it = memtable->Iterator();
            while (it->Valid())
            {
                blockbuilder_.Add(it->key(), it->value());
                it->Next();
            }

            if (x->PersistenceMemtable(blockbuilder_.Finish()))
            {
                // 插入sstable
                Insert(x);
                version_++;
                blockbuilder_.Reset();
                return true;
            }
            blockbuilder_.Reset();
            return false;
        }
        kvnode Get(const K &key)
        {

            ssnodeptr x = head_;
            while (x != nullptr)
            {

                kvnode ptr = x->Get(key);
                if (ptr != nullptr)
                    return ptr;
                x = x->next;
            }
            return nullptr;
        }
    };

    template <typename K, typename V>
    struct Sstable<K, V>::SstableNode
    {

        std::string name_;
        std::ofstream dest_;
        BlockReader blockreader_;
        SstableNode(std::string name) : name_(name), next(nullptr), blockreader_(name_)
        {
        }

        // 在该文件中找key,找到构造kvnode并返回否则返回nullptr
        kvnode Get(const K &key);

        bool PersistenceMemtable(std::string &content);

        void open()
        {
            // 打开文件流
            dest_.open(name_, std::ios::binary);
            if (!dest_.is_open())
            {
                std::cerr << "Failed to open file: " << name_ << std::endl;
            }
        }

        void close()
        {
            dest_.close();
        }

        bool Append(const char *data, int size)
        {
            dest_.write(data, size);
            if (dest_.fail())
            {
                // 写入失败
                return false;
            }
            return true;
        }

        SstableNode *next;
    };

    // 需要存入重启点
    template <typename K, typename V>
    bool Sstable<K, V>::SstableNode::PersistenceMemtable(std::string &content)
    {

        open();
        bool x = Append(content.data(), content.length());
        close();
        return x;
    }
    template <typename K, typename V>
    typename Sstable<K, V>::kvnode Sstable<K, V>::SstableNode::Get(const K &expectkey)
    {
        assert(!dest_.is_open());
        std::string value;
        if (blockreader_.Get(expectkey, &value))
        {
            return std::make_shared<KVnode<K, V>>(expectkey, value, KType::kTypeValue);
        }
        else
        {

            return nullptr;
        }
    }
}

#endif