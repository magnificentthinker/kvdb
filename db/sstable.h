#ifndef STORAGE_KVDB_DB_SSTABLE_H_
#define STORAGE_KVDB_DB_SSTABLE_H_

#include "util/KVNode.h"
#include "db/memtable.h"
#include "util/KVNode.h"
#include "util/varint.h"

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

            if (x->PersistenceMemtable(std::move(memtable)))
            {
                // 插入sstable
                Insert(x);
                version_++;
                return true;
            }
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

        // sharekey的间隔
        const int block_interval_ = 4;

        std::string name_;
        std::ofstream dest_;
        std::ifstream file_;
        SstableNode(std::string name) : name_(name), next(nullptr) {}

        // 在该文件中找key,找到构造kvnode并返回否则返回nullptr
        kvnode Get(const K &key);
        bool PersistenceMemtable(MemTablePtr memtable)
        {
            auto it = memtable->Iterator();

            open();

            char buf[10] = {0};
            const K *frontkey = nullptr;
            int length;
            int num = 0;
            while (it->Valid())
            {
                bool con = true;
                // std::string tmp = std::to_string(it->key());
                //  计算当前key与之前key的相同length
                int shared = shared_length(frontkey, &it->key());

                length = varint_encode(shared, buf);
                con &= Append(buf, length);
                // 计算不同key
                int unshared = it->key().length() - shared;
                length = varint_encode(unshared, buf);

                con &= Append(buf, length);
                // 放入不共享key

                con &= Append(it->key().substr(shared).data(), unshared);

                // 计算value

                if (it->type() == KType::kTypeValue)
                {
                    length = varint_encode(it->value().length(), buf);
                    con &= Append(buf, length);

                    con &= Append(it->value().data(), it->value().length());
                }
                else
                {
                    length = varint_encode(0, buf);
                    con &= Append(buf, length);
                }

                if (!con)
                    return con;
                frontkey = &it->key();
                it->Next();
                num++;
                // 到interval间隔
                if (num % block_interval_ == 0)
                {
                    frontkey = nullptr;
                }
            }

            close();
            return true;
        }

        void open()
        {
            // 打开文件流
            dest_.open(name_);
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

        int shared_length(const K *fr, const K *now);

        SstableNode *next;
    };

    // 返回相同前缀长度
    template <typename K, typename V>
    int Sstable<K, V>::SstableNode::shared_length(const K *fr, const K *now)
    {
        assert(now != nullptr);
        assert(*now != K());

        if (fr == nullptr)
            return 0;

        int length = 0;
        int flength = (*fr).length();
        int nowlength = (*now).length();
        while (length < flength && length < nowlength && (*fr)[length] == (*now)[length])
            length++;

        return length;
    }

    template <typename K, typename V>
    typename Sstable<K, V>::kvnode Sstable<K, V>::SstableNode::Get(const K &expectkey)
    {

        file_.open(name_);
        if (!file_.is_open())
        {
            assert(0);
            return nullptr;
        }

        std::string prevKey = "";
        while (true)
        {

            // 记录当前文件读取位置，用于后续检查是否读取到文件末尾
            std::streampos currentPos = file_.tellg();

            // 读取sharedkeylength
            char buf[10];
            int index = 0;
            unsigned char byte;
            do
            {
                if (file_.read(reinterpret_cast<char *>(&byte), 1).fail())
                {
                    // 读取失败，可能是文件结束，直接退出循环
                    assert(0);
                    return nullptr;
                }
                buf[index++] = byte;
            } while (byte & 0x80);
            int sharedLength = varint_decode(buf, index);

            // 读取unsharedkeylength
            index = 0;
            do
            {
                if (file_.read(reinterpret_cast<char *>(&byte), 1).fail())
                {
                    assert(0);
                    return nullptr;
                }
                buf[index++] = byte;
            } while (byte & 0x80);
            int unsharedLength = varint_decode(buf, index);

            // 读取unsharedkey
            std::string unsharedKey(unsharedLength, '\0');
            if (file_.read(&unsharedKey[0], unsharedLength).fail())
            {
                assert(0);
                return nullptr;
            }

            // 构建key
            std::string key = prevKey.substr(0, sharedLength) + unsharedKey;
            prevKey = key;

            // 读取valuelength
            index = 0;
            do
            {
                if (file_.read(reinterpret_cast<char *>(&byte), 1).fail())
                {
                    assert(0);
                    return nullptr;
                }
                buf[index++] = byte;
            } while (byte & 0x80);
            int valueLength = varint_decode(buf, index);

            // 读取value
            std::string value(valueLength, '\0');
            if (file_.read(&value[0], valueLength).fail())
            {
                assert(0);
                return nullptr;
            }

            if (key == expectkey)
            {

                // 找到返回当前kvnode
                file_.close();
                return std::make_shared<KVnode<K, V>>(key, value, KType::kTypeValue);
            }

            // 检查是否已经读取到文件末尾
            if (file_.eof())
            {
                break;
            }
        }

        file_.close();

        return nullptr;
    }
}

#endif