#ifndef STORAGE_KVDB_DB_SSTABLE_H_
#define STORAGE_KVDB_DB_SSTABLE_H_

#include "util/KVNode.h"
#include "db/memtable.h"
#include "util/KVNode.h"
#include "util/varint.h"

#include <memory>
#include <fstream>
#include <iostream>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>

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

        std::string name_;
        std::ofstream dest_;
        int fd_ = -1;
        char *mapped_data_ = nullptr;
        size_t file_size_ = 0;
        size_t current_pos_ = 0;
        std::string prevKey;
        SstableNode *next;

        SstableNode(std::string name) : name_(name), next(nullptr)
        {
            if (!MapFile())
            {
                throw std::runtime_error("Failed to map file: " + name);
            }
            AdviseAccessPattern(0, 4096);
        }

        ~SstableNode()
        {
            if (mapped_data_)
            {
                ::munmap(mapped_data_, file_size_);
                mapped_data_ = nullptr;
            }
            if (fd_ != -1)
            {
                ::close(fd_);
                fd_ = -1;
            }
            if (dest_.is_open())
            {
                dest_.close();
            }
        }

        // 在该文件中找key,找到构造kvnode并返回否则返回nullptr
        kvnode Get(const K &key)
        {
            if (!MapFile())
                return nullptr;
            current_pos_ = 0;
            prevKey.clear();

            while (current_pos_ < file_size_)
            {
                // 按需预读下一个数据块（示例：1MB窗口）
                if (current_pos_ % (1 << 20) == 0)
                {
                    size_t read_ahead = std::min(file_size_ - current_pos_, size_t(1 << 20));
                    AdviseAccessPattern(current_pos_, read_ahead);
                }

                // 读取 sharedLength
                uint32_t sharedLength;
                if (!ReadVarint32(sharedLength))
                    return nullptr;

                // 读取 unsharedLength
                uint32_t unsharedLength;
                if (!ReadVarint32(unsharedLength))
                    return nullptr;

                // 边界检查
                if (current_pos_ + unsharedLength > file_size_)
                    return nullptr;

                // 直接访问内存映射区域
                const char *unsharedKey = mapped_data_ + current_pos_;
                current_pos_ += unsharedLength;

                // 构建完整key
                std::string key_str = prevKey.substr(0, sharedLength) +
                                      std::string(unsharedKey, unsharedLength);
                prevKey = key_str;

                // 读取 valueLength
                uint32_t valueLength;
                if (!ReadVarint32(valueLength))
                    return nullptr;

                // 键比较决策
                if (key_str == key)
                {
                    if (current_pos_ + valueLength > file_size_)
                        return nullptr;
                    return std::make_shared<KVnode<K, V>>(
                        key_str,
                        std::string(mapped_data_ + current_pos_, valueLength),
                        KType::kTypeValue);
                }
                else if (key_str > key)
                {
                    return nullptr; // 提前终止
                }

                current_pos_ += valueLength;
            }
            return nullptr;
        }

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
                std::cerr << "Failed to open file for writing: " << name_ << std::endl;
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

    private:
        // 内存映射文件
        bool MapFile()
        {
            if (mapped_data_)
                return true;
            fd_ = ::open(name_.c_str(), O_RDONLY);
            if (fd_ == -1)
            {
                std::cerr << "open failed: " << std::strerror(errno) << ", file: " << name_ << std::endl;
                return false;
            }

            struct stat sb;
            if (::fstat(fd_, &sb) == -1)
            {
                ::close(fd_);
                std::cerr << "fstat failed: " << std::strerror(errno) << std::endl;
                return false;
            }
            file_size_ = sb.st_size;

            if (file_size_ == 0)
            {
                ::close(fd_);
                fd_ = -1;
                mapped_data_ = nullptr;
                return true;
            }

            mapped_data_ = static_cast<char *>(::mmap(nullptr, file_size_, PROT_READ, MAP_PRIVATE, fd_, 0));
            if (mapped_data_ == MAP_FAILED)
            {
                ::close(fd_);
                std::cerr << "mmap failed: " << std::strerror(errno) << ", file: " << name_ << std::endl;
                fd_ = -1;
                return false;
            }
            return true;
        }

        // 按需预读（可选优化）
        void AdviseAccessPattern(size_t offset, size_t length)
        {
            ::posix_madvise(mapped_data_ + offset, length, POSIX_MADV_WILLNEED);
        }

        bool ReadVarint32(uint32_t &result)
        {
            result = 0;
            int shift = 0;
            for (int i = 0; i < 5; ++i)
            { // Varint32最多5字节
                if (current_pos_ >= file_size_)
                    return false;

                uint8_t byte = static_cast<uint8_t>(mapped_data_[current_pos_++]);
                result |= (byte & 0x7F) << shift;
                if ((byte & 0x80) == 0)
                    break;
                shift += 7;
            }
            return true;
        }
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

}

#endif