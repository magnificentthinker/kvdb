#ifndef STORAGE_KVDB_DB_BLOCKREADER_H_
#define STORAGE_KVDB_DB_BLOCKREADER_H_

#include <fstream>
#include <string_view>
#include "util/varint.h"
#include <iostream>
#include <bitset>
namespace kvdb
{

    class BlockReader
    {
    private:
        const int block_interval_ = 4;
        std::string name_;
        std::ifstream file_;
        int restart_num_;
        void ReadRestart();
        int GetOffset(int index);

        int BinarySearch(std::string_view key);

        void DecodeEntry(int &sharedLength, int &unsharedLength, std::string &unsharedKey, int &valueLength, std::string &value);

    public:
        BlockReader(std::string name) : restart_num_(0), name_(name)
        {
        }

        bool Get(std::string_view key, std::string *buf);
    };

    void BlockReader::ReadRestart()
    {
        // 指向重启点u数目
        file_.seekg(-4, std::ios::end);
        if (file_.fail())
        {
            assert(0);
        }

        char buf[4];
        if (file_.read(buf, 4).fail())
        {
            assert(0);
        }
        // 得到重启点数目
        restart_num_ = int_deconde(buf);
    }

    // 根据重启点index读取具体重启点内指向的data偏移位置
    int BlockReader::GetOffset(int index)
    {

        file_.seekg(-4 - (restart_num_ - index) * 4, std::ios::end);

        char buf[4] = {0};
        if (file_.read(buf, 4).fail())
        {
            assert(0);
        }

        return int_deconde(buf);
    }

    // 查找key,找到返回true
    bool BlockReader::Get(std::string_view key, std::string *buf)
    {

        file_.open(name_, std::ios::binary);
        if (!file_.is_open())
        {
            assert(0);
        }

        ReadRestart();

        int offset = BinarySearch(key);
        file_.seekg(offset, std::ios::beg);

        std::string last_key;
        std::string current_key;
        int shared, unshared, valuelength;
        for (int i = 0; i < block_interval_; ++i)
        {
            buf->clear();
            DecodeEntry(shared, unshared, current_key, valuelength, *buf);

            // 改了好久的bug ，此时的currentkey是unsharedkey
            last_key.resize(shared);
            last_key.append(current_key.data(), unshared);

            if (last_key == key)
            {
                file_.close();
                return true;
            }
        }

        file_.close();
        return false;
    }

    // 对重启点进行二分查找
    int BlockReader::BinarySearch(std::string_view expectkey)
    {

        int l = 0, r = restart_num_ - 1;
        while (l < r)
        {
            int mid = (l + r + 1) >> 1;
            int offset = GetOffset(mid);

            file_.seekg(offset, std::ios::beg);

            int shared, unshared, valuelength;
            std::string key, value;

            DecodeEntry(shared, unshared, key, valuelength, value);

            if (key <= expectkey)
            {

                l = mid;
            }
            else
            {
                r = mid - 1;
            }
        }

        return GetOffset(l);
    }

    void BlockReader::DecodeEntry(int &sharedLength, int &unsharedLength, std::string &unsharedKey, int &valueLength, std::string &value)
    {

        char buf[10];
        int index = 0;
        unsigned char byte;
        do
        {
            if (file_.read(reinterpret_cast<char *>(&byte), 1).fail())
            {
                // 读取失败，可能是文件结束，直接退出循环
                assert(0);
            }
            buf[index++] = byte;
        } while (byte & 0x80);
        sharedLength = varint_decode(buf, index);

        // 读取unsharedkeylength
        index = 0;
        do
        {
            if (file_.read(reinterpret_cast<char *>(&byte), 1).fail())
            {
                assert(0);
            }
            buf[index++] = byte;
        } while (byte & 0x80);
        unsharedLength = varint_decode(buf, index);

        // 读取unsharedkey
        unsharedKey.resize(unsharedLength);
        if (file_.read(&unsharedKey[0], unsharedLength).fail())
        {
            assert(0);
        }

        // 读取valuelength
        index = 0;
        do
        {
            if (file_.read(reinterpret_cast<char *>(&byte), 1).fail())
            {
                assert(0);
            }
            buf[index++] = byte;
        } while (byte & 0x80);
        valueLength = varint_decode(buf, index);

        // 读取value
        value.resize(valueLength);
        if (file_.read(&value[0], valueLength).fail())
        {
            assert(0);
        }
    }
}

#endif