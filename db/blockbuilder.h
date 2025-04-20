#ifndef STORAGE_KVDB_DB_BLOCKBUILDER_H_
#define STORAGE_KVDB_DB_BLOCKBUILDER_H_

#include "util/varint.h"

#include <cassert>
#include <vector>
#include <string>
#include <string_view>
#include <iostream>
namespace kvdb
{

    class BlockBuilder
    {
    private:
        // sharekey 间隔
        const int block_interval_ = 4;
        // 重启点
        std::vector<int> restart_;

        std::string buf_;
        int count;
        std::string last_key_;

    public:
        BlockBuilder();

        void Reset();

        void Add(std::string_view key, std::string_view value);

        std::string &Finish();
    };

    BlockBuilder::BlockBuilder() : restart_(), count(0)
    {
        restart_.push_back(0);
    }

    // 重置builder
    void BlockBuilder::Reset()
    {
        buf_.clear();
        restart_.clear();
        restart_.emplace_back(0);
        count = 0;
        last_key_.clear();
    }

    void BlockBuilder::Add(std::string_view key, std::string_view value)
    {
        int shared = 0;
        if (count < block_interval_)
        {
            //  没超过间隔
            //  计算当前key与lastkey的相同前缀长度
            int min_length = std::min(key.length(), last_key_.length());
            while (shared < min_length && key[shared] == last_key_[shared])
                ++shared;
        }
        else
        {
            count = 0;
            restart_.push_back(buf_.size());
        }
        int unshared = key.length() - shared;

        putvarint(shared, &buf_);
        putvarint(unshared, &buf_);
        buf_.append(key.data() + shared, unshared);
        putvarint(value.length(), &buf_);
        buf_.append(value.data(), value.length());

        // 更新上一个key
        last_key_.resize(shared);
        last_key_.append(key.data() + shared, unshared);
        ++count;
    }

    std::string &BlockBuilder::Finish()
    {
        // 写入重启点
        for (int i : restart_)
        {
            putint(i, &buf_);
        }
        putint(restart_.size(), &buf_);

        return buf_;
    }
}

#endif