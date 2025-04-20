#ifndef STROAGE_KVDB_UTIL_VARINT_H
#define STROAGE_KVDB_UTIL_VARINT_H
#include <assert.h>
namespace kvdb
{

    // Varint 编码函数 返回length
    int varint_encode(int num, char *buf)
    {
        int length = 0;
        do
        {
            char byte = static_cast<char>(num & 0x7F);
            num >>= 7;
            if (num != 0)
            {
                byte |= 0x80;
            }
            buf[length++] = byte;
        } while (num != 0);

        assert(length <= 10);
        return length;
    }

    // Varint 解码函数 传入地址和size,返回解码后的内容
    int varint_decode(const char *encoded, int size)
    {
        uint64_t num = 0;
        int shift = 0;
        for (int i = 0; i < size; ++i)
        {
            unsigned char byte = encoded[i];
            num |= static_cast<uint64_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0)
            {
                break;
            }
            shift += 7;
        }
        return num;
    }
}

#endif