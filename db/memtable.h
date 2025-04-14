#ifndef STORAGE_KVDB_DB_MEMTABLE_H_
#define STORAGE_KVDB_DB_MEMTABLE_H_

namespace kvdb
{
    template <typename K, typename V>
    class MemTable
    {
    private:
        enum Ktype
        {
            kTypeValue = 0x0,
            kTypeDelete = 0x1,
        };

        const int KHeaderSize =
    };

}

#endif