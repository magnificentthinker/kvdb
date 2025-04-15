#ifndef STORAGE_KVDB_UTIL_KVNODE_H_
#define STORAGE_KVDB_UTIL_KVNODE_H_

namespace kvdb
{
    enum class KType
    {
        kTypeValue = 0x0,
        kTypeDelete = 0x1,
    };

    template <typename K, typename V>
    struct KVnode
    {
        K key;
        V value;

        KType type;
        KVnode(K k, V v, KType t) : key(k), value(v), type(t) {}
    };
}

#endif