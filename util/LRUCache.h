#ifndef STORAGE_KVDB_UTIL_LRUCACHE_H_
#define STORAGE_KVDB_UTIL_LRUCACHE_H_
#include <functional>
#include "util/KVNode.h"
#include <memory>

namespace kvdb
{
    namespace cache
    {
        template <typename K, typename V>
        struct LRUNode
        {
            std::shared_ptr<KVnode<K, V>> kvnode_;

            LRUNode(std::shared_ptr<KVnode<K, V>> x) : kvnode_(x)
            {
            }
            K &key() { return kvnode_->key; }
            V &value() { return kvnode_->value; }
            void SetValue(const V &value) { kvnode_->value = value; }
            LRUNode *next_hash;
            LRUNode *next;
            LRUNode *prev;
        };

        template <typename K, typename V>
        class HashTable
        {
            typedef LRUNode<K, V> Node;

        private:
            int length_ = 4096;
            int elems_;
            Node **list_;
            std::hash<K> hasher;

            Node **new_list_;
            int new_length_;
            const double load_factor_threshold = 0.75;
            bool rehash_flag;
            int rehash_index;

            // ready to rehash
            void StartRehash()
            {
                new_length_ = length_ * 2;
                new_list_ = new Node *[new_length_];
                rehash_index = 0;
                for (int i = 0; i < new_length_; ++i)
                    new_list_[i] = nullptr;

                rehash_flag = true;
            }

            void StepRehash()
            {
                if (!rehash_flag)
                    return;

                Node *current = list_[rehash_index];
                while (current != nullptr)
                {
                    Node *next = current->next_hash;
                    int index = hasher(current->key()) % new_length_;
                    current->next_hash = new_list_[index];
                    new_list_[index] = current;
                    current = next;
                }
                list_[rehash_index] = nullptr;
                ++rehash_index;

                if (rehash_index == length_)
                {
                    delete[] list_;
                    list_ = new_list_;
                    length_ = new_length_;
                    rehash_flag = false;
                    new_list_ = nullptr;
                }
            }

        public:
            HashTable() : list_(new Node *[length_]), elems_(0), rehash_index(0), new_list_(nullptr), rehash_flag(false)
            {
                for (int i = 0; i < length_; ++i)
                    list_[i] = nullptr;
            };

            // return nullptr if key is not find in table
            Node *Find(const K &key)
            {
                return *Seek(key);
            };
            void Insert(Node *x)
            {
                StepRehash();
                Node **ptr = Seek(x->key());
                Node *old = *ptr;

                x->next_hash = ((old == nullptr) ? nullptr : old->next_hash);
                *ptr = x;

                if (old == nullptr)
                    ++elems_;

                if (static_cast<double>(elems_) / length_ > load_factor_threshold && !rehash_flag)
                    StartRehash();
            };
            Node *Remove(const K &key)
            {
                StepRehash();
                Node **ptr = Seek(key);
                Node *result = *ptr;
                if (result != nullptr)
                {
                    *ptr = result->next_hash;
                    --elems_;
                }
                return result;
            };

        private:
            Node **Seek(const K &key)
            {

                int index = hasher(key) % length_;

                Node **ptr = &list_[index];

                while (*ptr != nullptr && (*ptr)->key() != key)
                    ptr = &(*ptr)->next_hash;

                if (*ptr == nullptr && rehash_flag)
                {
                    index = hasher(key) % new_length_;
                    ptr = &new_list_[index];
                    while (*ptr != nullptr && (*ptr)->key() != key)
                        ptr = &(*ptr)->next_hash;
                }
                return ptr;
            }
        };

        template <typename K, typename V>
        class LRUCache
        {
            typedef LRUNode<K, V> Node;
            typedef HashTable<K, V> Table;
            typedef std::shared_ptr<KVnode<K, V>> kvnode;

        private:
            Node *st_;
            Node *ed_;
            Table table_;
            const int capacity_;
            int size_;

            void MoveNodeToFront(Node *x);
            inline Node *NewNode(kvnode node) { return new Node(node); }
            void Remove(Node *x);

        public:
            LRUCache(int capacity) : capacity_(capacity), st_(nullptr), ed_(nullptr), table_(), size_(0)
            {
                assert(capacity_ > 0);
            };
            ~LRUCache()
            {
                while (st_ != nullptr)
                {
                    Node *x = st_->next;
                    delete st_;
                    st_ = x;
                }
            }

            bool Insert(const K &key, const V &value);
            void Insert(kvnode node);
            kvnode Get(const K &key);
            bool Contains(const K &key);
            void Remove(const K &key);
        };

        template <typename K, typename V>
        void LRUCache<K, V>::MoveNodeToFront(Node *x)
        {
            if (x == st_)
                return;

            if (x == ed_)
            {
                ed_ = x->prev;
                ed_->next = nullptr;
            }
            else
            {
                x->prev->next = x->next;
                x->next->prev = x->prev;
            }
            x->prev = nullptr;
            x->next = st_;
            st_->prev = x;
            st_ = x;
        }

        template <typename K, typename V>
        void LRUCache<K, V>::Remove(Node *x)
        {
            if (x == nullptr)
                return;

            if (x == st_)
            {
                st_ = x->next;
            }
            else if (x == ed_)
            {
                ed_ = x->prev;
                if (ed_ != nullptr)
                    ed_->next = nullptr;
            }
            else
            {
                x->next->prev = x->prev;
                x->prev->next = x->next;
            }

            delete table_.Remove(x->key());
        }

        // 在Table中调用，缓存内不一定有该key
        template <typename K, typename V>
        void LRUCache<K, V>::Remove(const K &key)
        {
            Node *x = table_.Find(key);
            Remove(x);
        }

        // 只在Table中的insert内调用，判断缓存是否持有kvnode
        template <typename K, typename V>
        bool
        LRUCache<K, V>::Insert(const K &key, const V &value)
        {
            Node *x = table_.Find(key);

            if (x != nullptr)
            {

                if (x->kvnode_.use_count() == 1)
                {
                    // 内存里只有缓存指向该数据，证明该数据已经持久化应该释放
                    Remove(x);
                    return false;
                }
                else
                {
                    // 内存中memtable也持有该数据，直接修改即可
                    x->SetValue(value);
                    MoveNodeToFront(x);
                    return true;
                }
            }
            return false;
        }

        // 只在Table的Get调用，调用时说明第一次在缓存get失败，一定增加新kvnode
        template <typename K, typename V>
        void LRUCache<K, V>::Insert(kvnode node)
        {

            ++size_;
            Node *x = NewNode(node);

            table_.Insert(x);

            if (st_ == nullptr)
            {
                st_ = x;
                ed_ = x;
                st_->next = nullptr;
            }
            else
            {
                x->prev = nullptr;
                x->next = st_;
                st_->prev = x;
                st_ = x;
            }

            if (size_ > capacity_)
            {

                Remove(ed_);
                --size_;
            }
        }

        // key must in cache
        template <typename K, typename V>
        typename LRUCache<K, V>::kvnode LRUCache<K, V>::Get(const K &key)
        {
            Node *x = table_.Find(key);

            if (x == nullptr)
                return nullptr;
            MoveNodeToFront(x);
            return x->kvnode_;
        }

        template <typename K, typename V>
        bool LRUCache<K, V>::Contains(const K &key)
        {
            Node *x = table_.Find(key);

            return x != nullptr;
        }
    }
}

#endif
