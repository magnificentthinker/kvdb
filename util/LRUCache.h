#ifndef STORAGE_KVDB_UTIL_LRUCACHE_H_
#define STORAGE_KVDB_UTIL_LRUCACHE_H_
#include <functional>

namespace kvdb
{
    namespace cache
    {
        template <typename K, typename V>
        struct LRUNode
        {
            K key_;
            V value_;

            LRUNode(K key, V value) : key_(key), value_(value)
            {
            }

            LRUNode *next_hash;
            LRUNode *next;
            LRUNode *prev;
        };

        // 不能insert同一个node - 是否用shared_ptr维护一下node？
        template <typename K, typename V>
        class HashTable
        {
            typedef LRUNode<K, V> Node;

        private:
            int length_ = 16;
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
                    int index = hasher(current->key_) % new_length_;
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
            Node *Find(K key) const
            {
                return *Seek(key);
            };
            void Insert(Node *x)
            {

                Node **ptr = Seek(x->key_);
                Node *old = *ptr;

                x->next_hash = ((old == nullptr) ? nullptr : old->next_hash);
                *ptr = x;

                if (old == nullptr)
                    ++elems_;

                if (static_cast<double>(elems_) / length_ > load_factor_threshold)
                    StartRehash();
                delete old;
            };
            void Remove(K key)
            {
                Node **ptr = Seek(key);
                Node *result = *ptr;
                if (result != nullptr)
                {
                    *ptr = result->next_hash;
                    --elems_;
                }
                delete result;
            };

        private:
            Node **Seek(K key) const
            {
                int index = hasher(key) % length_;

                Node **ptr = &list_[index];

                while (*ptr != nullptr && (*ptr)->key_ != key)
                    ptr = &(*ptr)->next_hash;

                if (*ptr == nullptr && rehash_flag)
                {
                    index = hasher(key) % new_length_;
                    ptr = &new_list_[index];
                    while (*ptr != nullptr && (*ptr)->key_ != key)
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

        private:
            Node *st_;
            Node *ed_;
            Table table_;
            const int capacity_;
            int size_;
            Node *NewNode(const K &key, const V &value);
            void MoveNodeToFront(Node *x);

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

            void Insert(const K &key, const V &value);
            V &Get(const K &key);
            bool Contains(const K &key);
        };

        template <typename K, typename V>
        LRUNode<K, V> *LRUCache<K, V>::NewNode(const K &key, const V &value)
        {
            return new LRUNode<K, V>(key, value);
        }

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
        void LRUCache<K, V>::Insert(const K &key, const V &value)
        {
            Node *x = table_.Find(key);

            if (x != nullptr)
            {
                x->value_ = value;
                MoveNodeToFront(x);
                return;
            }

            ++size_;
            x = NewNode(key, value);

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

                Node *tmp = ed_;
                ed_ = ed_->prev;
                if (ed_ != nullptr)
                    ed_->next = nullptr;
                table_.Remove(tmp->key_);
            }
        }

        // key must in cache
        template <typename K, typename V>
        V &LRUCache<K, V>::Get(const K &key)
        {
            Node *x = table_.Find(key);

            assert(x != nullptr);
            MoveNodeToFront(x);
            return x->value_;
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
