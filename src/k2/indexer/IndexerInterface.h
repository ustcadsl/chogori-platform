/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once

#include "log.h"

#include <map>
#include <deque>
#include <utility>

#include <k2/common/Common.h>
#include <k2/dto/Collection.h>
#include <k2/dto/K23SI.h>


namespace k2 {
//
//  K2 internal MVCC representation
//

    class KeyValueNode {
    private:
        uint64_t flags;
        dto::Key key;
        typedef struct {
            u_int64_t timestamp;
            dto::DataRecord *valuepointer;
        } _valuedata;
        _valuedata valuedata[3];//8type for ts and 8 byte for pointer

    public:
        KeyValueNode() {
            flags = 0;
            for (int i = 0; i < 3; ++i) {
                valuedata[i].timestamp = 0;
                valuedata[i].valuepointer = nullptr;
            }
        }

        KeyValueNode(dto::Key newKey) {
            flags = 0;
            key.schemaName = newKey.schemaName;
            key.partitionKey = newKey.partitionKey;
            key.rangeKey = newKey.rangeKey;
            for (int i = 0; i < 3; ++i) {
                valuedata[i].timestamp = 0;
                valuedata[i].valuepointer = nullptr;
            }
        }

        ~KeyValueNode() {
        }

        dto::Key get_key() {
            return key;
        }

        inline bool _get_flag_i(int i) {
            return bool(flags & (1 << i));
        }

        inline bool _set_flag_i(int i) {
            flags = flags | (1 << i);
            return _get_flag_i(i);
        }

        inline bool _set_zero_flag_i(int i) {
            flags = flags & (~(1 << i));
            return _get_flag_i(i);
        }

        inline bool is_writeintent() {
            return _get_flag_i(63);
        }

        inline bool is_tombstone(int i) {
            return _get_flag_i(61 - 3*i);
        }

        inline bool set_tombstone(int i, bool setas) {
            if (setas)
                _set_flag_i(61 - 3*i);
            else
                _set_zero_flag_i(61 - 3*i);
            return _get_flag_i(61 - 3*i);
        }

        inline bool is_inmem(int i) {
            return _get_flag_i(60 - 3*i);
        }

        inline bool set_inmem(int i, bool setas) {
            if (setas)
                _set_flag_i(60 - 3*i);
            else
                _set_zero_flag_i(60 - 3*i);
            return _get_flag_i(60 - 3*i);
        }

        inline bool is_exist(int i) {
            return _get_flag_i(59 - 3*i);
        }

        inline bool set_exist(int i, bool setas) {
            if (setas)
                _set_flag_i(59 - 3*i);
            else
                _set_zero_flag_i(59 - 3*i);
            return _get_flag_i(59 - 3*i);
        }

        int size() {
            return (int) ((flags << 32) >> 32);
        }

        inline void size_inc() {
            flags++;
        }

        int size_dec() {
            int tmp=(int)((flags << 32) >> 32);
            if (tmp<=0) 
                return 1;
            flags--;
            return 0;
        }

        void set_zero(int order) {
            valuedata[order].timestamp = 0;
            valuedata[order].valuepointer = nullptr;
        }

        // debugging
        void printAll() {
            for (int i = 0; i < 3; ++i) {
                std::cout << i << ": " << key << ", " << valuedata[i].timestamp << ", " << valuedata[i].valuepointer << std::endl;
            }
        }

        dto::DataRecord *get_datarecord(const dto::Timestamp &timestamp) {
            for (int i = 0; i < 3; ++i)
                if (timestamp.tEndTSECount() >= valuedata[i].timestamp)
                    return valuedata[i].valuepointer;
            dto::DataRecord *viter = valuedata[2].valuepointer;
            while (viter != nullptr && timestamp.compareCertain(viter->timestamp) < 0) {
                // skip newer records
                viter = viter->prevVersion;
            }
            return viter;
        }

        dto::DataRecord *get_datarecord(const dto::Timestamp &timestamp, int &order) {
            std::cout << "@get_datarecord: " << timestamp << ", "<< order;
            for (int i = 0; i < 3; ++i)
                if (timestamp.tEndTSECount() >= valuedata[i].timestamp) {
                    std::cout << key << ", " << valuedata[i].timestamp << ", " << valuedata[i].valuepointer << std::endl;
                    order = i;
                    return valuedata[i].valuepointer;
                }
            order = -1;
            dto::DataRecord *viter = valuedata[2].valuepointer;

            // Get pointer of cold version out of indexer.
            if (is_inmem(2)) {}
                // viter = static_cast<dto::DataRecord *>(pbrb->getPlogAddrRow(viter));
            else
                viter = viter->prevVersion;
            
            while (viter != nullptr && timestamp.compareCertain(viter->timestamp) < 0) {
                // skip newer records
                viter = viter->prevVersion;
            }
            return viter;
        }

        int insert_datarecord(dto::DataRecord *datarecord) {
            if(size() > 0) {
                if(valuedata[0].valuepointer->status == dto::DataRecord::Committed) {
                    datarecord->prevVersion = valuedata[0].valuepointer;
                    for (int i = 2; i > 0; i--) {
                        valuedata[i] = valuedata[i - 1];
                        set_tombstone(i, is_tombstone(i - 1));
                        set_exist(i, is_exist(i - 1));
                        set_inmem(i, is_inmem(i - 1));
                    }
                }
                else {
                    size_dec();
                    datarecord->prevVersion = valuedata[1].valuepointer;   
                }
            }
			         
            valuedata[0].valuepointer = datarecord;
            valuedata[0].timestamp = datarecord->timestamp.tEndTSECount();
            size_inc();
            set_tombstone(0, datarecord->isTombstone);
            set_exist(0, 1);
            set_inmem(0, 0);
            // printAll();
            return 0;
        }

        int insert_hot_datarecord(const dto::Timestamp &timestamp, dto::DataRecord *datarecord) {
            for (int i = 0; i < 3; ++i)
                if (timestamp.tEndTSECount() == valuedata[i].timestamp) {
                    set_inmem(i, 1);
                    // Set prevVer operation done in 
                    // datarecord->prevVersion = valuedata[i].valuepointer;
                    valuedata[i].valuepointer = datarecord;
                    return 0;
                }
            return 1;
        }

        int evict_hot_datarecord(const dto::Timestamp &timestamp, dto::DataRecord *datarecord) {
            for (int i = 0; i < 3; ++i)
                if (timestamp.tEndTSECount() == valuedata[i].timestamp) {
                    if (is_inmem(i) == false) return 1;
                    set_inmem(i, 0);
                    valuedata[i].valuepointer = datarecord;
                    return 0;
                }
            return 1;
        }

        int remove_datarecord(const dto::Timestamp &timestamp) {
            dto::DataRecord *toremove;
            for (int i = 0; i < 3; ++i) {
                if (timestamp.tEndTSECount() == valuedata[i].timestamp) {
                    toremove = valuedata[i].valuepointer;
                    if (i > 0) {
                        valuedata[i - 1].valuepointer->prevVersion = valuedata[i].valuepointer->prevVersion;
                    }
                    for (int j = i; j < 2; ++j) {
                        valuedata[j] = valuedata[j + 1];
                        set_tombstone(j, is_tombstone(j + 1));
                        set_exist(j, is_exist(j + 1));
                        set_inmem(j, is_inmem(j + 1));
                    }
                    size_dec();
                    if (valuedata[2].valuepointer != nullptr) {
                        if(valuedata[2].valuepointer->prevVersion != nullptr){
                            valuedata[2].valuepointer = valuedata[2].valuepointer->prevVersion;
                            set_tombstone(2, valuedata[2].valuepointer->isTombstone);
                            set_inmem(2, 0);
                        }
                        else {
                            set_zero(2);
                            set_tombstone(2, 0);
                            set_exist(2, 0);
                            set_inmem(2, 0);
                        }
                    }
                    delete toremove;
                    return 0;
                }
            }
            return 1;
        }

        int remove_datarecord(dto::DataRecord *datarecord) {
            if (datarecord == nullptr) {
                return 1;
            }

            dto::DataRecord *toremove;
            for (int i = 0; i < 3; ++i) {
                if (valuedata[i].valuepointer == nullptr) {
                    return 1;
                }
                if (valuedata[i].valuepointer == datarecord) {
                    return this->remove_datarecord(i);
                }
            }

            dto::DataRecord *viter = valuedata[2].valuepointer;
            while (viter->prevVersion != nullptr && viter->prevVersion != datarecord) {
                // skip newer records
                viter = viter->prevVersion;
            }
            if (viter->prevVersion == nullptr) {
                return 1;
            }
            toremove = viter->prevVersion;
            viter->prevVersion = toremove->prevVersion;
            size_dec();
            delete toremove;
            return 0;
        }

        int remove_datarecord(int order) {
            if (order >= 3 || valuedata[order].valuepointer == nullptr) {
                return 1;
            }
            dto::DataRecord *toremove = valuedata[order].valuepointer;
            if (order > 0) {
                valuedata[order - 1].valuepointer->prevVersion = valuedata[order].valuepointer->prevVersion;
            }
            for (int j = order; j < 2; ++j) {
                valuedata[j] = valuedata[j + 1];
                set_tombstone(j, is_tombstone(j + 1));
                set_exist(j, is_exist(j + 1));
                set_inmem(j, is_inmem(j + 1));
            }
            if (size_dec()==1) 
                K2LOG_D(log::indexer, "try to remove with no versions, Key: {} {} {}", key.schemaName, key.partitionKey, key.rangeKey);
            if (valuedata[2].valuepointer != nullptr) {
                if(valuedata[2].valuepointer->prevVersion != nullptr){
                    valuedata[2].valuepointer = valuedata[2].valuepointer->prevVersion;
                    set_tombstone(2, valuedata[2].valuepointer->isTombstone);
                    set_inmem(2, 0);
                }
                else {
                    set_zero(2);
                    set_tombstone(2, 0);
                    set_exist(2, 0);
                    set_inmem(2, 0);
                }
            }
            delete toremove;
            return 0;
        }

        dto::DataRecord *_getpointer(int order) {
            return valuedata[order].valuepointer;
        }

        dto::DataRecord *begin() {
            return this->_getpointer(0);
        }
    };

    class Indexer
    {

    public:
        virtual KeyValueNode *insert(dto::Key key) = 0;

        virtual KeyValueNode *find(dto::Key &key) = 0;

        virtual KeyValueNode *begin() = 0;

        virtual KeyValueNode *end() = 0;

        virtual KeyValueNode *getiter() = 0;

        virtual KeyValueNode *beginiter() = 0;

        virtual KeyValueNode *setiter(dto::Key &key) = 0;

        virtual KeyValueNode *inciter() = 0;

        virtual void erase(dto::Key key)=0;

        virtual size_t size()=0;
    };

/*
template<typename IndexerType, typename ValueType>
class Indexer
{
private:
  IndexerType idx;

public:
    typename IndexerType::iterator insert(dto::Key key);
    typename IndexerType::iterator find(dto::Key &key);
    typename IndexerType::iterator begin();
    typename IndexerType::iterator end();

    void erase(typename IndexerType::iterator it);
    size_t size();
};

template<typename IndexerType, typename ValueType>
inline typename IndexerType::iterator Indexer<IndexerType, ValueType>::insert(dto::Key key) {
    auto ret = idx.insert(std::pair<dto::Key, k2::KeyValueNode>(key, k2::KeyValueNode()));
    return ret.first;
}

template<typename IndexerType, typename ValueType>
inline typename IndexerType::iterator Indexer<IndexerType, ValueType>::find(dto::Key &key) {
    return idx.find(key);
}

template<typename IndexerType, typename ValueType>
inline typename IndexerType::iterator Indexer<IndexerType, ValueType>::begin() {
    return idx.begin();
}

template<typename IndexerType, typename ValueType>
inline typename IndexerType::iterator Indexer<IndexerType, ValueType>::end() {
    return idx.end();
}

template<typename IndexerType, typename ValueType>
inline void Indexer<IndexerType, ValueType>::erase(typename IndexerType::iterator it) {
    idx.erase(it);
}

template<typename IndexerType, typename ValueType>
inline size_t Indexer<IndexerType, ValueType>::size() {
    return idx.size();
}
*/



}
