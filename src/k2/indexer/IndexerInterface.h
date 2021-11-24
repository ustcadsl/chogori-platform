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
    struct NodeVerMetadata{
        bool isHot;
        dto::Timestamp timestamp;
        dto::DataRecord::Status status;
        bool isTombstone;
        uint64_t request_id;
        void print() {
            K2LOG_D(log::indexer, "Node Metadata: [isHot: {}, timestamp: {}, status: {}, isTombstone: {}, request_id: {}", isHot, timestamp, status, isTombstone, request_id);
        }
    };

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

        // Flags:
        //      63: is_writeintent
        //      61, 60, 59: tomestone[0], inmem[0], exist[0].
        //      ...

        inline bool _get_flag_i(int i) {
            return bool(flags & ((uint64_t)1 << i));
        }

        inline bool _set_flag_i(int i) {
            flags = flags | ((uint64_t)1 << i);
            K2ASSERT(log::indexer, _get_flag_i(i), "still 0 after set");
            return _get_flag_i(i);
        }

        inline bool _set_zero_flag_i(int i) {
            flags = flags & (~((uint64_t)1 << i));
            K2ASSERT(log::indexer, !_get_flag_i(i), "still 1 after clear");
            return _get_flag_i(i);
        }

        inline bool is_writeintent() {
            return _get_flag_i(63);
        }

        inline bool set_writeintent() {
            return _set_flag_i(63);
        }
        
        inline bool set_zero_writeintent() {
            return _set_zero_flag_i(63);
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
            std::cout << "KeyValueNode: " << this << " IsWI: " << is_writeintent() << std::endl;
            for (int i = 0; i < 3; ++i) {
                std::cout << i << ": " << key << ", " << valuedata[i].timestamp << ", " << valuedata[i].valuepointer << " is_inmem: "<< is_inmem(i) << std::endl;
            }
        }

        // verMetaData

        NodeVerMetadata getNodeVerMetaData(int order, PBRB *pbrb);

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

        dto::DataRecord *get_datarecord(const dto::Timestamp &timestamp, int &order, PBRB *pbrb);

        int insert_datarecord(dto::DataRecord *datarecord, PBRB *pbrb);

        /*dto::DataRecord *get_dataHotRecord(int order) {
            if (is_inmem(order) == false) return nullptr;
            retrun valuedata[i].valuepointer;
        }*/
        void setColdAddr(int order, dto::DataRecord *coldRecord) {
            valuedata[order].valuepointer = coldRecord;
        }

        int compareTimestamp(int order, dto::Timestamp &timestamp) {
            //K2LOG_I(log::indexer, "watermark:{}, version:{}", timestamp, valuedata[order].timestamp);
            if (timestamp.tEndTSECount() > valuedata[order].timestamp) {
                return -1;
            } else {
                return 1;
            }
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

        dto::DataRecord *_getColdVerPtr(int order, PBRB *pbrb);

        int remove_datarecord(dto::DataRecord *datarecord, PBRB *pbrb);

        int remove_datarecord(int order, PBRB *pbrb);

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
