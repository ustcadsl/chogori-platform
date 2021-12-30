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
#include "IndexerInterface.h"

#include <vector>
#include <chrono>
#include <k2/common/Common.h>
#include <hot/singlethreaded/HOTSingleThreaded.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>

namespace k2
{
    template<typename ValueType>
    struct KeyValueNodeKeyExtractor {
        inline size_t getKeyLength(ValueType const &value) const {
            dto::Key tempkey=value->get_key();
            return tempkey.schemaName.size() + tempkey.rangeKey.size()+tempkey.partitionKey.size();
        }
        inline dto::Key operator()(ValueType const &value) const {
            return value->get_key();
        }
    };
    typedef hot::singlethreaded::HOTSingleThreaded<k2::KeyValueNode*, KeyValueNodeKeyExtractor> HotIndexer;
    typedef hot::singlethreaded::HOTSingleThreadedIterator<k2::KeyValueNode*> HotIterator;
    
    class HOTindexer{
    private:
        HotIndexer idx;

    public:
        ~HOTindexer();
        HotIterator find(dto::Key &key);
        HotIterator lower_bound(const dto::Key& key);

        HotIterator begin();
        HotIterator end();
        HotIterator last();

        KeyValueNode* extractFromIter(HotIterator const& iterator);

        HotIterator setiter(const dto::Key &key);

        KeyValueNode* insert(dto::Key key);
        void erase(dto::Key key);

        size_t size();        
    };
    inline HOTindexer::~HOTindexer() {
        for(auto it = begin(); it!=end(); ++it) {
            delete *it;
        }
    }

    inline KeyValueNode* HOTindexer::insert(dto::Key key)
    {
        KeyValueNode* newKVNode = new KeyValueNode(key);
        bool ret = false;
        K2LOG_D(log::indexer, "Insert key {} and new KVNode @{}", key, (void*)(newKVNode));
        ret = idx.insert(newKVNode);
        if (ret) {
            return newKVNode;
        }
        else {
            delete newKVNode;
            K2LOG_W(log::indexer, "Insert new KVNode failed");
            return nullptr;
        }
    }
    inline HotIterator HOTindexer::find(dto::Key& key)
    {
        return idx.find(key);
    }
    inline HotIterator HOTindexer::begin()
    {
        return idx.begin();
    }
    inline HotIterator HOTindexer::end()
    {
        return idx.end();
    }
    inline HotIterator HOTindexer::setiter(const dto::Key &key)
    {
        // Return wrong result when there is only one leaf node
        return idx.find(key);
    }
    inline HotIterator HOTindexer::lower_bound(const dto::Key& key)
    {
        return idx.lower_bound(key);
    }
    inline HotIterator HOTindexer::last()
    {
        return idx.last();
    }
    inline void HOTindexer::erase(dto::Key key)
    {
        auto kit=idx.lookup(key);
        if (!kit.mIsValid) {
            return;
        }
        KeyValueNode* tempkvn=kit.mValue;
        idx.remove(key);
        delete tempkvn;       
    }

    inline size_t HOTindexer::size()
    {
        return idx.size();
    }

    inline KeyValueNode* HOTindexer::extractFromIter(HotIterator const& iterator)
    {
        return *iterator;
    }
}