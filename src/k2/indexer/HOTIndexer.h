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

#include "IndexerInterface.h"

#include <k2/common/Common.h>
#include <hot/singlethreaded/HOTSingleThreaded.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>

using namespace std;

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
    typedef hot::singlethreaded::HOTSingleThreadedIterator<k2::KeyValueNode*> Iterator;
    
    class HOTindexer{
    private:
        HotIndexer idx;
        Iterator scanit;
    public:
        KeyValueNode* insert(dto::Key key);
        KeyValueNode* find(dto::Key &key);
        Iterator begin();
        Iterator end();
        Iterator getiter();
        Iterator setiter(const dto::Key &key);
        Iterator lower_bound(const dto::Key& key);
        Iterator last();
        // Iterator beginiter();
        // Iterator inciter();

        void erase(dto::Key key);
        size_t size();
    };

    inline KeyValueNode* HOTindexer::insert(dto::Key key)
    {
        KeyValueNode* newkvnode = new KeyValueNode(key);
        bool ret = idx.insert(newkvnode);
        if (ret) {
            return newkvnode;
        }
        return nullptr;
    }
    inline KeyValueNode* HOTindexer::find(dto::Key& key)
    {
        auto kit=idx.lookup(key);
        if (!kit.mIsValid) {
            return nullptr;
        }
        return kit.mValue;
    }
    inline Iterator HOTindexer::begin()
    {
        return idx.begin();
    }
    inline Iterator HOTindexer::end()
    {
        return idx.end();
    }
    inline Iterator HOTindexer::getiter()
    {
        return scanit;
    }
    inline Iterator HOTindexer::setiter(const dto::Key &key)
    {
        // Return wrong result when there is only one leaf node
        return idx.find(key);
    }
    inline Iterator HOTindexer::lower_bound(const dto::Key& key)
    {
        return idx.lower_bound(key);
    }
    inline Iterator HOTindexer::last()
    {
        Iterator tmp=idx.begin();
        while(tmp != idx.end()) {
            Iterator it(tmp);
            ++tmp;
            if(tmp == idx.end()) {
                K2LOG_D(log::indexer, "Last Key {}", it!=idx.end() ? (*it)->get_key() : dto::Key());
                return it;
            }
        }
        return idx.end();
    }
    // inline KeyValueNode* HOTindexer::beginiter()
    // {
    //     scanit==idx.begin();
    //     if(scanit==idx.end()) return nullptr;
    //     return *scanit;
    // }
    // inline KeyValueNode* HOTindexer::inciter()
    // {
    //     ++scanit;
    //     if(scanit==idx.end()) return nullptr;
    //     return *scanit;
    // }
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
}