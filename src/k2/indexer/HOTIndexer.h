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
    inline String convertNull(String& str) {
        for(size_t i=0; i<str.size(); ++i) {
            if(str[i]==0) {
                str[i] = '0';
            }
        }
        return str;
    }
    template<typename ValueType>
    struct KeyValueNodeKeyExtractor {
        inline size_t getKeyLength(ValueType const &value) const {
            dto::Key tempkey=value->get_key();
            return tempkey.schemaName.size() + tempkey.rangeKey.size()+tempkey.partitionKey.size();
        }
        inline const char* operator()(ValueType const &value) const {
            dto::Key tempkey=value->get_key();
            String s=tempkey.schemaName+tempkey.partitionKey+tempkey.rangeKey;
            s = convertNull(s);
            char *s_ptr = (char *)malloc(s.length());
            strcpy(s_ptr,s.c_str());
            return s_ptr;
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
        // auto it = lower_bound(key);
        // K2ASSERT(log::indexer, it!=idx.end() && *it==newkvnode, "Lower bound key={} Test failed for key {}'s lowerBound", 
        //             it!=idx.end() ? (*it)->get_key():dto::Key(), key);
        if (ret) {
            String s=key.schemaName+key.partitionKey+key.rangeKey;
            K2LOG_D(log::indexer, "Insert key {} String {} Ptr {}", key, convertNull(s).c_str(), (void*)newkvnode);
            return newkvnode;
        }
        return nullptr;
    }
    inline KeyValueNode* HOTindexer::find(dto::Key& key)
    {
        String s=key.schemaName+key.partitionKey+key.rangeKey;
        auto kit=idx.lookup(convertNull(s).c_str());
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
        String s=key.schemaName+key.partitionKey+key.rangeKey;
        return idx.find(convertNull(s).c_str());
    }
    inline Iterator HOTindexer::lower_bound(const dto::Key& key)
    {
        String s=key.schemaName+key.partitionKey+key.rangeKey;
        return idx.lower_bound(convertNull(s).c_str());
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
        String s=key.schemaName+key.partitionKey+key.rangeKey;
        auto kit=idx.lookup(convertNull(s).c_str());
        if (!kit.mIsValid) {
            return;
        }
        KeyValueNode* tempkvn=kit.mValue;
        delete tempkvn;
        idx.remove(convertNull(s).c_str());
    }

    inline size_t HOTindexer::size()
    {
        return idx.getStatistics().second["numberValues"];
    }
}