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
/*
namespace k2
{

    template<typename ValueType>
    struct KeyValueNodeKeyExtractor {
        inline size_t getKeyLength(ValueType const &value) const {
            dto::Key tempkey=value->get_key();
            return tempkey.rangeKey.size()+tempkey.partitionKey.size();
        }
        inline const char* operator()(ValueType const &value) const {
            dto::Key tempkey=value->get_key();
            String s=tempkey.partitionKey+tempkey.rangeKey;
            return s.c_str();
        }
    };

    class HOTindexer : public Indexer{
    private:
        hot::singlethreaded::HOTSingleThreaded<k2::KeyValueNode*, KeyValueNodeKeyExtractor> idx;
        hot::singlethreaded::HOTSingleThreadedIterator<k2::KeyValueNode*> scanit;
    public:
        KeyValueNode* insert(dto::Key key);
        KeyValueNode* find(dto::Key &key);
        KeyValueNode* begin();
        KeyValueNode* end();
        KeyValueNode* getiter();
        KeyValueNode* setiter(dto::Key &key);
        KeyValueNode* beginiter();
        KeyValueNode* inciter();

        void erase(dto::Key key);
        size_t size();
    };

    inline KeyValueNode* HOTindexer::insert(dto::Key key)
    {
        KeyValueNode* newkvnode = new KeyValueNode(key);
        bool ret = idx.insert(newkvnode);
        if (ret) return newkvnode;
        return nullptr;
    }
    inline KeyValueNode* HOTindexer::find(dto::Key& key)
    {
        String s=key.partitionKey+key.rangeKey;
        auto kit=idx.lookup(s.c_str());
        if (!kit.mIsValid) {
            return nullptr;
        }
        return kit.mValue;
    }
    inline KeyValueNode* HOTindexer::begin()
    {
        auto kit=idx.begin();
        return *kit;
    }
    inline KeyValueNode* HOTindexer::end()
    {
        return nullptr;
    }
    inline KeyValueNode* HOTindexer::getiter()
    {
        if(scanit==idx.end()) return nullptr;
        return *scanit;
    }
    inline KeyValueNode* HOTindexer::setiter(dto::Key &key)
    {
        String s=key.partitionKey+key.rangeKey;
        scanit=idx.find(s.c_str());
        if (scanit==idx.end()) return nullptr;
        return *scanit;
    }
    inline KeyValueNode* HOTindexer::beginiter()
    {
        scanit==idx.begin();
        if(scanit==idx.end()) return nullptr;
        return *scanit;
    }
    inline KeyValueNode* HOTindexer::inciter()
    {
        scanit++;
        if(scanit==idx.end()) return nullptr;
        return *scanit;
    }
    inline void HOTindexer::erase(dto::Key key)
    {
        String s=key.partitionKey+key.rangeKey;
        auto kit=idx.lookup(s.c_str());
        if (!kit.mIsValid) {
            return;
        }
        KeyValueNode* tempkvn=kit.mValue;
        delete tempkvn;
        idx.remove(s.c_str());
    }

    inline size_t HOTindexer::size()
    {
        return idx.getStatistics().second["numberValues"];
    }
}
*/