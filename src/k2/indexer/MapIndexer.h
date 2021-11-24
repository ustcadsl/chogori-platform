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

namespace k2
{
    typedef std::map<dto::Key, k2::KeyValueNode*> MapIndexer;
    typedef std::map<dto::Key, k2::KeyValueNode*>::iterator MapIterator;

    class mapindexer{
    private:
        MapIndexer idx;
        MapIterator scanit;
    public:
        KeyValueNode* insert(dto::Key key);
        MapIterator find(dto::Key &key);
        MapIterator begin();
        MapIterator end();
        MapIterator getiter();
        MapIterator setiter(dto::Key &key);
        MapIterator lower_bound(const dto::Key &start);
        MapIterator last();

        void erase(dto::Key key);
        size_t size();
        KeyValueNode* extractFromIter(MapIterator const&iterator);
    };

    inline KeyValueNode* mapindexer::insert(dto::Key key)
    {
        // TODO handle insert fail
        KeyValueNode* newkvnode = new KeyValueNode(key);
        auto ret = idx.insert(std::pair<dto::Key , k2::KeyValueNode*>(key, newkvnode));
        return ret.first->second;
    }
    inline MapIterator mapindexer::find(dto::Key& key)
    {
        return idx.find(key);
    }
    inline MapIterator mapindexer::begin()
    {
        return idx.begin();
    }
    inline MapIterator mapindexer::end()
    {
        return idx.end();
    }
    inline MapIterator mapindexer::getiter()
    {
        return scanit;
    }
    inline MapIterator mapindexer::setiter(dto::Key &key)
    {
        return idx.find(key);
    }
    inline MapIterator mapindexer::last()
    {
        //TODO check 
        if(idx.empty()) {
            return idx.end();
        }
        auto rit = idx.rbegin();
        ++rit;
        return rit.base();
    }
    inline MapIterator mapindexer::lower_bound(const dto::Key &start)
    {
        return idx.lower_bound(start);
    }

    inline void mapindexer::erase(dto::Key key)
    {
        auto kit = idx.find(key);
        if (kit == idx.end()) return;
        idx.erase(kit);
    }

    inline size_t mapindexer::size()
    {
        return idx.size();
    }

    inline KeyValueNode* mapindexer::extractFromIter(MapIterator const& iterator)
    {
        return iterator->second;
    }
/*
template <typename ValueType>
class Indexer<std::map<dto::Key, k2::KeyValueNode>, ValueType>
{
private:
    std::map<dto::Key, k2::KeyValueNode> idx;
public:
    typename std::map<dto::Key, k2::KeyValueNode>::iterator insert(dto::Key key);
    typename std::map<dto::Key, k2::KeyValueNode>::iterator find(dto::Key &key);
    typename std::map<dto::Key, k2::KeyValueNode>::iterator begin();
    typename std::map<dto::Key, k2::KeyValueNode>::iterator end();
    void erase(typename std::map<dto::Key, k2::KeyValueNode>::iterator it);
    size_t size();
};
template <typename ValueType>
inline typename std::map<dto::Key, k2::KeyValueNode>::iterator Indexer<std::map<dto::Key, k2::KeyValueNode>, ValueType>::insert(dto::Key key) {
    auto ret = idx.insert(std::pair<dto::Key , k2::KeyValueNode>(key, k2::KeyValueNode(key)));
    return ret.first;
}
template <typename ValueType>
inline typename std::map<dto::Key, k2::KeyValueNode>::iterator Indexer<std::map<dto::Key, k2::KeyValueNode>, ValueType>::find(dto::Key &key) {
    return idx.find(key);
}
template <typename ValueType>
inline void Indexer<std::map<dto::Key, k2::KeyValueNode>, ValueType>::erase(typename std::map<dto::Key, k2::KeyValueNode>::iterator it) {
    idx.erase(it);
}
template <typename ValueType>
inline typename std::map<dto::Key, k2::KeyValueNode>::iterator Indexer<std::map<dto::Key, k2::KeyValueNode>, ValueType>::begin() {
    return idx.begin();
}
template <typename ValueType>
inline typename std::map<dto::Key, k2::KeyValueNode>::iterator Indexer<std::map<dto::Key, k2::KeyValueNode>, ValueType>::end() {
    return idx.end();
}
template <typename ValueType>
inline size_t Indexer<std::map<dto::Key, k2::KeyValueNode>, ValueType>::size() {
    return idx.size();
}*/

}