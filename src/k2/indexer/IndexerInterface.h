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
#include <map>
#include <deque>
#include <utility>

#include <k2/common/Common.h>
#include <k2/dto/Collection.h>
#include <k2/dto/K23SI.h>
namespace k2
{
//
//  K2 internal MVCC representation
//

class KeyValueNode
{
private:
	uint64_t flags;
	dto::DataRecord* keypointer;
	typedef struct{
		dto::Timestamp timestamp;
		dto::DataRecord* valuepointer;
		} _valuedata;
	_valuedata valuedata[3];//8type for ts and 8 byte for pointer

public:
	KeyValueNode() {
		flags=0;
		keypointer=0;
		for(int i=0;i<3;i++)
		{
			valuedata[i].timestamp = dto::Timestamp();
			valuedata[i].valuepointer = nullptr;
		}
	}
	dto::Key get_key(){
		//assert(keypointer!=0);
		return valuedata[0].valuepointer->key;
	}
	void set_zero(int order){
		valuedata[order].timestamp = dto::Timestamp();
		valuedata[order].valuepointer = nullptr;
	}
	dto::DataRecord* get_datarecord(const dto::Timestamp& timestamp){
		for(int i=0;i<3;++i)
			if(timestamp.compareCertain(valuedata[i].timestamp)>=0)
				return valuedata[i].valuepointer;
		dto::DataRecord* viter=valuedata[2].valuepointer;
		while (viter != nullptr && timestamp.compareCertain(viter->txnId.mtr.timestamp) < 0) {
         // skip newer records
        	viter = viter->prevVersion;
   	 	}
		return viter;
	}
	int insert_datarecord(dto::DataRecord* datarecord){
		datarecord->prevVersion = valuedata[0].valuepointer;
		for(int i=2;i>0;i--)
			valuedata[i] = valuedata[i-1];
		valuedata[0].valuepointer = datarecord;
		valuedata[0].timestamp = datarecord->txnId.mtr.timestamp;
		return 0;
	}
	int remove_datarecord(const dto::Timestamp& timestamp){
		dto::DataRecord* toremove;
		for(int i=0;i<3;++i)
			if(timestamp.compareCertain(valuedata[i].timestamp)==0)
			{
				toremove=valuedata[i].valuepointer;
				if(i>0)
				{
					valuedata[i-1].valuepointer->prevVersion=valuedata[i].valuepointer->prevVersion;
				}
				for(int j=i;j<2;++j)
					valuedata[j] = valuedata[j+1];
				set_zero(2);
				delete toremove;
				return 0;
			}
		return 1;
	}

	int remove_datarecord(dto::DataRecord* datarecord){
	    if(datarecord == nullptr) {
	        return 1;
	    }

		dto::DataRecord* toremove;
		for(int i=0;i<3;++i)
			if(valuedata[i].valuepointer==datarecord){
				this->remove_datarecord(i);
				return 0;
			}
		dto::DataRecord* viter=valuedata[2].valuepointer;
		while (viter->prevVersion != nullptr && viter->prevVersion != datarecord) {
         // skip newer records
        	viter = viter->prevVersion;
   	 	}
		if(viter->prevVersion == nullptr)
			return 1;
		toremove=viter->prevVersion;
		viter->prevVersion=toremove->prevVersion;
		delete toremove;
		return 0;
	}

	int remove_datarecord(int order){
	    if(order >= 3 || valuedata[order].valuepointer == nullptr) {
	        return 1;
	    }
		int i=order;
		dto::DataRecord* toremove=valuedata[i].valuepointer;
		if(i>0)
		{
			valuedata[i-1].valuepointer->prevVersion=valuedata[i].valuepointer->prevVersion;
		}
		for(int j=i;j<2;++j)
			valuedata[j]=valuedata[j+1];
		set_zero(2);
		delete toremove;
		return 0;
	}
	dto::DataRecord* _getpointer(int order){
		return valuedata[order].valuepointer;
	}
	dto::DataRecord* begin()
	{
		return this->_getpointer(0);
	}
	int size(){
		int i = 0;
		while(i < 3 && valuedata[i].valuepointer != nullptr) {
            i++;
        }
		if(i > 0) {
            dto::DataRecord* viter  = valuedata[i-1].valuepointer;
            while(viter->prevVersion != nullptr) {
                //skip newer records
                viter = viter->prevVersion;
                i++;
            }
		}
		return i;
	}
	bool empty() {
	    return valuedata[0].valuepointer == nullptr;
	}
};

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
}
