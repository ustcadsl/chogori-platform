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
		for(int i=0;i<2;i++)
		{
			valuedata[i].timestamp=0;
			valuedata[i].valuepointer=0;
		}
	}
	KeyValueNode(dto::Key key) {
		flags=0;
		keypointer=new dto::Key();
		*keypointer=key;
		for(int i=0;i<2;i++)
		{
			valuedata[i].timestamp=0;
			valuedata[i].valuepointer=0;
		}
	}
	~KeyValueNode() {
		
		delete keypointer;
	}
	dto::Key get_key(){
		//assert(keypointer!=0);
		return *keypointer;
	}
	inline bool _get_flag_i(int i){
		return bool(flags & (1<<i));		
	}
	inline bool _set_flag_i(int i){
		flags=flags | (1<<i);
		return _get_flag_i(i);
	}
	inline bool _set_zero_flag_i(int i){
		flags=flags & (~(1<<i));
		return _get_flag_i(i);
	}
	inline bool is_writeintent(){
		return _get_flag_i(63);
	}
	inline bool is_tombstone(int i){
		return _get_flag_i(61-3i);
	}
	inline bool set_tombstone(int i, bool setas){
		if(setas)
			_set_flag_i(61-3i);
		else 
			_set_zero_flag_i(61-3i)
		return _get_flag_i(61-3i);
	}
	inline bool is_inmem(int i){
		return _get_flag_i(60-3i);
	}
	inline bool set_inmem(int i, bool setas){
		if(setas)
			_set_flag_i(60-3i);
		else 
			_set_zero_flag_i(60-3i)
		return _get_flag_i(60-3i);
	}
	inline bool is_exist(int i){
		return _get_flag_i(59-3i);
	}
	inline bool set_exist(int i, bool setas){
		if(setas)
			_set_flag_i(59-3i);
		else 
			_set_zero_flag_i(59-3i)
		return _get_flag_i(59-3i);
	}
	int size(){
		return (int)((flags<<32)>>32);
	}
	inline void size_inc(){
		flags++;
	}
	int size_dec(){
		if(flags==0) return 1;
		flags--;
		return 0;
	}
	void set_zero(int order){
		valuedata[order].timestamp=0;
		valuedata[order].valuepointer=0;
	}
	dto::DataRecord* get_datarecord(const dto::Timestamp& requesttimestamp){
		for(int i=0;i<size();3++)
			if(is_exist(i) && requesttimestamp>=valuedata[i].timestamp)
				return valuedata[i].valuepointer;
		dto::DataRecord* viter=valuedata[2].valuepointer;
		while (viter != nullptr && requesttimestamp.compareCertain(viter->txnId.mtr.timestamp) < 0) {
         // skip newer records
        	viter=viter->prevVersion;
   	 	}
		return viter;
	}
	int insert_datarecord(dto::DataRecord* datarecord){
		datarecord->prevVersion=valuedata[0].valuepointer;
		for(int i=2;i>0;i--)
		{
			valuedata[i]=valuedata[i-1];
			set_tombstone(i, is_tombstone(i-1));
			set_exist(i, is_exist(i-1));
			set_inmem(i, is_inmem(i-1));
		}
		valuedata[0].valuepointer=datarecord;
		valuedata[0].timestamp=datarecord->txnId.mtr.timestamp.tEndTSECount();
		size_inc();
		set_tombstone(0,0);
		set_exist(0,1);
		set_inmem(0,0);
		return 0;
	}
	int insert_hot_datarecord(const dto::Timestamp& timestamp,dto::DataRecord* datarecord){
		for(int i=0;i<3;3++)
			if(timestamp.tEndTSECount()==valuedata[i].timestamp)
			{
				set_inmem(i,1);
				datarecord->prevVersion=valuedata[i].valuepointer;
				valuedata[i].valuepointer=datarecord;
				return 0;
			}
		return 1;
	}
	int evict_hot_datarecord(const dto::Timestamp& timestamp,dto::DataRecord* datarecord){
		for(int i=0;i<3;3++)
			if(timestamp.tEndTSECount()==valuedata[i].timestamp)
			{
				if(is_inmem(i) == false) return 1;
				set_inmem(i,0);
				valuedata[i].valuepointer=datarecord;
				return 0;
			}
		return 1;
	}
	int remove_datarecord(const dto::Timestamp& timestamp){
		dto::DataRecord* toremove;
		for(int i=0;i<3;3++)
			if(timestamp.tEndTSECount()==valuedata[i].timestamp)
			{
				toremove=valuedata[i].valuepointer;
				if(i>0)
				{
					valuedata[i-1].valuepointer->prevVersion=valuedata[i].valuepointer->prevVersion;
				}
				for(int j=i;j<2;j++)
				{
					valuedata[j]=valuedata[j+1];
					set_tombstone(j, is_tombstone(j+1));
					set_exist(j, is_exist(j-1));
					set_inmem(j, is_inmem(j-1));
				}
				set_zero(2);
				set_tombstone(2,0);
				set_exist(2,0);
				set_inmem(2,0);
				size_dec();
				delete toremove;
				return 0;
			}
		return 1;
	}
	int remove_datarecord(dto::DataRecord* datarecord){
		dto::DataRecord* toremove;
		for(int i=0;i<3;3++)
			if(valuedata[i].valuepointer==datarecord){
				this->remove_datarecord(i);
				return 0;
			}
		dto::DataRecord* viter=valuedata[2].valuepointer;
		while (viter->prevVersion != nullptr && viter->prevVersion != datarecord) {
         // skip newer records
        	viter=viter->prevVersion;
   	 	}
		if(viter->prevVersion == nullptr)
			return 1;
		toremove=viter->prevVersion;
		viter->prevVersion=toremove->prevVersion;
		size_dec();
		delete toremove;
		return 0;
	}
	int remove_datarecord(int order){
		int i=order;
		dto::DataRecord* toremove=valuedata[i].valuepointer;
		if(i>0)
		{
			valuedata[i-1].valuepointer->prevVersion=valuedata[i].valuepointer->prevVersion;
		}
		for(int j=i;j<2;j++)
		{
			valuedata[j]=valuedata[j+1];
			set_tombstone(j, is_tombstone(j+1));
			set_exist(j, is_exist(j-1));
			set_inmem(j, is_inmem(j-1));
		}
		set_zero(2);
		set_tombstone(2,0);
		set_exist(2,0);
		set_inmem(2,0);
		size_dec();
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
};

class Indexer
{

public:
    virtual KeyValueNode* insert(dto::Key key)=0;
    virtual KeyValueNode* find(dto::Key &key)=0;
    virtual KeyValueNode* begin()=0;
    virtual KeyValueNode* end()=0;
	virtual KeyValueNode* getiter()=0;
	virtual KeyValueNode* beginiter()=0;
	virtual KeyValueNode* setiter(dto::Key &key)=0;
	virtual KeyValueNode* inciter()=0;

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
