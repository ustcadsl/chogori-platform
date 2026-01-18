#ifndef __HOT__SINGLE_THREADED__MEMORY_POOL__
#define __HOT__SINGLE_THREADED__MEMORY_POOL__

#include <algorithm>
#include <array>
#include <cassert>
#include <jemalloc/jemalloc.h>

namespace hot { namespace singlethreaded {

class FreeListEntry;

class FreeListEntry {
private:
	FreeListEntry* mNext;
	size_t mListSize;

public:
	FreeListEntry() : mNext(nullptr), mListSize(0) {

	}

	FreeListEntry(FreeListEntry* next) : mNext(next), mListSize(next->mListSize + 1) {
	}

	FreeListEntry* getNext() {
		return mNext;
	}

	size_t getListSize() {
		return mListSize;
	}
};

template<typename ElementType, size_t NUMBER_LISTS, size_t EVICTION_BEGIN_SIZE = 200, size_t EVICTION_END_SIZE = EVICTION_BEGIN_SIZE/2> class MemoryPool;

template<typename ElementType, size_t NUMBER_LISTS, size_t EVICTION_BEGIN_SIZE, size_t EVICTION_END_SIZE> class MemoryPool {
	static constexpr size_t SIZE_BEFORE_EVICTION_BEGIN_SIZE = EVICTION_BEGIN_SIZE - 1u;
	static constexpr size_t ELEMENT_SIZE = sizeof(ElementType);
	static FreeListEntry TERMINATING_ENTRY;

	std::array<FreeListEntry*, NUMBER_LISTS> mFreeLists;
	size_t mNumberAllocations;

	size_t mNumberFrees;
	void* staticswap;

public:
  size_t mNumberNodeRequest;
public:
	MemoryPool() : mNumberAllocations(0ul), mNumberNodeRequest(0ul), mNumberFrees(0ul) {
		std::fill(mFreeLists.begin(), mFreeLists.end(), &TERMINATING_ENTRY);
		int error = posix_memalign(&staticswap, 256, 1024);
		if(error != 0) {
			//"Got error on alignment"
			throw std::bad_alloc();
		}
		//std::cout<<"alloc static swap area, addr= "<<staticswap<<std::endl;
	}

	MemoryPool(MemoryPool<ElementType, NUMBER_LISTS> const & other) = delete;
	MemoryPool& operator=(MemoryPool<ElementType, NUMBER_LISTS> const & other) = delete;

	~MemoryPool() {
		free(staticswap);
		for(FreeListEntry** freeList = mFreeLists.begin(); freeList != mFreeLists.end(); ++freeList) {
			while((*freeList)->getListSize() > 0) {
				*freeList = freeEntry(*freeList);
			}
		}
	}
  void* allocstatic()
	{
		return staticswap;
	}
 
	void* alloc(size_t numberElements) {
		FreeListEntry* & head = getFreeListHead(numberElements);
++mNumberNodeRequest;
		void* rawMemory;
		if(head->getListSize() == 0) {
			++mNumberAllocations;
      //std::cout<<"alloc size= "<<numberElements* sizeof(ElementType)<<"  elesize= "<<sizeof(ElementType)<<std::endl;
			int error = posix_memalign(&rawMemory, sizeof(ElementType), numberElements * sizeof(ElementType));
      //std::cout<<"alloc res= "<<error<<std::endl;
			if(error != 0) {
				//"Got error on alignment"
				throw std::bad_alloc();
			}
		} else {
			rawMemory = reinterpret_cast<void*>(head);
			head = head->getNext();
		}

		return rawMemory;
	}

	void returnToPool(size_t numberElements, void* rawMemory) {
		FreeListEntry* & head = getFreeListHead(numberElements);
		if(head->getListSize() < SIZE_BEFORE_EVICTION_BEGIN_SIZE) {
			head = new (rawMemory) FreeListEntry(head);
		} else {
			free(rawMemory);
			++mNumberFrees;
			while (head->getListSize() > EVICTION_END_SIZE) {
				head = freeEntry(head);
			}
		}
	}

	size_t getNumberAllocations() const {
		return mNumberAllocations;
	}

size_t getNumberNode() const {
		return mNumberNodeRequest;
	}
	size_t getNumberFrees() const {
		return mNumberFrees;
	}

private:
	FreeListEntry* freeEntry(FreeListEntry* head) {
		assert(head->getListSize() != 0u);
		FreeListEntry* next = head->getNext();
		free(head);
		++mNumberFrees;
		return next;
	}

	FreeListEntry* & getFreeListHead(size_t numberElements) {
		return mFreeLists[numberElementsToFreeListId(numberElements)];
	}

	size_t numberElementsToFreeListId(size_t numberElements) {
		assert(numberElements > 0);
		assert(numberElements <= NUMBER_LISTS);

		return numberElements - 1;
	}
};

template<typename ElementType, size_t NUMBER_LISTS, size_t EVICTION_BEGIN_SIZE, size_t EVICTION_END_SIZE>
	FreeListEntry MemoryPool<ElementType, NUMBER_LISTS, EVICTION_BEGIN_SIZE, EVICTION_END_SIZE>::TERMINATING_ENTRY {};

}}

#endif