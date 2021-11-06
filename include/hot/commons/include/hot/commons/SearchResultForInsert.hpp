#ifndef __HOT__COMMONS__SEARCH_RESULT_FOR_INSERT___
#define __HOT__COMMONS__SEARCH_RESULT_FOR_INSERT___

#include <cstdint>
#include <log.h>

namespace hot { namespace commons {

/**
 * A Helper Function for storing additional result information:
 * 	- the index of the return entry
 * 	- the most significant bit index of the containing node
 */
struct SearchResultForInsert {
	uint32_t mEntryIndex;
	uint16_t mMostSignificantBitIndex;

	inline SearchResultForInsert(uint32_t entryIndex, uint16_t mostSignificantBitIndex)
		: mEntryIndex(entryIndex), mMostSignificantBitIndex(mostSignificantBitIndex) {
	}

	inline SearchResultForInsert() {
	}

	inline void init(uint32_t entryIndex, uint16_t mostSignificantBitIndex) {
		K2LOG_D(k2::log::hot,"EntryIndex={} Most Significant Bit Index={}", entryIndex, mostSignificantBitIndex);
		mEntryIndex = entryIndex;
		mMostSignificantBitIndex = mostSignificantBitIndex;
	}
};

}}

#endif