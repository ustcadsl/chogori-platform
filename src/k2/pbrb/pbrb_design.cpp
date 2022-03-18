#include <iostream>
#include "pbrb_design.h"
#include "indexer.h"

namespace k2
{

namespace pbrb {
    bool isValid(uint32_t testVal) {
        return !(testVal & errMask);
    }
}
// Copy the header of row from DataRecord of query to (pagePtr, rowOffset)
void *PBRB::cacheRowHeaderFrom(uint32_t schemaId, BufferPage *pagePtr, RowOffset rowOffset, dto::DataRecord* rec) {
    //K2LOG_I(log::pbrb, "function cacheRowFromPlog(BufferPage, RowOffset:{}, PlogAddr", rowOffset);
    if (pagePtr == nullptr) {
        K2LOG_I(log::pbrb, "Trying to cache row to nullptr!");
        return nullptr;
    }
    //if (rowOffset > _schemaMap[getSchemaIDPage(pagePtr)].maxRowCnt) {
    if (rowOffset > _schemaMap[schemaId].maxRowCnt) {
        K2LOG_I(log::pbrb, "Row Offset out of range!");
        return nullptr;
    }
    if (isBitmapSet(pagePtr, rowOffset)) {
        K2LOG_I(log::pbrb, "Conflict: move row to occupied slot, In page, offset:{}", rowOffset);
        return nullptr;
    }

    //SchemaId schemaId = getSchemaIDPage(pagePtr);
    SchemaMetaData smd = _schemaMap[schemaId];
    uint32_t byteOffsetInPage = _pageHeaderSize + smd.occuBitmapSize + 
                            smd.rowSize * rowOffset;

    void *rowBasePtr = (uint8_t *)(pagePtr) + byteOffsetInPage;
    K2LOG_D(log::pbrb, "^^^^^^schemaId:{}, RowOffset:{}, occuBitmapSize:{}, rowSize:{}, byteOffsetInPage:{}, rowBasePtr:{}", schemaId, rowOffset, smd.occuBitmapSize, smd.rowSize, byteOffsetInPage, rowBasePtr);

    // Copy timestamp
    // uint32_t tsoId = timestamp.tsoId();
    // void *tsPtr = (void *) ((uint8_t *) rowBasePtr + 4);
    // memcpy(tsPtr, &tsoId, sizeof(uint32_t));

    setTimestampRow(rowBasePtr, rec->timestamp);
    setPlogAddrRow(rowBasePtr, rec);
    setStatusRow(rowBasePtr, rec->status);
    setIsTombstoneRow(rowBasePtr, rec->isTombstone);
    setRequestIdRow(rowBasePtr, rec->request_id);

    return rowBasePtr;
}

// Copy the field of row from DataRecord of query to (pagePtr, rowOffset)
void *PBRB::cacheRowFieldFromDataRecord(uint32_t schemaId, BufferPage *pagePtr, RowOffset rowOffset, void* valueAddr, size_t strSize, uint32_t fieldID, bool isStr)
{
    //K2LOG_I(log::pbrb, "function cacheRowFromPlog(BufferPage, RowOffset:{}, field:{}", rowOffset, field);
    if (pagePtr == nullptr) {
        K2LOG_E(log::pbrb, "Trying to cache row to nullptr!");
        return nullptr;
    }
    //if (rowOffset > _schemaMap[getSchemaIDPage(pagePtr)].maxRowCnt) {
    if (rowOffset > _schemaMap[schemaId].maxRowCnt) {
        K2LOG_E(log::pbrb, "Row Offset out of range!");
        return nullptr;
    }
    if (isBitmapSet(pagePtr, rowOffset)) {
        K2LOG_E(log::pbrb, "Conflict: move row to occupied slot, In page, offset:{}", rowOffset);
        return nullptr;
    }

    //SchemaId schemaId = getSchemaIDPage(pagePtr);
    SchemaMetaData smd = _schemaMap[schemaId];
    uint32_t byteOffsetInPage = _pageHeaderSize + smd.occuBitmapSize + 
                            smd.rowSize * rowOffset;

    void *rowBasePtr = (uint8_t *)(pagePtr) + byteOffsetInPage;

    // Copy a field to PBRB row
    void *destPtr = (void *) ((uint8_t *) rowBasePtr + smd.fieldsInfo[fieldID].fieldOffset);
    
    // TYPE: STRING
    if (isStr) {
        size_t copySize = strSize;
        k2::String *strPtr = static_cast<k2::String *>(valueAddr);
        //const size_t maxStrSize = FTSize[static_cast<int>(k2::dto::FieldType::STRING)] - 1;
        size_t maxStrSize = smd.fieldsInfo[fieldID].fieldSize;

        memcpy(destPtr, &strSize, sizeof(size_t)); //////
        destPtr = (void *) ((uint8_t *) rowBasePtr + smd.fieldsInfo[fieldID].fieldOffset + sizeof(size_t)); //////
        if (strSize + sizeof(size_t) > maxStrSize) {
            //K2LOG_W(log::pbrb, "Trying to insert field strSize(: {}) > maxStrSize(: {}), Cut the string",
            //    strSize, maxStrSize);
            size_t segmentSize = maxStrSize - sizeof(size_t) - sizeof(void *);
            //k2::String *buf = new k2::String(strPtr->substr(0, segmentSize));
            //memcpy(destPtr, buf->c_str(), segmentSize);
            memcpy(destPtr, valueAddr, segmentSize);
            //delete buf;

            ///////////////////////////////
            destPtr = (void *) ((uint8_t *) rowBasePtr + smd.fieldsInfo[fieldID].fieldOffset + sizeof(size_t) + segmentSize); //////
            //std::byte *fieldValueSegment = new std::byte[strSize-segmentSize]; //////
            uint8_t *fieldValueSegment = static_cast<uint8_t *>(operator new((strSize-segmentSize) * sizeof(std::byte)));
            memcpy(fieldValueSegment, strPtr->substr(segmentSize, strSize).c_str(), strSize-segmentSize); //////
            memcpy(destPtr, &fieldValueSegment, sizeof(void *));

            // void *retVal = *((void **)((uint8_t *)destPtr));
            // K2LOG_I(log::pbrb, "^^^strSize:{}, fieldValueSegment:{}, destPtr:{}", strSize, (void *)(uint8_t *)fieldValueSegment, retVal);
            //delete (uint8_t *)retVal;
            //////////////////////////////
            copySize = maxStrSize;
        } else {
            //memcpy(destPtr, strPtr->c_str(), strSize + 1);
            memcpy(destPtr, valueAddr, strSize);
        }
        /*else if (strSize > 0) {
            //memcpy(destPtr, strPtr->c_str(), strSize + 1);
            memcpy(destPtr, strPtr->c_str(), strSize);
        }
        else {
            *(uint8_t *)destPtr = '\0';
            K2LOG_D(log::pbrb, "StrSize == 0, Wrote \\0");
            return rowBasePtr;
        }*/
        K2LOG_D(log::pbrb, "fieldID:{}, fieldOffset:{}, destPtr:{}, Copied k2::String: {}", fieldID, smd.fieldsInfo[fieldID].fieldOffset, destPtr, strPtr->c_str());
        K2LOG_D(log::pbrb, "Copied {} byte(s) to {}", copySize + 1, destPtr);
        //K2LOG_I(log::pbrb, "strPtr size: {},  strSize + 1: {}", strPtr->size(), strSize + 1);
        // printFieldsRow(pagePtr, rowOffset);
        return rowBasePtr;
    }

    // TYPE: OTHERS
    size_t copySize = smd.fieldsInfo[fieldID].fieldSize;
    K2LOG_D(log::pbrb, "fieldID:{}, fieldOffset:{}, destPtr:{}, copySize:{}, strSize:{}", fieldID, smd.fieldsInfo[fieldID].fieldOffset, destPtr, copySize, strSize);
    // printFieldsRow(pagePtr, rowOffset);
    // + 4 to move to the real address in simple plog
    memcpy(destPtr, valueAddr, copySize);

    return rowBasePtr;
}


// Copy the payload of row from DataRecord of query to (pagePtr, rowOffset)
void *PBRB::cacheRowPayloadFromDataRecord(uint32_t schemaId, BufferPage *pagePtr, RowOffset rowOffset, Payload& rowData)
{
    //K2LOG_I(log::pbrb, "function cacheRowFromPlog(BufferPage, RowOffset:{}", rowOffset);
    if (pagePtr == nullptr) {
        K2LOG_I(log::pbrb, "Trying to cache row to nullptr!");
        return nullptr;
    }
    //if (rowOffset > _schemaMap[getSchemaIDPage(pagePtr)].maxRowCnt) {
    if (rowOffset > _schemaMap[schemaId].maxRowCnt) {
        K2LOG_I(log::pbrb, "Row Offset out of range!");
        return nullptr;
    }
    if (isBitmapSet(pagePtr, rowOffset)) {
        K2LOG_I(log::pbrb, "Conflict: move row to occupied slot, In page, offset:{}", rowOffset);
        return nullptr;
    }
    //SchemaId schemaId = getSchemaIDPage(pagePtr);
    SchemaMetaData smd = _schemaMap[schemaId];
    uint32_t byteOffsetInPage = _pageHeaderSize + smd.occuBitmapSize + 
                            smd.rowSize * rowOffset;

    void *rowBasePtr = (void *) ((uint8_t *)(pagePtr) + byteOffsetInPage + smd.fieldsInfo[0].fieldOffset);
    size_t rowPayloadSize = rowData.getSize();
    rowData.seek(0);
    memcpy(rowBasePtr, &rowPayloadSize, sizeof(size_t));
    rowBasePtr = (void *) ((uint8_t *)(pagePtr) + byteOffsetInPage + smd.fieldsInfo[0].fieldOffset + sizeof(size_t));
    //K2LOG_I(log::pbrb, "^^^rowPayloadSize:{}, smd.rowSize - smd.fieldsInfo[0].fieldOffset:{}", rowPayloadSize, smd.rowSize - smd.fieldsInfo[0].fieldOffset);
    if ((rowPayloadSize + sizeof(size_t)) > (smd.rowSize - smd.fieldsInfo[0].fieldOffset)) { 
    //TODO: long payload exceeds row size
        size_t inPlaceSize = smd.rowSize - smd.fieldsInfo[0].fieldOffset - sizeof(size_t) - sizeof(void *);
        rowData.read(rowBasePtr, inPlaceSize); ///
        size_t outPlaceSize = rowPayloadSize - inPlaceSize;
        uint8_t *outPlaceValue = static_cast<uint8_t *>(operator new((outPlaceSize) * sizeof(std::byte)));
        rowData.read(outPlaceValue, outPlaceSize); ///
        void *destPtr = (void *) ((uint8_t *)(pagePtr) + byteOffsetInPage + smd.rowSize - sizeof(void *) );
        memcpy(destPtr, &outPlaceValue, sizeof(void *));
        void *retVal = *((void **)((uint8_t *)destPtr));
        K2LOG_I(log::pbrb, "^^^rowPayloadSize:{}, outPlaceValue:{}, retVal:{}", rowPayloadSize, (void *)(uint8_t *)outPlaceValue, retVal);
        //K2LOG_I(log::pbrb, "********cut long payload row, rowData.getSize:{}, row size:{}, rowPayloadSize:{}", rowData.getSize(), smd.rowSize, rowPayloadSize);
    } else {
        rowData.read(rowBasePtr, rowPayloadSize); ///
        //K2LOG_I(log::pbrb, "********rowBasePtr:{}, row size:{}, smd.rowSize:{}, rowPayloadSize:{}", (void *)(uint8_t *)rowBasePtr, rowData.getSize(), smd.rowSize, rowPayloadSize);
    }
    return rowBasePtr;
}

// Copy memory from plog to (pagePtr, rowOffset)
void *PBRB::cacheRowFromPlog(BufferPage *pagePtr, RowOffset rowOffset, PLogAddr pAddress)
{
    std::cout << "\nfunction cacheRowFromPlog(BufferPage * " << pagePtr 
              << ", RowOffset " << rowOffset << ", PLogAddr " << pAddress << "):\n" << std::endl;

    if (pagePtr == nullptr) {
        std::cout << "Trying to cache row to nullptr!" << std::endl;
        return nullptr;
    }
    if (rowOffset > _schemaMap[getSchemaIDPage(pagePtr)].maxRowCnt) {
        std::cout << "Row Offset out of range" << std::endl;
        return nullptr;
    }
    if (isBitmapSet(pagePtr, rowOffset)) {
        std::cout << "Conflict: move row to occupied slot" << std::endl
                  << "\t In page: " << pagePtr << std::endl
                  << "offset: " << rowOffset << std::endl;
        return nullptr;
    }

    SchemaId schemaId = getSchemaIDPage(pagePtr);
    SchemaMetaData smd = _schemaMap[schemaId];
    uint32_t byteOffsetInPage = _pageHeaderSize + smd.occuBitmapSize + 
                            smd.rowSize * rowOffset;

    void *rowBasePtr = (uint8_t *)(pagePtr) + byteOffsetInPage;

    // Copy timestamp
    void *tsPtr = (void *) ((uint8_t *) rowBasePtr + 4);
    memcpy(tsPtr, (uint8_t *)pAddress + 4, 4);

    // Copy from plog to PBRB
    void *destPtr = (void *) ((uint8_t *) rowBasePtr + smd.fieldsInfo[0].fieldOffset);
    size_t copySize = smd.rowSize - smd.fieldsInfo[0].fieldOffset;

    // + 4 to move to the real address in simple plog
    memcpy(destPtr, (uint8_t *)pAddress + 8, copySize);
    
    // Set PlogAddre in row.
    void *plogAddr = (void *) ((uint8_t *) rowBasePtr + 20);
    memcpy(plogAddr, &pAddress, 8);
    setRowBitMapPage(pagePtr, rowOffset);

    std::cout << "Cached Plog Row:"
              << "\n\tPlog Address: " << pAddress 
              << "\n\tBufferPage: " << pagePtr 
              << "\n\trowOffset: " << rowOffset
              << "\n\tHot Row Pointer: " << rowBasePtr << std::endl; 

    return rowBasePtr;
}

void *PBRB::cacheColdRow(PLogAddr pAddress)
{
    SchemaId schemaId = readFromPlog<SchemaId>(pAddress, 4);
    if (_schemaMap.find(schemaId) == _schemaMap.end() || _schemaMap[schemaId].headPage == nullptr) {
        if (createCacheForSchema(schemaId) == nullptr)
            return nullptr;
    }
    
    auto retVal = findCacheRowPosition(schemaId);
    BufferPage *pagePtr = retVal.first;
    RowOffset rowOffset = retVal.second;

    return cacheRowFromPlog(pagePtr, rowOffset, pAddress);

}


/* ...New MapIndexer Integrated. Temporally disabled

// cache cold row in pAddress with key, insert hot address into indexer;
// return hot address.
void *PBRB::cacheColdRow(PLogAddr pAddress, String key)
{
    SchemaId schemaId = readFromPlog<SchemaId>(pAddress, 4);
    if (_schemaMap.find(schemaId) == _schemaMap.end() || _schemaMap[schemaId].headPage == nullptr) {
        if (createCacheForSchema(schemaId) == nullptr)
            return nullptr;
    }

    // 1. evict expired versions.
    void *replaceAddr = nullptr;
    KVN &kvNode = (*_indexer)[key];
    // 1. has hot row:
    if (kvNode.hasCachedVer()) {
        for (int i = kvNode.rowNum - 1;i >= 0;i--) {
            if (kvNode.isCached[i] && kvNode.timestamp[i] < (int) watermark.tStartTSECount()) {
            //if (kvNode.isCached[i] && kvNode.timestamp[i] < *watermark) {
                replaceAddr = kvNode.addr[i];
                PLogAddr coldAddr = evictRow(kvNode.addr[i]);
                kvNode.hotToCold(i, coldAddr);
            }
        }
        // three hot rows: replace the last row:
        if (kvNode.isAllCached()) {
            replaceAddr = kvNode.addr[2];
            PLogAddr coldAddr = evictRow(kvNode.addr[2]);
            kvNode.hotToCold(2, coldAddr);
        }
    }

    // found an address to replace.
    if (replaceAddr != nullptr) {
        auto retVal = findRowByAddr(replaceAddr);
        BufferPage *pagePtr = retVal.first;
        RowOffset rowOffset = retVal.second;
        cacheRowFromPlog(pagePtr, rowOffset, pAddress);
        kvNode.insertRow(replaceAddr, getTimestampRow(replaceAddr).tStartTSECount(), true, this);
        return replaceAddr;
    }

    // need to find new place to cache row.
    auto retPair = findCacheRowPosition(schemaId, key);
    BufferPage *pagePtr = retPair.first;
    RowOffset rowOffset = retPair.second;
    
    // split
    std::cout << getHotRowsNumPage(pagePtr) << " " << 0.8 * _schemaMap[schemaId].maxRowCnt << std::endl;
    if (getHotRowsNumPage(pagePtr) > 0.8 * _schemaMap[schemaId].maxRowCnt) {
        splitPage(pagePtr);
        auto retPair = findCacheRowPosition(schemaId, key);
        pagePtr = retPair.first;
        rowOffset = retPair.second;
    }

    void *hotAddr = cacheRowFromPlog(pagePtr, rowOffset, pAddress);
    if (hotAddr != nullptr) {
        kvNode.insertRow(hotAddr, getTimestampRow(hotAddr).tStartTSECount(), true, this);
        return hotAddr;
    }
    return nullptr;
}
*/

//find an empty slot in the page
/*RowOffset PBRB::findEmptySlotInPage(uint32_t schemaID, BufferPage *pagePtr)
{
    //uint32_t schemaId = getSchemaIDPage(pagePtr);
    uint32_t maxRowCnt = _schemaMap[schemaID].maxRowCnt;
    uint8_t globalByte = 0;
    uint8_t curByte = 0;
    for (uint32_t i = 0; i < maxRowCnt; i++)
    {
        uint32_t byteIndex = i / 8;
        if(i % 8 ==0) {
            globalByte = readFromPage<uint8_t>(pagePtr, _pageHeaderSize + byteIndex, 1);
            //K2LOG_I(log::pbrb, "######globalByte:{}, byteIndex:{}", globalByte, byteIndex);
        }
        uint32_t Offset = i % 8;
        curByte = globalByte >> Offset;
        uint8_t bitValue = curByte & (uint8_t) 0x1;
        if (bitValue == 0)
            return i; //find an empty slot, return the offset
    }
    return 0xFFFFFFFF; //not find an empty slot
}*/

/*RowOffset PBRB::findEmptySlotInPage(uint32_t schemaID, BufferPage *pagePtr)
{
    //uint32_t schemaId = getSchemaIDPage(pagePtr);
    const SchemaMetaData& smd = _schemaMap[schemaID];
    uint32_t maxRowCnt = smd.maxRowCnt;
    for (uint32_t i = 0; i < maxRowCnt; i++)
    {
        if(!isBitmapSet(pagePtr, i))
            return i;
    }
    return 0xFFFFFFFF; //not find an empty slot
}*/

//find an empty slot between the beginOffset and endOffset in the page
RowOffset PBRB::findEmptySlotInPage(uint32_t schemaID, BufferPage *pagePtr, RowOffset beginOffset, RowOffset endOffset)
{
    // auto start = Clock::now();
    //uint32_t schemaId = getSchemaIDPage(pagePtr);
    uint32_t maxRowCnt = _schemaMap[schemaID].maxRowCnt;
    int result = -1;
    if (endOffset > maxRowCnt)
        return 0xFFFFFFFF;
    // TODO: Skip full pages.
    for (uint32_t i = beginOffset; i < endOffset; i++)
    {
        uint32_t byteIndex = i / 8;
        uint8_t curByte = readFromPage<uint8_t>(pagePtr, _pageHeaderSize + byteIndex, 1);
        
        uint32_t Offset = i % 8;
        curByte = curByte >> Offset;
        uint8_t bitValue = curByte & (uint8_t) 0x1;
        if (bitValue == 0) {
            result = i;
            break; //find an empty slot, return the offset
        }
    }
    // auto end = Clock::now();
    // auto ns = nsec(end - start).count();
    // auto smd = _schemaMap[schemaID];
    // fcrp.InsertInPage(fcrp.accessId, smd.schema->name, ns, smd.curPageNum, smd.curRowNum, 2);
    return result; //not find an empty slot
}

RowOffset PBRB::findEmptySlotInPage(uint32_t schemaID, BufferPage *pagePtr)
{
	//uint32_t schemaId = getSchemaIDPage(pagePtr);
    // auto start = Clock::now();
	const SchemaMetaData& smd = _schemaMap[schemaID];
    int result = -1;
	uint32_t maxRowCnt = smd.maxRowCnt;
    
    // Mar. 2022: skip the full pages.
    if (getHotRowsNumPage(pagePtr) == smd.maxRowCnt) {
        return result;
    }

    for (uint32_t i = 0; i < smd.occuBitmapSize; i++) {
		uint8_t bitmap = pagePtr->content[_pageHeaderSize + i];
		if (bitmap == 0xFF)
			continue;
		for (uint32_t j = 0; j < 8; j++) {
			if ((bitmap >> j & 0x1) == 0) {
				if (i * 8 + j < maxRowCnt)
					result = i * 8 + j;
				else
					// out of range.
					result = 0xFFFFFFFF;
                // auto end = Clock::now();
                // auto ns = nsec(end - start).count();
                // fcrp.InsertInPage(fcrp.accessId, smd.schema->name, ns, smd.curPageNum, smd.curRowNum, 1);
                return result;
			}
		}
	}
    // auto end = Clock::now();
    // auto ns = nsec(end - start).count();
    // fcrp.InsertInPage(fcrp.accessId, smd.schema->name, ns, smd.curPageNum, smd.curRowNum, 1);
	return result; //not find an empty slot
}

// Find Cache Row Position From pagePtr to end
std::pair<BufferPage *, RowOffset> PBRB::findCacheRowPosition(uint32_t schemaID, BufferPage *pagePtr)
{
    // Validate pagePtr.
    if (pagePtr == nullptr) {
        K2LOG_E(log::pbrb, "find a cache row in nullptr");
        return std::make_pair(nullptr, 0);
    }
    else if (getMagicPage(pagePtr) != 0x1010) {
        K2LOG_E(log::pbrb, "page magic != 0x1010");
        return std::make_pair(nullptr, 0);
    }
    else if (schemaID != getSchemaIDPage(pagePtr))
    {
        K2LOG_E(log::pbrb, "pagePtr schemaID: {} != schemaID {}", getSchemaIDPage(pagePtr), schemaID);
        return std::make_pair(nullptr, 0);
    }

    uint32_t maxTryCnt = 10;
    uint32_t tryNum = 0;

    BufferPage *iterPtr = pagePtr;
    while (iterPtr != nullptr && tryNum++ < maxTryCnt) {
        RowOffset rowOffset = findEmptySlotInPage(schemaID, iterPtr);
        if (rowOffset & 0x80000000)
            // go to next page;
            iterPtr = getNextPage(iterPtr);
        else
            return std::make_pair(iterPtr, rowOffset);
    }
    // allocate a newpage and insert in new page.
    if (iterPtr == nullptr)
        pagePtr = AllocNewPageForSchema(schemaID);
    else {
        pagePtr = AllocNewPageForSchema(schemaID, pagePtr);
    }
    //return std::make_pair(nullptr, 0);
    return std::make_pair(pagePtr, 0);
}

//find an empty slot by querying the page one by one in turn
std::pair<BufferPage *, RowOffset> PBRB::findCacheRowPosition(uint32_t schemaID)
{
    // fcrp.fcrpBase++;
    BufferPage *pagePtr = _schemaMap[schemaID].headPage;
    //K2LOG_D(log::pbrb, "^^^^^^^^findCacheRowPosition, schemaID:{}, pagePtr empty:{}", schemaID, pagePtr==nullptr);
    while (pagePtr != nullptr) {
        RowOffset rowOffset = findEmptySlotInPage(schemaID, pagePtr);
        if (rowOffset & 0x80000000)
            // go to next page;
            pagePtr = getNextPage(pagePtr);
        else
            return std::make_pair(pagePtr, rowOffset);
    }
    //all allocated pages for the schema are full, so allocate a new page
    pagePtr = AllocNewPageForSchema(schemaID);
    //return std::make_pair(nullptr, 0);
    return std::make_pair(pagePtr, 0);
}

//find and empty slot that try to keep rows within/between pages as orderly as possible
std::pair<BufferPage *, RowOffset> PBRB::findCacheRowPosition(uint32_t schemaID, dto::Key key) {
    
    fcrp.accessId++;
    // fcrp.fcrpNew++;
    // K2LOG_I(log::pbrb, "SchemaID: {}", schemaID);
    // _schemaMap[schemaID].printInfo();
    String sname = _schemaMap[schemaID].schema->name;
    // K2LOG_I(log::pbrb, "SchemaName: {}", sname);
    int pageNum = _schemaMap[schemaID].curPageNum;
    int rowNum = _schemaMap[schemaID].curRowNum;
    auto idxStart = Clock::now();
    IndexerIterator iter = _indexer->find(key);
    if (iter == _indexer->end()) {
        K2LOG_E(log::pbrb, "Cannot find kvnode for key: {}", key);
        return std::make_pair(nullptr, 0);
    }
    KeyValueNode &node = *_indexer->extractFromIter(iter);
    auto idxEnd = Clock::now();
    int idxNsecs = nsec(idxEnd - idxStart).count();
    BufferPage *pagePtr;
    RowOffset rowOffset;
    // 1. has hot row:
    int firstHotVer = node.first_cached_ver();
    if (firstHotVer != -1) {
        auto case1Start = Clock::now();
        RowAddr hotAddr = static_cast<RowAddr>(node._getpointer(firstHotVer));
        pagePtr = getPageAddr(hotAddr);
        rowOffset = findEmptySlotInPage(schemaID, pagePtr);
        // TODO: 3 hot version -> evict
        if (pbrb::isValid(rowOffset)) {
            // void *hotAddr = cacheRowFromPlog(pagePtr, rowOffset, pAddr);
            // kvNode.insertHotRow(hotAddr);
            auto p = std::make_pair(pagePtr, rowOffset);
            auto case1End = Clock::now();
            auto case1Ns = nsec(case1End - case1Start).count();
            fcrp.Insert(fcrp.accessId, idxNsecs, case1Ns, 11, sname, 0, 0, pageNum, rowNum);
            return p;
        }
        else {
            // auto ret = findCacheRowPosition(schemaID);
            // optimize: from current page.
            auto ret = findCacheRowPosition(schemaID, pagePtr);
            auto case1End = Clock::now();
            auto case1Ns = nsec(case1End - case1Start).count();
            fcrp.Insert(fcrp.accessId, idxNsecs, case1Ns, 12, sname, 0, 0, pageNum, rowNum);
            return ret;
        }
    }

    // 2. 3 cold versions.
    else {
        // auto case2Start = Clock::now();
        IndexerIterator prevIter = _indexer->find(key);
        IndexerIterator nextIter = prevIter;

        int maxRetryCnt = 5;
        auto case2prev = Clock::now();
        BufferPage *prevPageAddr = nullptr;
        RowOffset prevOff = 0;
        for (int i = 0; i < maxRetryCnt; i++) {
            if (prevIter != _indexer->begin())
            {
                prevIter--;
                KeyValueNode &prevNode = *_indexer->extractFromIter(prevIter); 
                int firstHotVer = prevNode.first_cached_ver();
                if (firstHotVer != -1) {
                    RowAddr prevAddr = static_cast<RowAddr>(prevNode._getpointer(firstHotVer));
                    auto retVal = findRowByAddr(prevAddr);
                    prevPageAddr = retVal.first;
                    prevOff = retVal.second;
                    break;
                }
            }
            else break;
        }
        auto case2Next = Clock::now();
        BufferPage *nextPageAddr = nullptr;
        RowOffset nextOff = 0;
        for (int i = 0; i < maxRetryCnt && nextIter != _indexer->end(); i++) {
            nextIter++;
            if (nextIter == _indexer->end())
                break;
            KeyValueNode &nextNode = *_indexer->extractFromIter(nextIter); 
            int firstHotVer = nextNode.first_cached_ver();
            if (firstHotVer != -1) {
                RowAddr nextAddr = static_cast<RowAddr>(nextNode._getpointer(firstHotVer));
                auto retVal = findRowByAddr(nextAddr);
                nextPageAddr = retVal.first;
                nextOff = retVal.second;
                break;
            }
        }
        auto case2PointerEnd = Clock::now();
        auto prevTime = nsec(case2Next - case2prev).count();
        auto nextTime = nsec(case2PointerEnd - case2Next).count();
        std::pair<BufferPage *, RowOffset> ret;
        // cannot find neighboring pages...
        if (prevPageAddr == nullptr && nextPageAddr == nullptr) {
            ret = findCacheRowPosition(schemaID);
            auto case2End = Clock::now();
            auto case2Ns = nsec(case2End - case2PointerEnd).count();
            fcrp.Insert(fcrp.accessId, idxNsecs, case2Ns, 21, sname, prevTime, nextTime, pageNum, rowNum);
            return ret;
        }

        // insert into next page...
        else if (prevPageAddr == nullptr && nextPageAddr != nullptr) {
            RowOffset off = findEmptySlotInPage(schemaID, nextPageAddr);
            
            if (off & 0x80000000) 
                // optimize: from nextPageAddr;
                ret = findCacheRowPosition(schemaID, nextPageAddr);
            else
                ret = std::make_pair(nextPageAddr, off);

            auto case2End = Clock::now();
            auto case2Ns = nsec(case2End - case2PointerEnd).count();
            fcrp.Insert(fcrp.accessId, idxNsecs, case2Ns, 22, sname, prevTime, nextTime, pageNum, rowNum);

            return ret;
        }

        // insert into prev page...
        else if (prevPageAddr != nullptr && nextPageAddr == nullptr) {
            RowOffset off = findEmptySlotInPage(schemaID, prevPageAddr);
            if (off & 0x80000000)
                // optimize: from prevPageAddr;
                ret = findCacheRowPosition(schemaID, prevPageAddr);
            else
                ret = std::make_pair(prevPageAddr, off);

            auto case2End = Clock::now();
            auto case2Ns = nsec(case2End - case2PointerEnd).count();
            fcrp.Insert(fcrp.accessId, idxNsecs, case2Ns, 23, sname, prevTime, nextTime, pageNum, rowNum);
            return ret;
        }

        // insert into page with lower occupancy rate...
        else {
            if (prevPageAddr == nextPageAddr) {
                RowOffset off = findEmptySlotInPage(schemaID, prevPageAddr, prevOff + 1, nextOff);
                if (off & 0x80000000)
                    off = findEmptySlotInPage(schemaID, prevPageAddr);
                
                if (off & 0x80000000)
                    ret = findCacheRowPosition(schemaID, prevPageAddr);
                else 
                    ret = std::make_pair(prevPageAddr, off);

                auto case2End = Clock::now();
                auto case2Ns = nsec(case2End - case2PointerEnd).count();
                fcrp.Insert(fcrp.accessId, idxNsecs, case2Ns, 24, sname, prevTime, nextTime, pageNum, rowNum);               
                return ret;
            }

            uint16_t prevHotNum = getHotRowsNumPage(prevPageAddr);
            uint16_t nextHotNum = getHotRowsNumPage(nextPageAddr);
            if (prevHotNum == nextHotNum && prevHotNum == _schemaMap[schemaID].maxRowCnt)
            {
                // case 27:
                ret = findCacheRowPosition(schemaID, prevPageAddr);
                
                auto case2End = Clock::now();
                auto case2Ns = nsec(case2End - case2PointerEnd).count();
                fcrp.Insert(fcrp.accessId, idxNsecs, case2Ns, 27, sname, prevTime, nextTime, pageNum, rowNum);  
                return ret;             
            }
            else if (prevHotNum <= nextHotNum) {
                RowOffset off = findEmptySlotInPage(schemaID, prevPageAddr, prevOff + 1, _schemaMap[schemaID].maxRowCnt);
                if (off & 0x80000000)
                    off = findEmptySlotInPage(schemaID, prevPageAddr);
                if (off & 0x80000000)
                    ret = findCacheRowPosition(schemaID, prevPageAddr);
                else
                    ret = std::make_pair(prevPageAddr, off);
                
                auto case2End = Clock::now();
                auto case2Ns = nsec(case2End - case2PointerEnd).count();
                fcrp.Insert(fcrp.accessId, idxNsecs, case2Ns, 25, sname, prevTime, nextTime, pageNum, rowNum);               
                return ret;

            }
            else {
                RowOffset off = findEmptySlotInPage(schemaID, nextPageAddr, 0, nextOff);
                if (off & 0x80000000)
                    off = findEmptySlotInPage(schemaID, nextPageAddr);
                if (off & 0x80000000)
                    ret = findCacheRowPosition(schemaID, nextPageAddr);
                else 
                    ret = std::make_pair(nextPageAddr, off);
                
                auto case2End = Clock::now();
                auto case2Ns = nsec(case2End - case2PointerEnd).count();
                fcrp.Insert(fcrp.accessId, idxNsecs, case2Ns, 26, sname, prevTime, nextTime, pageNum, rowNum);    
                return ret;
            }
        }
    }

    return std::make_pair(nullptr, 0);
}

float PBRB::getAveragePageListUsage(float& maxPageListUsage) {
    float pageListUsageSum = 0.0;
    int pageListNum = 0;
    float averagePageListUsage = 0.0;
    for (auto &schemaEntry : _schemaMap) {
        uint32_t sID = schemaEntry.first;
        SchemaMetaData sMeta = schemaEntry.second;
        BufferPage *pagePtr = sMeta.headPage;
        long totalHotRowsOfaPageList = 0;
        long totalMaxRow = 0;
        float curPageListUsage = 0.0;
        while (pagePtr) {
            //outputHeader(pagePtr);
            totalHotRowsOfaPageList += getHotRowsNumPage(pagePtr);
            totalMaxRow += sMeta.maxRowCnt;
            pagePtr = getNextPage(pagePtr);
        }     
        curPageListUsage = (float)totalHotRowsOfaPageList/(float)totalMaxRow;
        if(curPageListUsage > maxPageListUsage) maxPageListUsage = curPageListUsage;
        //pageListUsageSum += (float)totalHotRowsOfaPageList/(float)totalMaxRow;
        pageListUsageSum += curPageListUsage;
        K2LOG_I(log::pbrb, "schemaID:{}, totalHotRowsOfaPageList:{}, totalMaxRow:{}, sMeta.maxRowCnt:{}, maxPageListUsage:{}", sID, totalHotRowsOfaPageList, totalMaxRow, sMeta.maxRowCnt, maxPageListUsage);
        pageListNum++;
    }
    averagePageListUsage = pageListUsageSum/(float)pageListNum;
    K2LOG_I(log::pbrb, "####averagePageListUsage:{}, maxPageListUsage:{}", averagePageListUsage, maxPageListUsage);
    return averagePageListUsage;
}

float PBRB::getCurPageListUsage(uint32_t schemaID) {
    float curPageListUsage = 0.0;
    int pageCount = 0;
    long totalHotRowsOfaPageList = 0;
    long totalMaxRow = 0;
    for (auto &schemaEntry : _schemaMap) {
        uint32_t sID = schemaEntry.first;
        if(sID == schemaID){
            SchemaMetaData sMeta = schemaEntry.second;
            BufferPage *pagePtr = sMeta.headPage;
            while (pagePtr) {
                //outputHeader(pagePtr);
                pageCount++;
                totalHotRowsOfaPageList += getHotRowsNumPage(pagePtr);
                totalMaxRow += sMeta.maxRowCnt;
                pagePtr = getNextPage(pagePtr);
            }     
            curPageListUsage = (float)totalHotRowsOfaPageList/(float)totalMaxRow;
            break;       
        }
    }
    K2LOG_I(log::pbrb, "####schemaID:{}, curPageListUsage:{}, pageCount:{}, totalHotRowsOfaPageList:{}, totalMaxRow:{}", schemaID, curPageListUsage, pageCount, totalHotRowsOfaPageList, totalMaxRow);
    return curPageListUsage;
}

/*
void PBRB::doBackgroundPBRBGC(mapindexer& _indexer, dto::Timestamp& newWaterMark, Duration& retentionPeriod) {
    K2LOG_I(log::pbrb, "####in doBackgroundPBRBGC, _freePageList.size:{}", _freePageList.size());
    if ((float)_freePageList.size()/(float)_maxPageNumber >= 0.4) { //0.1
        return;
    }
    if(newWaterMark.compareCertain(watermark) > 0) watermark = newWaterMark;
    float maxPageListUsage = 0.0;
    float avgPageListUsage = getAveragePageListUsage(maxPageListUsage);

    //if (avgPageListUsage <= 0.1 && maxPageListUsage < 0.2) { //0.9, 0.95
    if (avgPageListUsage <= 0.6) {
        return;
    } else {  //if (avgPageListUsage > 0.6 && avgPageListUsage <= 0.8) {
        //float ratio = (float)((avgPageListUsage-0.6)/(1-0.6));
        retentionPeriod/(100);
        //int ratio = (avgPageListUsage-0.9)*100;
        //watermark = watermark + retentionPeriod*ratio/(100-90); //Change the watermark dynamiclly according to retentionTime and avgPageListUsage
        //watermark = watermark + retentionPeriod*ratio/(100); //Change the watermark dynamiclly according to retentionTime and avgPageListUsage
        K2LOG_I(log::pbrb, "set a newer waterMark:{}, original waterMark:{}, avgPageListUsage:{}, _freePageList size:{}", watermark, newWaterMark, avgPageListUsage, _freePageList.size());
    } 

    MapIterator indexIterator = _indexer.begin();
    for (; indexIterator!=_indexer.end(); indexIterator++) {
        KeyValueNode* nodePtr = indexIterator->second;
        //K2LOG_I(log::pbrb, "######in doBackgroundPBRBGC, schemaName:{}, key:{}", nodePtr->get_key().schemaName, nodePtr->get_key());
        for (int i = 0; i < 3; i++) {
            if (!nodePtr->is_inmem(i)) continue;
            //K2LOG_I(log::pbrb, "######watermark:{}", watermark);
            if (nodePtr->compareTimestamp(i, watermark) < 0) {
                //K2LOG_I(log::pbrb, "evict row:{}, order:{}", nodePtr->get_key(), i);
                void* HotRowAddr = static_cast<void *> (nodePtr->_getpointer(i));
                auto pair = findRowByAddr(HotRowAddr);
                BufferPage *pagePtr = pair.first;
                RowOffset rowOff = pair.second;
                dto::DataRecord *coldAddr = static_cast<dto::DataRecord *> (getPlogAddrRow(HotRowAddr));
                nodePtr->setColdAddr(i, coldAddr);
                nodePtr->set_inmem(i, 0);
                //nodePtr->printAll();
                //outputHeader(pagePtr);
                //K2LOG_I(log::pbrb, "before removeHotRow, schemaName:{}, getHotRowsNumPage(pagePtr):{}", nodePtr->get_key().schemaName, getHotRowsNumPage(pagePtr));
                removeHotRow(pagePtr, rowOff);
                //K2LOG_I(log::pbrb, "after removeHotRow getHotRowsNumPage(pagePtr):{}", getHotRowsNumPage(pagePtr));
                //TODO: release heap space
            }
        }
    }
    float avgPageListUsage1 = getAveragePageListUsage(maxPageListUsage);
    K2LOG_I(log::pbrb, "####before GC avgPageListUsage:{}, after GC avgPageListUsage:{}, _freePageList:{}", avgPageListUsage, avgPageListUsage1, _freePageList.size());
    
    //TODO: merge pages that with low usage
}

void PBRB::doBackgroundPageListGC(String schemaName, uint32_t schemaID, mapindexer& _indexer, dto::Timestamp& newWaterMark, Duration& retentionPeriod) {
    K2LOG_I(log::pbrb, "####in doBackgroundPageListGC, _freePageList.size:{}, schemaID:{}, schemaName:{}", _freePageList.size(), schemaID, schemaName);
    //if(newWaterMark.compareCertain(watermark) > 0) watermark = newWaterMark;
    //int ratio = (avgPageListUsage-0.9)*100;
    dto::Timestamp tempWaterMark = newWaterMark;
    retentionPeriod/(100-90);
    //watermark = watermark + retentionPeriod*ratio/(100-90); //Change the watermark dynamiclly according to retentionTime and avgPageListUsage
    //watermark = watermark + retentionPeriod*ratio/(100); //Change the watermark dynamiclly according to retentionTime and avgPageListUsage
    tempWaterMark = tempWaterMark + retentionPeriod*1/4;
    K2LOG_I(log::pbrb, "set a newer tempWaterMark:{}, newWaterMark:{}, retentionPeriod*3/4:{}", tempWaterMark, newWaterMark, retentionPeriod*3/4);
    long totalKeyN = 0;
    MapIterator indexIterator = _indexer.begin();
    //MapIndexer::iterator indexIterator = _indexer.begin();
    for (; indexIterator!=_indexer.end(); indexIterator++) {
        totalKeyN++;
        KeyValueNode* nodePtr = indexIterator->second;
        //K2LOG_I(log::pbrb, "######key schemaName:{}, schemaName:{}", nodePtr->get_key().schemaName, schemaName);
        for (int i = 0; i < 3; i++) {
            if (!nodePtr->is_inmem(i)) continue;
            if(nodePtr->get_key().schemaName != schemaName) continue;
            //K2LOG_I(log::pbrb, "located schemaName:{}", schemaName);
            if (nodePtr->compareTimestamp(i, tempWaterMark) < 0) {
                K2LOG_I(log::pbrb, "evict row:{}, order:{}", nodePtr->get_key(), i);
                void* HotRowAddr = static_cast<void *> (nodePtr->_getpointer(i));
                auto pair = findRowByAddr(HotRowAddr);
                BufferPage *pagePtr = pair.first;
                RowOffset rowOff = pair.second;
                dto::DataRecord *coldAddr = static_cast<dto::DataRecord *> (getPlogAddrRow(HotRowAddr));
                nodePtr->setColdAddr(i, coldAddr);
                nodePtr->set_inmem(i, 0);
                //nodePtr->printAll();
                removeHotRow(pagePtr, rowOff);
                //TODO: release heap space
            }
        }
    }
    float curPageListUsage = getCurPageListUsage(schemaID);
    K2LOG_I(log::pbrb, "####after doBackgroundPageListGC, curPageListUsage:{}, totalKeyN:{}, schemaName:{}, schemaID:{}", curPageListUsage, totalKeyN, schemaName, schemaID);
    //TODO: merge pages that with low usage
}*/

//mark the row as unoccupied when evicting a hot row
void PBRB::removeHotRow(BufferPage *pagePtr, RowOffset offset)
{
    clearRowBitMapPage(pagePtr, offset);
}

void PBRB::releaseHeapSpace(BufferPage *pagePtr, void *rowAddr) {
    SchemaId sid = getSchemaIDPage(pagePtr);
    SchemaMetaData smd = _schemaMap[sid];
    for (uint32_t idx = 0; idx < smd.schema->fields.size(); idx++) {
        dto::SchemaField& field = smd.schema->fields[idx];
        if (field.type == dto::FieldType::STRING) {
            size_t fieldOffset = smd.fieldsInfo[idx].fieldOffset;
            void *srcAddr = (void *)((uint8_t *)rowAddr + fieldOffset);
            size_t maxStrSize = smd.fieldsInfo[idx].fieldSize;
            size_t RowFieldSize; //////
            memcpy(&RowFieldSize, srcAddr, sizeof(size_t)); //////
            if(RowFieldSize + sizeof(size_t) > maxStrSize) {
                void *heapAddr = *((void **)((uint8_t *)rowAddr + fieldOffset + maxStrSize - sizeof(void *)));
                delete (char *)heapAddr;
                K2LOG_I(log::pbrb, "in releaseHeapSpace, RowFieldSize:{}, heapAddr:{}", RowFieldSize, (void *)(uint8_t *)heapAddr);
            }
        }
    }
}

void PBRB::releasePayloadHeapSpace(BufferPage *pagePtr, void *rowAddr) {
    SchemaId sid = getSchemaIDPage(pagePtr);
    SchemaMetaData smd = _schemaMap[sid];
    void* srcAddr = (void*) ((uint8_t *)rowAddr + smd.fieldsInfo[0].fieldOffset); //row header offset
    size_t payloadRowSize;
    memcpy(&payloadRowSize, srcAddr, sizeof(size_t));
    if (payloadRowSize + sizeof(size_t) > smd.rowSize - smd.fieldsInfo[0].fieldOffset) { 
        void *heapAddr = *((void **)((uint8_t *)rowAddr + smd.rowSize - sizeof(void *) )); 
        delete (char *)heapAddr;
        K2LOG_I(log::pbrb, "in releasePayloadHeapSpace, payloadRowSize:{}, heapAddr:{}", payloadRowSize, (void *)(uint8_t *)heapAddr);
    }
}

std::pair<BufferPage *, RowOffset> PBRB::findRowByAddr(void *rowAddr) {
    BufferPage *pagePtr = getPageAddr(rowAddr);
    uint32_t offset = (uint64_t) rowAddr & mask;
    SchemaId sid = getSchemaIDPage(pagePtr);
    SchemaMetaData smd = _schemaMap[sid];
    RowOffset rowOff = (offset - smd.fieldsInfo[0].fieldOffset) / smd.rowSize;
    return std::make_pair(pagePtr, rowOff);
}

PLogAddr PBRB::evictRow(void *rowAddr) {
    auto pair = findRowByAddr(rowAddr);
    BufferPage *pagePtr = pair.first;
    RowOffset rowOff = pair.second;
    std::cout << "\nEvict Row In " << rowAddr 
              << " (Page: " << pagePtr 
              << ", Row Offset: " << rowOff << ")" << std::endl;

    evictCnt++;
    removeHotRow(pagePtr, rowOff);
    PLogAddr pAddr;
    memcpy(&pAddr, (uint8_t *)rowAddr + 20, 8);
    return pAddr;
}

void *PBRB::copyRowInPages(BufferPage *srcPagePtr, RowOffset srcOffset,
                           BufferPage *dstPagePtr, RowOffset dstOffset) {
    SchemaId dstSid = getSchemaIDPage(srcPagePtr);
    SchemaId srcSid = getSchemaIDPage(dstPagePtr);
    assert(dstSid == srcSid);                       // SchemaIds should be same.
    assert(!isBitmapSet(dstPagePtr, dstOffset));    // dst position should be empty.
    void *src = getAddrByPageAndOffset(dstSid, srcPagePtr, srcOffset);
    void *dst = getAddrByPageAndOffset(srcSid, dstPagePtr, dstOffset);

    memcpy(dst, src, _schemaMap[dstSid].rowSize);
    setRowBitMapPage(dstPagePtr, dstOffset);

    return dst;
}

/* New Indexer integrated... Temporally disabled
bool PBRB::splitPage(BufferPage *pagePtr) {
    
    // 1. get metadata; find a free page.
    SchemaId sid = getSchemaIDPage(pagePtr);
    assert(_schemaMap.find(sid) != _schemaMap.end());
    SchemaMetaData smd = _schemaMap[sid];

    BufferPage *newPage = _freePageList.front();
    _freePageList.pop_front();
    initializePage(newPage);
    setSchemaIDPage(newPage, sid);
    
    // 2. Find offsets which need to move
    uint32_t maxRowNum = smd.maxRowCnt;
    uint32_t moveCnt = getHotRowsNumPage(pagePtr) / 2;
    std::vector<RowOffset> offVec(moveCnt);
    int cursor = moveCnt - 1;
    for (int idx = maxRowNum;idx >= 0 && cursor >= 0; idx--) {
        if (isBitmapSet(pagePtr, idx))
            offVec[cursor--] = idx;
    }

    // 3. insert newPage into LinkList.
    BufferPage *nextPage = getNextPage(pagePtr);
    setNextPage(newPage, nextPage);
    setPrevPage(newPage, pagePtr);
    setNextPage(pagePtr, newPage);
    if (nextPage != nullptr) 
        setPrevPage(nextPage, newPage);
    
    splitCnt++;
    
    auto p = _schemaMap[sid].headPage;
    while (p) {
        std::cout << p << std::endl;
        auto q = getNextPage(p);
        if (q == p) break;
        p = q;
    }
    // 4. copy rows in offVec;

    // NOTE: support only field[0] as key and type == INT32_T
    assert(smd.schema->fields[0].type == k2::dto::FieldType::INT32T);

    RowOffset newOff = 0;
    for (RowOffset offset: offVec) {
        // 4.1: got key info from schema
        std::cout << "Move offset: " << offset << std::endl;
        int uid = 0;
        size_t byteOffsetInPage = _pageHeaderSize + smd.occuBitmapSize + smd.rowSize * offset;
        void *oldRowAddr = (void *) ((uint8_t *)pagePtr + byteOffsetInPage);
        readFromPage(pagePtr, byteOffsetInPage + smd.fieldsInfo[0].fieldOffset, smd.fieldsInfo[0].fieldSize, &uid);
        String key = smd.schema->getKey(uid);

        assert(_indexer->find(key) != _indexer->end());

        // 4.2: Copy row 
        // (NOTE: just move to the same offset)
        // void *newRowAddr = copyRowInPages(pagePtr, offset, newPage, offset);

        // move from offset 0.
        void *newRowAddr = copyRowInPages(pagePtr, offset, newPage, newOff++);

        // 4.3: update _indexer.
        KVN &kvNode = (*_indexer)[key];
        uint32_t ts = getTimestampRow(oldRowAddr).tStartTSECount();
        int verIdx = kvNode.findVerByTs(ts);
        
        assert(verIdx != -1 && kvNode.isCached[verIdx] == true);    

        kvNode.addr[verIdx] = newRowAddr;

        // 4.4: remove row in old page.
        assert(isBitmapSet(pagePtr, offset));
        removeHotRow(pagePtr, offset);
    }



    return true;
}

bool PBRB::mergePage(BufferPage *pagePtr1, BufferPage *pagePtr2) {
    
    // 1. Check condition.
    SchemaId sid1 = getSchemaIDPage(pagePtr1);
    SchemaId sid2 = getSchemaIDPage(pagePtr2);
    assert(sid1 == sid2);
    SchemaId sid = sid1;

    SchemaMetaData smd = _schemaMap[sid];
    uint32_t rowNum1 = getHotRowsNumPage(pagePtr1);
    uint32_t rowNum2 = getHotRowsNumPage(pagePtr2);
    assert(rowNum1 + rowNum2 <= smd.maxRowCnt);

    // 2. Find empty slots in pagePtr1
    std::vector<RowOffset> offVec1;
    for (RowOffset i = 0; i < smd.maxRowCnt && offVec1.size() < rowNum2; i++) {
        if (!isBitmapSet(pagePtr1, i))
            offVec1.push_back(i);
    }
    assert(offVec1.size() == rowNum2);

    std::vector<RowOffset> offVec2;
    for (RowOffset i = 0; i < smd.maxRowCnt && offVec2.size() < rowNum2; i++) {
        if (isBitmapSet(pagePtr2, i))
            offVec2.push_back(i);
    }
    assert(offVec2.size() == rowNum2);

    // 3. Copy rows from pagePtr2

    // NOTE: support only field[0] as key and type == INT32_T
    assert(smd.schema->fields[0].type == k2::dto::FieldType::INT32T);

    for (size_t i = 0; i < offVec1.size(); i++) {
        RowOffset offset1 = offVec1[i];
        RowOffset offset2 = offVec2[i];

        // 3.1: got key info from page2
        int uid = 0;
        size_t byteOffsetInPage = _pageHeaderSize + smd.occuBitmapSize + smd.rowSize * offset2;
        void *oldRowAddr = (void *) ((uint8_t *)pagePtr2 + byteOffsetInPage);
        readFromPage(pagePtr2, byteOffsetInPage + smd.fieldsInfo[0].fieldOffset, smd.fieldsInfo[0].fieldSize, &uid);
        String key = smd.schema->getKey(uid);

        // 3.2: copy row
        void *newRowAddr = copyRowInPages(pagePtr2, offset2, pagePtr1, offset1);

        // 3.3: update _indexer
        KVN &kvNode = (*_indexer)[key];
        uint32_t ts = getTimestampRow(oldRowAddr).tStartTSECount();
        int verIdx = kvNode.findVerByTs(ts);
        
        assert(verIdx != -1 && kvNode.isCached[verIdx] == true);    

        kvNode.addr[verIdx] = newRowAddr;
        // 3.4: remove row in old page.
        assert(isBitmapSet(pagePtr2, offset2));
        removeHotRow(pagePtr2, offset2);
    }

    // 4. Delete pagePtr2 from linklist
    auto prev = getPrevPage(pagePtr2);
    auto next = getNextPage(pagePtr2);
    if (prev == nullptr)
        _schemaMap[sid].headPage = getNextPage(pagePtr2);
    else 
        setNextPage(prev, next);
    
    if (next != nullptr)
        setPrevPage(next, prev);

    return true;
}

*/

// 1.2 Row get & set Functions.

// dto::timestamp (_tEndTSECount 8, _tsoId 4, _tStartDelta 4) (Starts in Byte 4)
dto::Timestamp PBRB::getTimestampRow(RowAddr rAddr) {
    uint64_t tEndTSECount;
    uint32_t tsoId, tStartDelta;
    
    // memcpy.
    uint8_t *srcPtr = (uint8_t *)rAddr;
    memcpy(&tEndTSECount, srcPtr + 4, sizeof(uint64_t));
    memcpy(&tsoId, srcPtr + 12, sizeof(uint32_t));
    memcpy(&tStartDelta, srcPtr + 16, sizeof(uint32_t));

    dto::Timestamp ts(tEndTSECount, tsoId, tStartDelta);
    return ts;
}

void PBRB::setTimestampRow(RowAddr rAddr, dto::Timestamp &ts) {
    uint64_t tEndTSECount = ts.tEndTSECount();
    uint32_t tsoId = ts.tsoId();
    uint32_t tStartDelta = ts.tEndTSECount() - ts.tStartTSECount();
    
    // memcpy.
    uint8_t *dstPtr = (uint8_t *)rAddr;
    memcpy(dstPtr + 4, &tEndTSECount, sizeof(uint64_t));
    memcpy(dstPtr + 12, &tsoId, sizeof(uint32_t));
    memcpy(dstPtr + 16, &tStartDelta, sizeof(uint32_t));
}

// PlogAddr
void *PBRB::getPlogAddrRow(RowAddr rAddr) {
    void *retVal = *((void **)((uint8_t *)rAddr + 20));
    return retVal;
}

void PBRB::setPlogAddrRow(RowAddr rAddr, void *PlogAddr) {
    uint8_t *dstPtr = (uint8_t *)rAddr;
    memcpy(dstPtr + 20, &PlogAddr, sizeof(void *));
}

// Status
dto::DataRecord::Status PBRB::getStatusRow(RowAddr rAddr) {
    // Validation.
    K2ASSERT(log::pbrb, sizeof(uint8_t) == sizeof(dto::DataRecord::Status), "Status size != 1");

    dto::DataRecord::Status status;
    
    uint8_t *srcPtr = (uint8_t *)rAddr + 28;
    memcpy(&status, srcPtr, sizeof(uint8_t));

    return status;
}
Status PBRB::setStatusRow(RowAddr rAddr, const dto::DataRecord::Status status) {
    // Validation.
    K2ASSERT(log::pbrb, sizeof(uint8_t) == sizeof(dto::DataRecord::Status), "Status size != 1");

    uint8_t *dstPtr = (uint8_t *)rAddr + 28;
    memcpy(dstPtr, &status, sizeof(uint8_t));

    return Statuses::S200_OK("Set status in row successfully");
}

// isTombstone
bool PBRB::getIsTombstoneRow(RowAddr rAddr) {
    uint8_t tmp;
    uint8_t *srcPtr = (uint8_t *)rAddr + 29;
    memcpy(&tmp, srcPtr, sizeof(uint8_t));
    return tmp ? true : false;
}

Status PBRB::setIsTombstoneRow(RowAddr rAddr, const bool& isTombstone) {

    uint8_t *dstPtr = (uint8_t *)rAddr + 29;
    uint8_t tmp = 0;
    if (isTombstone)
        tmp = 0xFF;
    memcpy(dstPtr, &tmp, sizeof(uint8_t));

    return Statuses::S200_OK("Set isTombstone in row successfully");
}

// request_id
uint64_t PBRB::getRequestIdRow(RowAddr rAddr) {
    uint64_t retVal = *((uint64_t *)((uint8_t *)rAddr + 30));
    return retVal;
}

Status PBRB::setRequestIdRow(RowAddr rAddr, const uint64_t& request_id) {

    uint8_t *dstPtr = (uint8_t *)rAddr + 30;
    memcpy(dstPtr, &request_id, sizeof(uint64_t));
    
    return Statuses::S200_OK("Set request_id in row successfully");
}

}