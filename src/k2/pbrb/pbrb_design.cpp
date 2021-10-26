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
void *PBRB::cacheRowHeaderFrom(BufferPage *pagePtr, RowOffset rowOffset, dto::DataRecord* rec) {
    //K2LOG_I(log::pbrb, "function cacheRowFromPlog(BufferPage, RowOffset:{}, PlogAddr", rowOffset);
    if (pagePtr == nullptr) {
        K2LOG_I(log::pbrb, "Trying to cache row to nullptr!");
        return nullptr;
    }
    if (rowOffset > _schemaMap[getSchemaIDPage(pagePtr)].maxRowCnt) {
        K2LOG_I(log::pbrb, "Row Offset out of range!");
        return nullptr;
    }
    if (isBitmapSet(pagePtr, rowOffset)) {
        K2LOG_I(log::pbrb, "Conflict: move row to occupied slot, In page, offset:{}", rowOffset);
        return nullptr;
    }

    SchemaId schemaId = getSchemaIDPage(pagePtr);
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
void *PBRB::cacheRowFieldFromDataRecord(BufferPage *pagePtr, RowOffset rowOffset, void* valueAddr, size_t strSize, uint32_t fieldID, bool isStr)
{
    //K2LOG_I(log::pbrb, "function cacheRowFromPlog(BufferPage, RowOffset:{}, field:{}", rowOffset, field);
    if (pagePtr == nullptr) {
        K2LOG_I(log::pbrb, "Trying to cache row to nullptr!");
        return nullptr;
    }
    if (rowOffset > _schemaMap[getSchemaIDPage(pagePtr)].maxRowCnt) {
        K2LOG_I(log::pbrb, "Row Offset out of range!");
        return nullptr;
    }
    if (isBitmapSet(pagePtr, rowOffset)) {
        K2LOG_I(log::pbrb, "Conflict: move row to occupied slot, In page, offset:{}", rowOffset);
        return nullptr;
    }

    SchemaId schemaId = getSchemaIDPage(pagePtr);
    SchemaMetaData smd = _schemaMap[schemaId];
    uint32_t byteOffsetInPage = _pageHeaderSize + smd.occuBitmapSize + 
                            smd.rowSize * rowOffset;

    void *rowBasePtr = (uint8_t *)(pagePtr) + byteOffsetInPage;

    // Copy a field to PBRB row
    void *destPtr = (void *) ((uint8_t *) rowBasePtr + smd.fieldsInfo[fieldID].fieldOffset);
    
    // TYPE: STRING
    if (isStr) {
        K2ASSERT(log::pbrb, strSize < FTSize[static_cast<int>(k2::dto::FieldType::STRING)] - 1, "strSize < FTSize[String] - 1");
        
        if (strSize > 0)
            memcpy(destPtr, ((k2::String *) valueAddr)->c_str(), strSize + 1);
        else {
            *(uint8_t *)destPtr = '\0';
            K2LOG_D(log::pbrb, "StrSize == 0, Wrote \\0");
            return rowBasePtr;
        }
        
        K2LOG_D(log::pbrb, "Copied k2::String: {}", valueAddr);
        K2LOG_D(log::pbrb, "Copied {} byte(s) to {}", strSize + 1, destPtr);
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
            if (kvNode.isCached[i] && kvNode.timestamp[i] < (int) watermark->tStartTSECount()) {
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

//find an empty slot between the beginOffset and endOffset in the page
RowOffset PBRB::findEmptySlotInPage(BufferPage *pagePtr, RowOffset beginOffset, RowOffset endOffset)
{
    uint32_t schemaId = getSchemaIDPage(pagePtr);
    
    uint32_t maxRowCnt = _schemaMap[schemaId].maxRowCnt;
    
    if (endOffset > maxRowCnt)
        return 0xFFFFFFFF;

    for (uint32_t i = beginOffset; i < endOffset; i++)
    {
        uint32_t byteIndex = i / 8;
        uint8_t curByte = readFromPage<uint8_t>(pagePtr, _pageHeaderSize + byteIndex, 1);
        
        uint32_t Offset = i % 8;
        curByte = curByte >> Offset;
        uint8_t bitValue = curByte & (uint8_t) 0x1;
        if (bitValue == 0)
            return i; //find an empty slot, return the offset
    }
    return 0xFFFFFFFF; //not find an empty slot
}

//find an empty slot in the page
RowOffset PBRB::findEmptySlotInPage(BufferPage *pagePtr)
{
    uint32_t schemaId = getSchemaIDPage(pagePtr);
    
    uint32_t maxRowCnt = _schemaMap[schemaId].maxRowCnt;
    for (uint32_t i = 0; i < maxRowCnt; i++)
    {
        uint32_t byteIndex = i / 8;
        uint8_t curByte = readFromPage<uint8_t>(pagePtr, _pageHeaderSize + byteIndex, 1);

        uint32_t Offset = i % 8;
        curByte = curByte >> Offset;
        uint8_t bitValue = curByte & (uint8_t) 0x1;
        if (bitValue == 0)
            return i; //find an empty slot, return the offset
    }
    return 0xFFFFFFFF; //not find an empty slot
}

//find an empty slot by querying the page one by one in turn
std::pair<BufferPage *, RowOffset> PBRB::findCacheRowPosition(uint32_t schemaID)
{
    BufferPage *pagePtr = _schemaMap[schemaID].headPage;
    K2LOG_D(log::pbrb, "^^^^^^^^findCacheRowPosition, schemaID:{}, pagePtr empty:{}", schemaID, pagePtr==nullptr);
    while (pagePtr != nullptr) {
        RowOffset rowOffset = findEmptySlotInPage(pagePtr);
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
std::pair<BufferPage *, RowOffset> PBRB::findCacheRowPosition(uint32_t schemaID, String key) {
    KVN &kvNode = (*_indexer)[key];
    BufferPage *pagePtr;
    RowOffset rowOffset;
    // 1. has hot row:
    if (kvNode.hasCachedVer()) {
        pagePtr = getPageAddr(kvNode.addr[0]);
        rowOffset = findEmptySlotInPage(pagePtr);
        if (rowOffset & 0x80000000)
            return findCacheRowPosition(schemaID);
        else {
            // void *hotAddr = cacheRowFromPlog(pagePtr, rowOffset, pAddr);
            // kvNode.insertHotRow(hotAddr);
            return std::make_pair(pagePtr, rowOffset);
        }
    }

    // 2. 3 cold versions.
    else {
        auto pit = _indexer->find(key);
        auto nit = pit;

        int maxRetryCnt = 5;
        BufferPage *prevPageAddr = nullptr;
        RowOffset prevOff = 0;
        for (int i = 0; i < maxRetryCnt; i++) {
            if (pit != _indexer->begin())
            {
                pit--;
                if (pit->second.hasCachedVer()) {
                    auto retVal = findRowByAddr(pit->second.addr[0]);
                    prevPageAddr = retVal.first;
                    prevOff = retVal.second;
                    break;
                }
            }
            else break;
        }

        BufferPage *nextPageAddr = nullptr;
        RowOffset nextOff = 0;
        for (int i = 0; i < maxRetryCnt && nit != _indexer->end(); i++) {
            nit++;
            if (nit == _indexer->end())
                break;
            if (nit->second.hasCachedVer()) {
                auto retVal = findRowByAddr(nit->second.addr[0]);
                nextPageAddr = retVal.first;
                nextOff = retVal.second;
                break;
            }
        }

        // cannot find neighboring pages...
        if (prevPageAddr == nullptr && nextPageAddr == nullptr) {
            return findCacheRowPosition(schemaID);
        }

        // insert into next page...
        else if (prevPageAddr == nullptr && nextPageAddr != nullptr) {
            RowOffset off = findEmptySlotInPage(nextPageAddr);
            if (off & 0x80000000)
                return findCacheRowPosition(schemaID);
            else
                return std::make_pair(nextPageAddr, off);
        }

        // insert into prev page...
        else if (prevPageAddr != nullptr && nextPageAddr == nullptr) {
            RowOffset off = findEmptySlotInPage(prevPageAddr);
            if (off & 0x80000000)
                return findCacheRowPosition(schemaID);
            else
                return std::make_pair(prevPageAddr, off);
        }

        // insert into page with lower occupancy rate...
        else {
            if (prevPageAddr == nextPageAddr) {
                RowOffset off = findEmptySlotInPage(prevPageAddr, prevOff + 1, nextOff);
                if (off & 0x80000000)
                    off = findEmptySlotInPage(prevPageAddr);
                if (off & 0x80000000)
                    return findCacheRowPosition(schemaID);
                return std::make_pair(prevPageAddr, off);
            }

            uint16_t prevHotNum = getHotRowsNumPage(prevPageAddr);
            uint16_t nextHotNum = getHotRowsNumPage(nextPageAddr);
            if (prevHotNum <= nextHotNum) {
                RowOffset off = findEmptySlotInPage(prevPageAddr, prevOff + 1, _schemaMap[schemaID].maxRowCnt);
                if (off & 0x80000000)
                    off = findEmptySlotInPage(prevPageAddr);
                if (off & 0x80000000)
                    return findCacheRowPosition(schemaID);
                return std::make_pair(prevPageAddr, off);
            }
            else {
                RowOffset off = findEmptySlotInPage(nextPageAddr, 0, nextOff);
                if (off & 0x80000000)
                    off = findEmptySlotInPage(nextPageAddr);
                if (off & 0x80000000)
                    return findCacheRowPosition(schemaID);
                return std::make_pair(nextPageAddr, off);
            }
        }
    }

    return std::make_pair(nullptr, 0);
}


//mark the row as unoccupied when evicting a hot row
void PBRB::removeHotRow(BufferPage *pagePtr, RowOffset offset)
{
    clearRowBitMapPage(pagePtr, offset);
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
    void *src = getAddrByPageAndOffset(srcPagePtr, srcOffset);
    void *dst = getAddrByPageAndOffset(dstPagePtr, dstOffset);

    memcpy(dst, src, _schemaMap[dstSid].rowSize);
    setRowBitMapPage(dstPagePtr, dstOffset);

    return dst;
}

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