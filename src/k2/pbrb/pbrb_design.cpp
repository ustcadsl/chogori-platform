#include <iostream>
#include "pbrb_design.h"
#include "indexer.h"

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
            if (kvNode.isCached[i] && kvNode.timestamp[i] < *watermark) {
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

    // found a address to replace.
    if (replaceAddr != nullptr) {
        auto retVal = findRowByAddr(replaceAddr);
        BufferPage *pagePtr = retVal.first;
        RowOffset rowOffset = retVal.second;
        cacheRowFromPlog(pagePtr, rowOffset, pAddress);
        kvNode.insertRow(replaceAddr, getTimestamp(replaceAddr), true, this);
        return replaceAddr;
    }

    // need to find new place to cache row.
    auto retPair = findCacheRowPosition(schemaId, key);
    BufferPage *pagePtr = retPair.first;
    RowOffset rowOffset = retPair.second;
    void *hotAddr = cacheRowFromPlog(pagePtr, rowOffset, pAddress);
    if (hotAddr != nullptr) {
        kvNode.insertRow(hotAddr, getTimestamp(hotAddr), true, this);
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

std::pair<BufferPage *, RowOffset> PBRB::findCacheRowPosition(uint32_t schemaID)
{
    BufferPage *pagePtr = _schemaMap[schemaID].headPage;
    while (pagePtr != nullptr) {
        RowOffset rowOffset = findEmptySlotInPage(pagePtr);
        if (rowOffset & 0x80000000)
            // go to next page;
            pagePtr = getNextPage(pagePtr);
        else
            return std::make_pair(pagePtr, rowOffset);
    }
    return std::make_pair(nullptr, 0);
}

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
    initializePage(newPage);
    setSchemaIDPage(newPage, sid);
    
    // 2. Find offsets which need to move
    std::vector<RowOffset> offVec;
    uint32_t maxRowNum = smd.maxRowCnt;
    uint32_t moveCnt = getHotRowsNumPage(pagePtr) / 2;
    for (RowOffset i = 0; i < maxRowNum && offVec.size() < moveCnt; i++) {
        if (isBitmapSet(pagePtr, i))
            offVec.push_back(i);
    }

    // 3. insert newPage into LinkList.
    BufferPage *nextPage = getNextPage(pagePtr);
    setNextPage(newPage, nextPage);
    setPrevPage(newPage, pagePtr);
    setNextPage(pagePtr, newPage);
    if (nextPage != nullptr) 
        setPrevPage(nextPage, newPage);
    
    // 4. copy rows in offVec;

    // NOTE: support only field[0] as key and type == INT32_T
    assert(smd.schema->fields[0].type == INT32T);

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
        void *newRowAddr = copyRowInPages(pagePtr, offset, newPage, offset);

        // 4.3: update _indexer.
        KVN &kvNode = (*_indexer)[key];
        uint32_t ts = getTimestamp(oldRowAddr);
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
        uint32_t ts = getTimestamp(oldRowAddr);
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