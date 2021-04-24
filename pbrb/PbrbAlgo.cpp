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
    uint32_t rowOffsetInPage = _pageHeaderSize + smd.occuBitmapSize + 
                            smd.rowSize * rowOffset;

    void *rowBasePtr = (uint8_t *)(pagePtr) + rowOffsetInPage;

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
    setHotRowsNumPage(pagePtr, getHotRowsNumPage(pagePtr) + 1);

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
    auto retPair = findCacheRowPosition(schemaId, key, pAddress);
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
            BufferPage *pagePtr = getNextPage(pagePtr);
        else
            return std::make_pair(pagePtr, rowOffset);
    }
    return std::make_pair(nullptr, 0);
}

std::pair<BufferPage *, RowOffset> PBRB::findCacheRowPosition(uint32_t schemaID, String key, PLogAddr pAddr) {
    KVN &kvNode = (*_indexer)[key];
    BufferPage *pagePtr;
    RowOffset rowOffset;
    // 1. has hot row:
    if (kvNode.hasCachedVer()) {
        pagePtr = getPageAddr(kvNode.addr[0]);
        rowOffset = findEmptySlotInPage(pagePtr);
        if (rowOffset & 0x80000000)
            return std::make_pair(nullptr, 0);
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
void PBRB::removeHotRow(BufferPage *pagePtr, int offset)
{
    clearRowBitMapPage(pagePtr, offset);
    setHotRowsNumPage(pagePtr, getHotRowsNumPage(pagePtr) - 1);
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