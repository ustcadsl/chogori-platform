#include <iostream>
#include "pbrb_design.hpp"

struct KVN {
    bool isCached[3] = {false};
    int timestamp[3] = {0};
    void *addr[3] = {nullptr};
    int rowNum = 0;
    uint32_t recentVer = {0};

    bool hasCachedVer() { return isCached[0] || isCached[1] || isCached[2]; }
    bool isAllCached() { return isCached[0] && isCached[1] && isCached[2];}

    int findVerByTs(int ts) {
        for (int i = 0; i < rowNum; i++) {
            if (timestamp[i] == ts) 
                return i;
        }
        return -1;
    }

    void *findColdAddrByTs(int ts) {
        for (int i = 0; i < rowNum; i++) {
            if (timestamp[i] == ts && !isCached[i]) 
                return addr[i];
        }
        return nullptr;
    }

    bool insertRow(void *newAddr, int ts, bool isHot, PBRB *pbrb) {

        int pos = findVerByTs(ts);

        if (pos >= 0) {
            isCached[pos] = isHot;
            addr[pos] = newAddr;
            return true;
        }

        int insertPos;

        for (insertPos = 0;insertPos < rowNum;insertPos++) 
            if (ts > timestamp[insertPos])
                break;

        if (insertPos >= 3)
        {
            std::cout << "This record is older than 3 versions here." << std::endl;
            return false;
        }
        else if (insertPos < rowNum) {

            // evict the oldest hot version
            if (rowNum == 3 && isCached[2]) {
                hotToCold(2, pbrb->evictRow(addr[2]));
            }

            // move rows from insertPos to the last ver;
            for (int i = 2; i > insertPos; i--) {
                timestamp[i] = timestamp[i - 1];
                isCached[i] = isCached[i - 1];
                addr[i] = addr[i - 1];
            }
        }

        // insert into insertPos
        timestamp[insertPos] = ts;
        isCached[insertPos] = isHot;
        addr[insertPos] = newAddr;

        if (rowNum < 3)
            rowNum++;

        return true;
    }

    void hotToCold(int x, void *pAddr){
        isCached[x] = false;
        addr[x] = pAddr;
    }

    void coldToHot(int x, void *hAddr){
        isCached[x] = true;
        addr[x] = hAddr;
    }
};

std::map<std::string, KVN> indexer;


// Copy memory from plog to (pagePtr, rowOffset)
void *PBRB::cacheRowFromPlog(BufferPage *pagePtr, RowOffset rowOffset, PLogAddr pAddress)
{
    std::cout << "\nfunction cacheRowFromPlog(BufferPage * " << pagePtr 
              << ", RowOffset " << rowOffset << ", PLogAddr " << pAddress << "):\n" << std::endl;

    if (pagePtr == nullptr) {
        std::cout << "Trying to cache row to nullptr!" << std::endl;
        return nullptr;
    }
    if (rowOffset > schemaMap[getSchemaIDPage(pagePtr)].maxRowCnt) {
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
    SchemaMetaData smd = schemaMap[schemaId];
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
    if (schemaMap.find(schemaId) == schemaMap.end() || schemaMap[schemaId].headPage == nullptr) {
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
    if (schemaMap.find(schemaId) == schemaMap.end() || schemaMap[schemaId].headPage == nullptr) {
        if (createCacheForSchema(schemaId) == nullptr)
            return nullptr;
    }

    // 1. evict expired versions.
    void *replaceAddr = nullptr;
    KVN &kvNode = indexer[key];
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
    
    uint32_t maxRowCnt = schemaMap[schemaId].maxRowCnt;
    
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
    
    uint32_t maxRowCnt = schemaMap[schemaId].maxRowCnt;
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
    BufferPage *pagePtr = schemaMap[schemaID].headPage;
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
    KVN &kvNode = indexer[key];
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
        auto pit = indexer.find(key);
        auto nit = pit;

        int maxRetryCnt = 5;
        BufferPage *prevPageAddr = nullptr;
        RowOffset prevOff = 0;
        for (int i = 0; i < maxRetryCnt; i++) {
            if (pit != indexer.begin())
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
        for (int i = 0; i < maxRetryCnt && nit != indexer.end(); i++) {
            nit++;
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
                RowOffset off = findEmptySlotInPage(prevPageAddr, prevOff + 1, schemaMap[schemaID].maxRowCnt);
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
    SchemaMetaData smd = schemaMap[sid];
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


// ====================TEST=====================

char str[100][124] = {
    "Test01_01_1\0",
    "Test02_01_2\0",
    "Test03_01_3\0",
    "Test01_02_4\0",
    "Test01_03_5\0",
    "Test02_02_6\0",
    "Test01_04_7\0",
    "Test01_05_8\0"
};

int uid[100] = {1, 2, 3, 1, 1, 2, 1, 1};
int ver[100] = {1, 1, 1, 2, 3, 2, 4, 5};
int timestamp[100] = {1, 2, 3, 4, 5, 6, 7, 8};
int wm = 0;

std::string makeKey(std::string schemaName, int uid) {
    const char *sname = schemaName.c_str();
    char str[256];
    sprintf(str, "%s_%04d", sname, uid);
    return str;
}

// Tests for findEmptyRow.
int test1() {
    indexer.clear();
    wm = 0;
    SimplePlog Plog2;
    SimpleSchema S1({
        "S001", 0, {
            {INT32T, "uid"}, {STRING, "str"}
        }
    });
    auto sid1 = schemaUMap.addSchema(&S1);

    PBRB pbrb(8, &wm);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < 3; i++) {
        std::string newKey = makeKey(S1.name, uid[i]);
        std::cout << "Insert Key: " << newKey << " into indexer." << std::endl;
        
        if (indexer.find(newKey) == indexer.end()) {
            KVN kvn;
            kvn.insertRow(Plog2.cursor, timestamp[i], false, &pbrb);
            indexer.insert({newKey, kvn});
        }
        else {
            KVN &kvn = indexer[newKey];
            kvn.insertRow(Plog2.cursor, timestamp[i], false, &pbrb);
        }

        Plog2.writeSId(sid1);
        Plog2.write(&timestamp[i], 4);
        Plog2.write(&uid[i], 4);
        Plog2.write(str[i], 116);
    }

    for (int i = 3; i < 8; i++) {
        Plog2.writeSId(sid1);
        Plog2.write(&timestamp[i], 4);
        Plog2.write(&uid[i], 4);
        Plog2.write(str[i], 116);
    }

    uint8_t *basePtr = (uint8_t *)Plog2.plog;

    std::string testKey1("S001_0001");
    std::string testKey2("S001_0002");
    std::string testKey3("S001_0003");

    auto hot1 = pbrb.cacheRowFromPlog(page1, 2, indexer[testKey1].addr[0]);
    indexer[testKey1].insertRow(hot1, pbrb.getTimestamp(hot1), true, &pbrb);
    auto hot3 = pbrb.cacheRowFromPlog(page1, 5, indexer[testKey3].addr[0]);
    indexer[testKey3].insertRow(hot3, pbrb.getTimestamp(hot3), true, &pbrb);
    pbrb.cacheColdRow(basePtr + 5 * 128, testKey2);
    std::cout << "\n================ " << __func__ << " ================" << std::endl;
    pbrb.printRowsBySchema(sid1);

    return 0;
}

int test2() {
    indexer.clear();
    wm = 0;
    SimplePlog Plog2;
    SimpleSchema S1({
        "S001", 0, {
            {INT32T, "uid"}, {STRING, "str"}
        }
    });
    auto sid1 = schemaUMap.addSchema(&S1);

    PBRB pbrb(8, &wm);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < 4; i++) {
        std::string newKey = makeKey(S1.name, uid[i]);
        std::cout << "Insert Key: " << newKey << " into indexer." << std::endl;
        
        if (indexer.find(newKey) == indexer.end()) {
            KVN kvn;
            kvn.insertRow(Plog2.cursor, timestamp[i], false, &pbrb);
            indexer.insert({newKey, kvn});
        }
        else {
            KVN &kvn = indexer[newKey];
            kvn.insertRow(Plog2.cursor, timestamp[i], false, &pbrb);
        }

        Plog2.writeSId(sid1);
        Plog2.write(&timestamp[i], 4);
        Plog2.write(&uid[i], 4);
        Plog2.write(str[i], 116);
    }

    for (int i = 4; i < 8; i++) {
        Plog2.writeSId(sid1);
        Plog2.write(&timestamp[i], 4);
        Plog2.write(&uid[i], 4);
        Plog2.write(str[i], 116);
    }

    uint8_t *basePtr = (uint8_t *)Plog2.plog;

    std::string testKey1("S001_0001");
    std::string testKey2("S001_0002");
    std::string testKey3("S001_0003");

    auto hot1 = pbrb.cacheRowFromPlog(page1, 1, indexer[testKey1].addr[1]);
    indexer[testKey1].insertRow(hot1, pbrb.getTimestamp(hot1), true, &pbrb);
    auto hot2 = pbrb.cacheRowFromPlog(page1, 2, indexer[testKey1].addr[0]);
    indexer[testKey1].insertRow(hot2, pbrb.getTimestamp(hot2), true, &pbrb);
    auto hot3 = pbrb.cacheRowFromPlog(page2, 4, indexer[testKey3].addr[0]);
    indexer[testKey3].insertRow(hot3, pbrb.getTimestamp(hot3), true, &pbrb);
    pbrb.cacheColdRow(basePtr + 5 * 128, testKey2);
    std::cout << "\n================ " << __func__ << " ================" << std::endl;
    pbrb.printRowsBySchema(sid1);
    return 0;
}

int test3() {
    indexer.clear();
    wm = 0;
    SimplePlog Plog2;
    SimpleSchema S1({
        "S001", 0, {
            {INT32T, "uid"}, {STRING, "str"}
        }
    });
    auto sid1 = schemaUMap.addSchema(&S1);

    PBRB pbrb(8, &wm);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < 4; i++) {
        std::string newKey = makeKey(S1.name, uid[i]);
        std::cout << "Insert Key: " << newKey << " into indexer." << std::endl;
        
        if (indexer.find(newKey) == indexer.end()) {
            KVN kvn;
            kvn.insertRow(Plog2.cursor, timestamp[i], false, &pbrb);
            indexer.insert({newKey, kvn});
        }
        else {
            KVN &kvn = indexer[newKey];
            kvn.insertRow(Plog2.cursor, timestamp[i], false, &pbrb);
        }

        Plog2.writeSId(sid1);
        Plog2.write(&timestamp[i], 4);
        Plog2.write(&uid[i], 4);
        Plog2.write(str[i], 116);
    }

    for (int i = 4; i < 8; i++) {
        Plog2.writeSId(sid1);
        Plog2.write(&timestamp[i], 4);
        Plog2.write(&uid[i], 4);
        Plog2.write(str[i], 116);
    }

    uint8_t *basePtr = (uint8_t *)Plog2.plog;

    std::string testKey1("S001_0001");
    std::string testKey2("S001_0002");
    std::string testKey3("S001_0003");

    auto hot1 = pbrb.cacheRowFromPlog(page2, 1, indexer[testKey1].addr[1]);
    indexer[testKey1].insertRow(hot1, pbrb.getTimestamp(hot1), true, &pbrb);
    auto hot2 = pbrb.cacheRowFromPlog(page1, 2, indexer[testKey1].addr[0]);
    indexer[testKey1].insertRow(hot2, pbrb.getTimestamp(hot2), true, &pbrb);
    auto hot3 = pbrb.cacheRowFromPlog(page2, 4, indexer[testKey3].addr[0]);
    indexer[testKey3].insertRow(hot3, pbrb.getTimestamp(hot3), true, &pbrb);
    pbrb.cacheColdRow(basePtr + 5 * 128, testKey2);
    std::cout << "\n================ " << __func__ << " ================" << std::endl;
    pbrb.printRowsBySchema(sid1);
    return 0;
}

int test4() {
    indexer.clear();
    wm = 0;
    SimplePlog Plog1;
    SimpleSchema S1({
        "S001", 0, {
            {INT32T, "uid"}, {STRING, "str"}
        }
    });
    auto sid1 = schemaUMap.addSchema(&S1);

    PBRB pbrb(8, &wm);

    pbrb.createCacheForSchema(sid1);

    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < 4; i++) {
        std::string newKey = makeKey(S1.name, uid[i]);
        std::cout << "Insert Key: " << newKey << " into indexer." << std::endl;
        
        if (indexer.find(newKey) == indexer.end()) {
            KVN kvn;
            kvn.insertRow(Plog1.cursor, timestamp[i], false, &pbrb);
            indexer.insert({newKey, kvn});
        }
        else {
            KVN &kvn = indexer[newKey];
            kvn.insertRow(Plog1.cursor, timestamp[i], false, &pbrb);
        }

        Plog1.writeSId(sid1);
        Plog1.write(&timestamp[i], 4);
        Plog1.write(&uid[i], 4);
        Plog1.write(str[i], 116);
    }

    for (int i = 4; i < 8; i++) {
        Plog1.writeSId(sid1);
        Plog1.write(&timestamp[i], 4);
        Plog1.write(&uid[i], 4);
        Plog1.write(str[i], 116);
    }

    uint8_t *basePtr = (uint8_t *)Plog1.plog;

    std::cout << "Read Row 3 of plog: \n\tSchemaId: " << readFromPlog<uint32_t>(basePtr + 2 * 128, 4)
              << "\n\tTimeStamp: " << readFromPlog<uint32_t>(basePtr + 2 * 128 + 4, 4)
              << "\n\tuid: " << readFromPlog<uint32_t>(basePtr + 2 * 128 + 8, 4)
              << std::endl;

    std::string testKey1("S001_0001");
    std::string testKey2("S001_0002");
    std::string testKey3("S001_0003");
    
    pbrb.cacheColdRow(indexer[testKey1].findColdAddrByTs(4), testKey1); // cache 4

    pbrb.cacheColdRow(indexer[testKey1].findColdAddrByTs(1), testKey1); // cache 1

    pbrb.cacheColdRow(basePtr + 4 * 128, testKey1); // cache 5

    pbrb.cacheColdRow(basePtr + 5 * 128, testKey2); // cache 6
    pbrb.printRowsBySchema(sid1);

    pbrb.cacheColdRow(basePtr + 6 * 128, testKey1); // evict 1; cache 7
    pbrb.printRowsBySchema(sid1);
    wm = 7;

    pbrb.cacheColdRow(basePtr + 7 * 128, testKey1); // evict 4, 5; cache 8

    std::cout << "\n================ " << __func__ << " ================" << std::endl;
    pbrb.printRowsBySchema(sid1);
    return 0;
}

int main() {
    test1();
    test2();
    test3();
    test4();
    return 0;
}
