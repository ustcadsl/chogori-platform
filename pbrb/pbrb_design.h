#include <iostream>
#include <cstdlib>
#include <cstring>
#include <new>
#include <memory>
#include <bitset>
#include <cstddef>
#include <cassert>

#include <map>
#include <list>
#include <tuple>
#include <vector>

#include "plog.h"
#include "schema.h"

const int pageSize = 1024;
const long long mask = 0x00000000000003FF;

struct BufferPage
{
    uint8_t content[pageSize];
};

using String = std::string;
using SchemaId = uint32_t;
using SchemaVer = uint16_t;
using RowOffset = uint32_t;

struct fieldMetaData
{
    uint32_t fieldSize;
    uint32_t fieldOffset;
    bool isNullable;
    bool isVariable;
};

extern SchemaUMap schemaUMap;
extern uint32_t FTSize[];

struct SchemaMetaData
{
    SimpleSchema *schema;
    uint32_t occuBitmapSize;
    uint32_t nullableBitmapSize;
    uint32_t maxRowCnt;
    std::vector<fieldMetaData> fieldsInfo;
    uint32_t rowSize;

    BufferPage *headPage = nullptr;

    SchemaMetaData(SchemaId schemaId, uint32_t pageSize, uint32_t pageHeaderSize, 
                   uint32_t rowHeaderSize, BufferPage *pagePtr) {
        setInfo(schemaId, pageSize, pageHeaderSize, rowHeaderSize);
        setHeadPage(pagePtr);
    }

    SchemaMetaData(SchemaId schemaId, uint32_t pageSize, uint32_t pageHeaderSize, uint32_t rowHeaderSize) {
        setInfo(schemaId, pageSize, pageHeaderSize, rowHeaderSize);
    }


    // Default Construct
    SchemaMetaData() {}

    void setHeadPage(BufferPage *pagePtr) {
        headPage = pagePtr;
        std::cout << "Set HeadPagePtr: " << headPage << std::endl;
    }

    // Construct from a Schema
    void setInfo(SchemaId schemaId, uint32_t pageSize, uint32_t pageHeaderSize, uint32_t rowHeaderSize) {
        // read from Schema
        if (schemaUMap.umap.find(schemaId) == schemaUMap.umap.end())
            return;

        schema = schemaUMap.umap[schemaId];

        setNullBitmapSize(schema->fields.size());

        uint32_t currRowOffset = rowHeaderSize + nullableBitmapSize;

        // Set Metadata
        for (int i = 0;i < schema->fields.size();i++) {
            FieldType currFT = schema->fields[i].type;
            fieldMetaData fieldObj;
            
            fieldObj.fieldSize = FTSize[currFT];
            fieldObj.fieldOffset = currRowOffset;
            fieldObj.isNullable = false;
            fieldObj.isVariable = false;

            fieldsInfo.push_back(fieldObj);

            // Go to next field.
            currRowOffset += fieldObj.fieldSize;
        }
        
        // set rowSize
        rowSize = currRowOffset;
        setOccuBitmapSize(pageSize, rowHeaderSize);
        maxRowCnt = (pageSize - pageHeaderSize - occuBitmapSize) / rowSize;

        std::cout << "\nGot Schema: " << schemaId << std::endl 
                  << "name: " << schema->name << std::endl
                  << "occuBitmapSize: " << occuBitmapSize << std::endl
                  << "rowSize: " << rowSize << std::endl
                  << "\trowHeaderSize: " << rowHeaderSize << std::endl
                  << "\tnullableBMSize: " << nullableBitmapSize << std::endl
                  << "\tAllFieldSize: " << rowSize - rowHeaderSize - nullableBitmapSize << std::endl
                  << "maxRowCnt: " << maxRowCnt << std::endl;
    }

    void setNullBitmapSize(uint32_t fieldNumber) {
        nullableBitmapSize = (fieldNumber - 1) / 8 + 1;
    }

    void setOccuBitmapSize(uint32_t pageSize, uint32_t rowHeaderSize) {
        occuBitmapSize = (pageSize / rowSize - 1) / 8 + 1;
    }

};

class KVN;
typedef std::map<std::string, KVN> Index;

class PBRB
{

private:
    uint32_t _maxPageNumber;
    uint32_t _pageSize = pageSize;
    uint32_t _pageHeaderSize = 64;
    uint32_t _rowHeaderSize = 4 + 16 + 8;

    //A list to store allocated free pages
    std::list<BufferPage *> _freePageList;

    //a Map to store pages used by different SKV table, and one SKV table corresponds to a list
    std::map<int, std::list<BufferPage *>> _usedPageMap;

    // A map to record the schema metadata.
    std::map<SchemaId, SchemaMetaData> _schemaMap;

    Index *_indexer;
public:

    int *watermark;
    //Initialize a PBRB cache
    PBRB(int maxPageNumber, int *wm, Index *indexer)
    {
        watermark = wm;
        this->_maxPageNumber = maxPageNumber;
        auto aligned_val = std::align_val_t{_pageSize}; //page size = 64KB

        _indexer = indexer;
        /* method 1:
        for (int i = 0; i < maxPageNumber; i++)
        {
            //support by c++17, Align the last 16 bits of the address
            BufferPage *pagePtr = static_cast<BufferPage *>(operator new(sizeof(BufferPage), aligned_val));
            freePageList.add(pagePtr);
        }*/

        // method 2:
        // alloc maxPageNumber pages when initialize.
        BufferPage *basePtr = static_cast<BufferPage *>(operator new(maxPageNumber * sizeof(BufferPage), aligned_val));
        std::cout << std::dec << "\nPBRB: Allocated " << maxPageNumber << " Pages in " << basePtr << std::endl;
        for (int idx = 0; idx < maxPageNumber; idx++)
        {
            _freePageList.push_back(basePtr + idx);
        }
    }

    // 0. Two template functions to read and write data in the page.

    // From srcPtr, write size byte(s) of data to pagePtr with offset.
    template <typename T>
    void writeToPage(BufferPage *pagePtr, size_t offset, const T *srcPtr, size_t size)
    {
        const void *sPtr = static_cast<const void *>(srcPtr);
        void *destPtr = static_cast<void *>(pagePtr->content + offset);
        memcpy(destPtr, sPtr, size);
    }

    // Read from pagePtr + offset with size to srcPtr;
    template <typename T>
    T readFromPage(const BufferPage *pagePtr, size_t offset, size_t size)
    {
        T result;
        void *dPtr = static_cast<void *>(&result);
        const void *sPtr = static_cast<const void *>(pagePtr->content + offset);
        memcpy(dPtr, sPtr, size);
        return result;
    }

    void readFromPage(const BufferPage *pagePtr, size_t offset, size_t size, void *dPtr) {
        const void *sPtr = static_cast<const void *>(pagePtr->content + offset);
        memcpy(dPtr, sPtr, size);
    }

    BufferPage *getPageAddr(void *rowAddr){
        return (BufferPage *) ((uint64_t) rowAddr & ~mask);
    }

    // 1. Header 'set' and 'get' functions.

    // set (magic, 0, 2)
    void setMagicPage(BufferPage *pagePtr, uint16_t magic)
    {
        // magic is 2 bytes;
        writeToPage<uint16_t>(pagePtr, 0, &magic, 2);
    }

    // get (magic, 0, 2)
    uint16_t getMagicPage(const BufferPage *pagePtr)
    {
        return readFromPage<uint16_t>(pagePtr, 0, 2);
    }

    // set (schemaID, 2, 4)
    void setSchemaIDPage(BufferPage *pagePtr, uint32_t schemaID)
    {
        writeToPage<SchemaId>(pagePtr, 2, &schemaID, 4);
    }

    // get (schemaID, 2, 4)
    SchemaId getSchemaIDPage(const BufferPage *pagePtr)
    {
        return readFromPage<uint32_t>(pagePtr, 2, 4);
    }

    // set (schemaVer, 6, 2)
    void setSchemaVerPage(BufferPage *pagePtr, uint16_t schemaVer)
    {
        writeToPage<uint16_t>(pagePtr, 6, &schemaVer, 2);
    }

    // get (schemaVer, 6, 2)
    uint16_t getSchemaVerPage(const BufferPage *pagePtr)
    { //schemaVer is 2 bytes
        return readFromPage<uint16_t>(pagePtr, 6, 2);
    }

    // get (prevPagePtr, 8, 8)
    void setPrevPage(BufferPage *pagePtr, BufferPage *prevPagePtr)
    {
        writeToPage<BufferPage *>(pagePtr, 8, &prevPagePtr, 8);
    }

    // set (prevPagePtr, 8, 8)
    BufferPage *getPrevPage(const BufferPage *pagePtr)
    {
        return readFromPage<BufferPage *>(pagePtr, 8, 8);
    }

    // set (nextPagePtr, 16, 8)
    void setNextPage(BufferPage *pagePtr, BufferPage *nextPagePtr)
    { //page pointer is 8 bytes
        writeToPage<BufferPage *>(pagePtr, 16, &nextPagePtr, 8);
    }

    // get (nextPagePtr, 16, 8)
    BufferPage *getNextPage(const BufferPage *pagePtr)
    {
        return readFromPage<BufferPage *>(pagePtr, 16, 8);
    }

    // set (hotRowsNum, 24, 2)
    void setHotRowsNumPage(BufferPage *pagePtr, uint16_t hotRowsNum)
    {
        writeToPage<uint16_t>(pagePtr, 24, &hotRowsNum, 2);
    }

    // set (hotRowsNum, 24, 2)
    uint16_t getHotRowsNumPage(const BufferPage *pagePtr)
    {
        return readFromPage<uint16_t>(pagePtr, 24, 2);
    }

    void setReservedHeader(BufferPage *pagePtr)
    { //reserved is 38 bytes
        memset(pagePtr->content + 26, 0, _pageHeaderSize - 26);
    }

    // 1.2 row get functions.

    void printFieldsRow(const BufferPage *pagePtr, RowOffset rowOffset) {
        SchemaMetaData smd = _schemaMap[getSchemaIDPage(pagePtr)];
        char buf[4096];
        size_t rowOffsetInPage = _pageHeaderSize + smd.occuBitmapSize + 
                                 smd.rowSize * rowOffset;
        for (int idx = 0; idx < smd.fieldsInfo.size(); idx++) {   
            readFromPage(pagePtr, rowOffsetInPage + smd.fieldsInfo[idx].fieldOffset, 
                rowOffsetInPage + smd.fieldsInfo[idx].fieldSize, buf);
            auto t = smd.schema->fields[idx].type;
            if (t == STRING)
                std::cout << "(index, field name, data): " << idx << ", "
                          << smd.schema->fields[idx].name << ", "
                          << buf << std::endl;
            else if (t == INT32T)
                std::cout << "(index, field name, data): " << idx << ", "
                          << smd.schema->fields[idx].name << ", "
                          << *(int *)buf << std::endl;
        }
    }

    // 2. Occupancy Bitmap functions.

    //a bit for a row, page size = 64KB, row size = 128B, there are at most 512 rows, so 512 bits=64 Bytes is sufficient
    void setRowBitMapPage(BufferPage *pagePtr, RowOffset rowOffset)
    {
        uint32_t byteIndex = rowOffset / 8;
        uint32_t offset = rowOffset % 8;
        pagePtr->content[_pageHeaderSize + byteIndex] |= 0x1 << offset;
        setHotRowsNumPage(pagePtr, getHotRowsNumPage(pagePtr) + 1);
    }

    void clearRowBitMapPage(BufferPage *pagePtr, RowOffset rowOffset)
    {
        uint32_t byteIndex = rowOffset / 8;
        uint32_t offset = rowOffset % 8;
        pagePtr->content[_pageHeaderSize + byteIndex] &= ~(0x1 << offset);
        setHotRowsNumPage(pagePtr, getHotRowsNumPage(pagePtr) - 1);
    }

    bool isBitmapSet(BufferPage *pagePtr, RowOffset rowOffset)
    {
        uint32_t byteIndex = rowOffset / 8;
        uint32_t offset = rowOffset % 8;
        uint8_t bit = (pagePtr->content[_pageHeaderSize + byteIndex] >> rowOffset) & 1;
        if (bit)
            return true;
        else
            return false;
    }

    // 3. Oprations.

    // 3.1 Initialize a schema.

    void initializePage(BufferPage *pagePtr) {
    
        memset(pagePtr, 0, sizeof(BufferPage));

        setMagicPage(pagePtr, 0x1010);
        setSchemaIDPage(pagePtr, -1);
        setSchemaVerPage(pagePtr, 0);
        setPrevPage(pagePtr, nullptr);
        setNextPage(pagePtr, nullptr);
        setHotRowsNumPage(pagePtr, 0);
        setReservedHeader(pagePtr);
    }

    //create a pageList for a SKV table according to schemaID
    BufferPage *createCacheForSchema(SchemaId schemaId) {
        
        if (_freePageList.empty())
            return nullptr;
        
        // Get a page and set schemaMetadata.
        BufferPage *pagePtr = _freePageList.front();
        SchemaMetaData smd(schemaId, _pageSize, _pageHeaderSize, _rowHeaderSize, pagePtr);
        _schemaMap.insert({schemaId, smd});

        // Initialize Page.
        memset(pagePtr, 0, sizeof(BufferPage));

        initializePage(pagePtr);
        setSchemaIDPage(pagePtr, schemaId);

        _freePageList.pop_front();

        return pagePtr;
    };

    BufferPage *AllocNewPageForSchema(SchemaId schemaId) {
        if (_freePageList.empty())
            return nullptr;

        BufferPage *pagePtr = _schemaMap[schemaId].headPage;
        if (pagePtr == nullptr)
            return createCacheForSchema(schemaId);
        else {
            BufferPage *newPage = _freePageList.front();
            
            // Initialize Page.
            memset(newPage, 0, sizeof(BufferPage));

            initializePage(newPage);
            setSchemaIDPage(newPage, schemaId);
            setNextPage(newPage, _schemaMap[schemaId].headPage);

            _schemaMap[schemaId].headPage = newPage;
            _freePageList.pop_front();
            return newPage;
        }
    };

    void outputHeader(BufferPage *pagePtr) {
        std::cout << "\nMagic: " << std::hex << getMagicPage(pagePtr) << std::endl << std::oct
                  << "SchemaId: " << getSchemaIDPage(pagePtr) << std::endl
                  << "SchemaVer: " << getSchemaVerPage(pagePtr) << std::endl
                  << "PrePageAddre: " << getPrevPage(pagePtr) << std::endl
                  << "NextPageAddre: " << getNextPage(pagePtr) << std::endl
                  << "HotRowNum: " << getHotRowsNumPage(pagePtr) << std::endl;
    }
    // Offsets for each fields – relative offset (from the beginning of the record) for each fields
    // as there are variable length fields and nullable fields length (thus each fields will have
    // different offset for different row). Note: for nullable fields, if it is null,
    // we could use 0 as indicator inside the offset list.
    // Fields part.
    // – Null fields doesn’t exist here.
    // Out-of-Place fields holds a location(PlogId + 4 byte offset, which can be optimized).
    // Variable length fields length can be calculated from its offset and next non null neighbor offset.

    //cache the record that read from Plog into the PBRB
    // void cacheRowFromPlog(BufferPage *pagePtr, RowOffset rowOffset, SKVRecord record, PLogAddr pAddress);

    // move cold row in pAddress to PBRB
    void *cacheColdRow(PLogAddr pAddress);

    // move cold row in pAddress to PBRB and insert hot address into KVNode
    void *cacheColdRow(PLogAddr pAddress, String key);
    
    // Copy memory from plog to (pagePtr, rowOffset)
    void *cacheRowFromPlog(BufferPage *pagePtr, RowOffset rowOffset, PLogAddr pAddress);
    
    // find an empty slot between the beginOffset and endOffset in the page
    RowOffset findEmptySlotInPage(BufferPage *pagePtr, RowOffset beginOffset, RowOffset endOffset);

    // find an empty slot in the page
    RowOffset findEmptySlotInPage(BufferPage *pagePtr);

    std::pair<BufferPage *, RowOffset> findCacheRowPosition(uint32_t schemaID);

    // Find the page pointer and row offset to cache cold row
    std::pair<BufferPage *, RowOffset> findCacheRowPosition(uint32_t schemaID, String key, PLogAddr pAddr);
    
    // store hot row in the empty row
    // void cacheHotRow(uint32_t schemaID, SKVRecord hotRecord);

    // return pagePtr and rowOffset.
    std::pair<BufferPage *, RowOffset> findRowByAddr(void *rowAddr);

    // evict row and return cold addr.
    PLogAddr evictRow(void *rowAddr);
    
    //mark the row as unoccupied when evicting a hot row
    void removeHotRow(BufferPage *pagePtr, RowOffset offset);

    //split a full page into two pages
    bool splitPage(BufferPage *pagePtr);

    //merge two pages into one page, and recliam a page
    bool mergePage(BufferPage *pagePtr1, BufferPage *pagePtr2);

    //allocate a free page from the freePageList to store hot rows
    BufferPage *allocateFreePage();

    //after merging two pages, reclaim a page and insert it to freePageList
    void reclaimPage(BufferPage *pagePtr);

    // copy a row from (srcPagePtr, srcOffset) to (dstPagePtr, dstOffset)
    void *copyRowInPages(BufferPage *srcPagePtr, RowOffset srcOffset, BufferPage *dstPagePtr, RowOffset dstOffset);
    
    //insert a hot row into a pagelist according to schemaID
    //bool insertHotRow(int schemaID, Row hotRow) {}

    //trigger evicition in a pagelist according to the schemaID and eviction policy
//    rowPosition pickRowToEviction(SchemaId schemaID, evictPolicy policy);

    //evict rows from PBRB cache in the background
    void doBackgroundPBRBGC();

    uint32_t getTimestamp(void *addr) {
        uint32_t ts;
        memcpy(&ts, (uint8_t *)addr + 4, 4);
        return ts;
    }

    void printRowsBySchema(SchemaId sid) {
        SchemaMetaData smd = _schemaMap[sid];
        BufferPage *pagePtr = smd.headPage;
        while (pagePtr) {
            std::cout << "\nIn Page: " << pagePtr << std::endl;
            outputHeader(pagePtr);

            for (int i = 0; i < smd.maxRowCnt; i++)
                if (isBitmapSet(pagePtr, i)) {
                    std::cout << "Row Offset: " << i << std::endl;
                    printFieldsRow(pagePtr, i);
                }
            
            pagePtr = getNextPage(pagePtr);
        }
    }

    void *getAddrByPageAndOffset(BufferPage *pagePtr, RowOffset offset) {
        auto sid = getSchemaIDPage(pagePtr);
        assert(_schemaMap.find(sid) != _schemaMap.end());
        size_t byteOffsetInPage = _pageHeaderSize + _schemaMap[sid].occuBitmapSize + 
                                  _schemaMap[sid].rowSize * offset;
        void *address = (void *) ((uint8_t *)pagePtr + byteOffsetInPage);
        return address;
    }
};