#pragma once

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

#include <k2/common/Log.h>
#include <k2/dto/FieldTypes.h>
#include <k2/dto/Timestamp.h>
#include <k2/dto/SKVRecord.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/Collection.h>
#include <k2/dto/K23SI.h>
//#include <k2/indexer/IndexerInterface.h>
#include <fstream>
#include <k2/indexer/MapIndexer.h>
//#include <k2/indexer/HOTIndexer.h>
#include "plog.h"
#include "schema.h"

namespace k2::log {
inline thread_local k2::logging::Logger pbrb("k2::pbrb");
}

namespace k2
{

using SchemaId = uint32_t;
using SchemaVer = uint16_t;
using RowOffset = uint32_t;
using RowAddr = void *;
using CRC32 = uint32_t;

const int pageSize = 64*1024; //64KB
const long long mask = 0x000000000000FFFF; //0x000000000000FFFF;
namespace pbrb {
    const uint32_t errMask = 1 << 31;

    const uint32_t rowCRC32Offset = 0;
    const uint32_t rowTSOffset = sizeof(CRC32);
    const uint32_t rowPlogAddrOffset = sizeof(CRC32) + sizeof(dto::Timestamp);

    bool isValid(uint32_t testVal);
}

// Field Types

/*struct SchemaField {
    k2::dto::FieldType type;
    String name;
};*/

struct SimpleSchema {
    String name;
    uint32_t version = 0;
    uint32_t schemaId = pbrb::errMask;
    std::vector<k2::dto::SchemaField> fields;
    String getKey(int id){
        char buf[256];
        sprintf(buf, "%s_%04d", name.c_str(), id);
        return buf;
    };
};

struct SchemaUMap {
    std::unordered_map<SchemaId, SimpleSchema> umap; 
    uint32_t currIdx = 0;
    uint32_t addSchema(SimpleSchema schemaPtr) {
        uint32_t retVal = currIdx;
        umap.insert({currIdx++, schemaPtr});
        schemaPtr.schemaId = retVal;
        return retVal;
    }
};

static uint32_t FTSize[256] = {
    0,                // NULL_T = 0,
    100,    // STRING, 128 // NULL characters in string is OK
    sizeof(int16_t),  // INT16T,
    sizeof(int32_t),  // INT32T,
    sizeof(int64_t),  // INT64T,
    sizeof(float),    // FLOAT, // Not supported as key field for now
    sizeof(double),   // DOUBLE,  // Not supported as key field for now
    sizeof(bool),     // BOOL,
    sizeof(std::decimal::decimal64),  // DECIMAL64, // Provides 16 decimal digits of precision
    sizeof(std::decimal::decimal128), // DECIMAL128, // Provides 34 decimal digits of precision
    1,      // FIELD_TYPE, // The value refers to one of these types. Used in query filters.
    // NOT_KNOWN = 254,
    // NULL_LAST = 255
};

static SchemaUMap schemaUMap;

struct BufferPage
{
    uint8_t content[pageSize];
};

struct fieldMetaData
{
    uint32_t fieldSize;
    uint32_t fieldOffset;
    bool isNullable;
    bool isVariable;
};

//extern SchemaUMap schemaUMap;
//extern uint32_t FTSize[];

// std::map<std::pair<String, uint32_t>, uint32_t> schemaMap (name + version) -> version 
// uint32_t cursor = 0;
// inserting:
//      
struct SchemaMetaData
{
    SimpleSchema *schema;
    uint32_t occuBitmapSize;
    uint32_t nullableBitmapSize;
    uint32_t maxRowCnt;
    std::vector<fieldMetaData> fieldsInfo;
    uint32_t rowSize;

    uint32_t curPageNum = 0;
    uint32_t curRowNum = 0;
    BufferPage *headPage = nullptr;
    BufferPage *tailPage = nullptr;

    void printInfo()
    {
        K2LOG_I(log::pbrb, "{}, {}, {}, {}, {}, {}, {}, {}", (void *)schema, occuBitmapSize, nullableBitmapSize, maxRowCnt, rowSize, curPageNum, curRowNum, (void *)headPage);
    }

    SchemaMetaData(SchemaId schemaId, uint32_t pageSize, uint32_t pageHeaderSize,
                    uint32_t rowHeaderSize, BufferPage *pagePtr) {
        setInfo(schemaId, pageSize, pageHeaderSize, rowHeaderSize);
        setHeadPage(pagePtr);
        K2LOG_I(log::pbrb, "Set HeadPagePtr for schema:({}, {}), pagePtr empty:{}", schemaId, schema->name, pagePtr == nullptr);
    }

    SchemaMetaData(SchemaId schemaId, uint32_t pageSize, uint32_t pageHeaderSize, uint32_t rowHeaderSize) {
        setInfo(schemaId, pageSize, pageHeaderSize, rowHeaderSize);
    }

    // Default Construct
    SchemaMetaData() {}

    void setHeadPage(BufferPage *pagePtr) {
        headPage = pagePtr;
        tailPage = pagePtr;
    }

    // Construct from a Schema
    void setInfo(SchemaId schemaId, uint32_t pageSize, uint32_t pageHeaderSize, uint32_t rowHeaderSize) {
        // read from Schema
        // K2LOG_I(log::pbrb, "set smd info: rowHeaderSize: {}", rowHeaderSize);
        if (schemaUMap.umap.find(schemaId) == schemaUMap.umap.end())
            return;

        schema = &(schemaUMap.umap[schemaId]);

        setNullBitmapSize(schema->fields.size());

        uint32_t currRowOffset = rowHeaderSize + nullableBitmapSize;

        // Set Metadata
        for (size_t i = 0;i < schema->fields.size();i++) {
            k2::dto::FieldType currFT = schema->fields[i].type;
            fieldMetaData fieldObj;

            fieldObj.fieldSize = FTSize[static_cast<int>(currFT)];
            fieldObj.fieldOffset = currRowOffset;
            fieldObj.isNullable = false;
            fieldObj.isVariable = false;

            fieldsInfo.push_back(fieldObj);

            // Go to next field.
            currRowOffset += fieldObj.fieldSize;

            // K2LOG_I(log::pbrb, "Current type: {}, offset: {}, currRowOffset: {}, rowHeaderSize :{}, nullableBitmapSize: {}", schema->fields[i].type, fieldObj.fieldOffset, currRowOffset, rowHeaderSize, nullableBitmapSize);
        }

        // set rowSize
        rowSize = currRowOffset;
        setOccuBitmapSize(pageSize);
        maxRowCnt = (pageSize - pageHeaderSize - occuBitmapSize) / rowSize;

        // K2LOG_I(log::pbrb, "Add new schema:{}", schema->name);
        // std::cout << std::dec << "\nGot Schema: " << schemaId << std::endl 
        //           << "name: " << schema->name << std::endl
        //           << "occuBitmapSize: " << occuBitmapSize << std::endl
        //           << "rowSize: " << rowSize << std::endl
        //           << "\trowHeaderSize: " << rowHeaderSize << std::endl
        //           << "\tnullableBMSize: " << nullableBitmapSize << std::endl
        //           << "\tAllFieldSize: " << rowSize - rowHeaderSize - nullableBitmapSize << std::endl
        //           << "maxRowCnt: " << maxRowCnt << std::endl;
    }

    void setNullBitmapSize(uint32_t fieldNumber) {
        nullableBitmapSize = (fieldNumber - 1) / 8 + 1;
    }

    void setOccuBitmapSize(uint32_t pageSize) {
        occuBitmapSize = (pageSize / rowSize - 1) / 8 + 1;
    }

};

typedef mapindexer IndexerT;
typedef MapIterator IndexerIterator;

class KVN;
typedef std::map<std::string, KVN> Index;

class PBRB
{

private:

    uint32_t _maxPageNumber;
    uint32_t _pageSize = pageSize;
    uint32_t _pageHeaderSize = 64;
    uint32_t _rowHeaderSize = 4 + 16 + 8 + 1 + 1 + 8;

    uint32_t _maxPageSearchingNum;
    std::string _fcrpOutputFileName;
    //A list to store allocated free pages
    std::list<BufferPage *> _freePageList;

    //a Map to store pages used by different SKV table, and one SKV table corresponds to a list
    std::map<int, std::list<BufferPage *>> _usedPageMap;

    // A map to record the schema metadata.

    IndexerT *_indexer;

    uint32_t splitCnt = 0, evictCnt = 0;

    //std::vector<void*> largeFieldValueVec;

public:

    struct FCRPSlowCaseStatus {
        bool isFound = true;
        int searchPageNum = 0;
    };
    struct cacheRowPosMetrix{
        int size = 0;
        //
        std::vector<int> idxStep;
        std::vector<int> findStep;
        std::vector<int> prev;
        std::vector<int> next;
        std::vector<int> type;
        std::vector<int> accessId1;
        std::vector<String> sname;
        std::vector<int> totalInPageTime;
        std::vector<int> pageSize1;
        std::vector<int> rowSize1;
        std::vector<int> moduleNs;
        std::vector<int> isFound;
        std::vector<int> searchPageNum;

        void Insert(int id, int a, int b, int t, String s, int p, int n, int pn, int rn, FCRPSlowCaseStatus &stat)
        {
            accessId1.push_back(id);
            idxStep.push_back(a);
            findStep.push_back(b);
            type.push_back(t);
            sname.push_back(s);
            prev.push_back(p);
            next.push_back(n);
            pageSize1.push_back(pn);
            rowSize1.push_back(rn);
            isFound.push_back(stat.isFound);
            searchPageNum.push_back(stat.searchPageNum);
            return;
        }

        std::vector<int> accessIds;
        std::vector<String> sname2;
        std::vector<int> inPageTime;
        std::vector<int> pageSize2;
        std::vector<int> rowSize2;
        std::vector<int> inPageType;
        // insert id, time, pageNum, rowNum for access "id"
        void InsertInPage(int id, String sname, int time, int p, int r, int type)
        {
            // K2LOG_I(log::pbrb, "insert accessId:{}, Sname:{}, time:{} ns, pageNum: {}, rowNum: {}, Type:{}", id, sname, time, p, r, type);
            accessIds.push_back(id);
            inPageTime.push_back(time);
            pageSize2.push_back(p);
            rowSize2.push_back(r);
            inPageType.push_back(type);
            sname2.push_back(sname);
        }

        // add all in page time;
        void TotalInPageTime()
        {
            int size = accessIds.size();
            int lastId = accessIds[0];
            int curId = accessIds[0];
            int time = 0;
            pageSize1.push_back(pageSize2[0]);
            rowSize1.push_back(rowSize2[0]);
            for (int idx = 0; idx < size; idx++)
            {
                curId = accessIds[idx];
                if (curId != lastId)
                {
                    totalInPageTime.push_back(time);
                    time = inPageTime[idx];
                    pageSize1.push_back(pageSize2[idx]);
                    rowSize1.push_back(rowSize2[idx]);
                    lastId = curId;
                }
                else
                {
                    time += inPageTime[idx];
                }
            }
            inPageTime.push_back(time);
        }
        int fcrpBase = 0;
        int fcrpNew = 0;
        int accessId = 0;

    } fcrp;
    int align[10000];
    std::map<SchemaId, SchemaMetaData> _schemaMap;
    //SchemaMetaData tempSmeta;
    //int *watermark;
    std::ofstream ofile;
    std::ofstream accessFile;
    k2::dto::Timestamp watermark;
    //Initialize a PBRB cache
    //PBRB(int maxPageNumber, int *wm, Index *indexer)
    PBRB(int maxPageNumber, k2::dto::Timestamp *wm, IndexerT *indexer, uint32_t maxPageSearchingNum)
    {
        watermark = *wm;
        this->_maxPageNumber = maxPageNumber;
        auto aligned_val = std::align_val_t{_pageSize}; //page size = 64KB

        _indexer = indexer;
        _maxPageSearchingNum = maxPageSearchingNum;

        char buf[256];
        sprintf(buf, "fcrp_20WH_400s_maxTryNum_%02d.csv", _maxPageSearchingNum);
        _fcrpOutputFileName = buf;
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
        K2LOG_I(log::pbrb, "PBRB: Allocated: {} Pages in: {}", maxPageNumber, (void *)basePtr);
        K2LOG_I(log::pbrb, "maxPageSearchingNum: {}", _maxPageSearchingNum);
        K2LOG_I(log::pbrb, "fcrp output filename: {}", _fcrpOutputFileName);
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

    // 1.2 Row get & set functions.

    // Row Struct:
    // CRC (4) | Timestamp (16) | PlogAddr (8) | Status (1) | isTombStone(1)

    // CRC:
    uint32_t getCRCRow();
    void setCRCRow();

    // Timestamp: (RowAddr + 4, 16)
    dto::Timestamp getTimestampRow(RowAddr rAddr);
    void setTimestampRow(RowAddr rAddr, dto::Timestamp &ts);

    // PlogAddr: (RowAddr + 20, 8)
    void *getPlogAddrRow(RowAddr rAddr);
    void setPlogAddrRow(RowAddr rAddr, void *PlogAddr);

    // Status: (RowAddr + 28, 1)
    dto::DataRecord::Status getStatusRow(RowAddr rAddr);
    Status setStatusRow(RowAddr rAddr, const dto::DataRecord::Status status);

    // isTombstone: (RowAddr + 29, 1)
    bool getIsTombstoneRow(RowAddr rAddr);
    Status setIsTombstoneRow(RowAddr rAddr, const bool &isTombstone);

    // requestId: (RowAddr + 30, 8)
    uint64_t getRequestIdRow(RowAddr rAddr);
    Status setRequestIdRow(RowAddr rAddr, const uint64_t &request_id);

    // 2. Occupancy Bitmap functions.

    // a bit for a row, page size = 64KB, row size = 128B, there are at most 512 rows, so 512 bits=64 Bytes is sufficient
    void setRowBitMapPage(BufferPage *pagePtr, RowOffset rowOffset)
    {
        SchemaMetaData &smd = _schemaMap[getSchemaIDPage(pagePtr)];
        uint32_t byteIndex = rowOffset / 8;
        uint32_t offset = rowOffset % 8;
        pagePtr->content[_pageHeaderSize + byteIndex] |= 0x1 << offset;
        setHotRowsNumPage(pagePtr, getHotRowsNumPage(pagePtr) + 1);
        smd.curRowNum++;
    }

    void clearRowBitMapPage(BufferPage *pagePtr, RowOffset rowOffset)
    {
        SchemaMetaData &smd = _schemaMap[getSchemaIDPage(pagePtr)];
        uint32_t byteIndex = rowOffset / 8;
        uint32_t offset = rowOffset % 8;
        pagePtr->content[_pageHeaderSize + byteIndex] &= ~(0x1 << offset);
        setHotRowsNumPage(pagePtr, getHotRowsNumPage(pagePtr) - 1);
        smd.curRowNum--;
    }

    inline bool isBitmapSet(BufferPage *pagePtr, RowOffset rowOffset)
    {
        uint32_t byteIndex = rowOffset / 8;
        uint32_t offset = rowOffset % 8;
        uint8_t bit = (pagePtr->content[_pageHeaderSize + byteIndex] >> offset) & 1;
        if (bit)
            return true;
        else
            return false;
    }

    // 3. Operations.

    // 3.1 Initialize a schema.

    void initializePage(BufferPage *pagePtr, SchemaMetaData &smd) {
    
        // Memset May Cause Performance Problems.
        // Optimized to just clear the header and occuBitMap
        memset(pagePtr, 0, _pageHeaderSize + smd.occuBitmapSize);

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
        return createCacheForSchema(schemaId, 0);
    }

    BufferPage *createCacheForSchema(SchemaId schemaId, SchemaVer schemaVer) {
    
        if (_freePageList.empty())
            return nullptr;

        // Get a page and set schemaMetadata.
        BufferPage *pagePtr = _freePageList.front();
        SchemaMetaData smd(schemaId, _pageSize, _pageHeaderSize, _rowHeaderSize, pagePtr);
        _schemaMap.insert_or_assign(schemaId, smd);

        // Initialize Page.
        //memset(pagePtr, 0, sizeof(BufferPage));

        initializePage(pagePtr, smd);
        setSchemaIDPage(pagePtr, schemaId);
        setSchemaVerPage(pagePtr, schemaVer);

        K2LOG_D(log::pbrb, "createCacheForSchema, schemaId: {}, pagePtr empty:{}, _freePageList size:{}, pageSize: {}, smd.rowSize: {}, _schemaMap[0].rowSize: {}", schemaId, pagePtr==nullptr,  _freePageList.size(), sizeof(BufferPage), smd.rowSize, _schemaMap[0].rowSize);

        _freePageList.pop_front();

        return pagePtr;
    };


    BufferPage *AllocNewPageForSchema(SchemaId schemaId) {
        // al1
        auto al1Start = Clock::now();

        if (_freePageList.empty())
            return nullptr;

        BufferPage *pagePtr = _schemaMap[schemaId].headPage;
        _schemaMap[schemaId].curPageNum++;            

        auto al1End = Clock::now();
        auto al1ns = nsec(al1End - al1Start).count();

        if (pagePtr == nullptr)
            return createCacheForSchema(schemaId);
        else {
            // al2
            auto al2Start = Clock::now();
            K2ASSERT(log::pbrb, _freePageList.size() > 0, "There is no free page in free page list!");
            BufferPage *newPage = _freePageList.front();

            // Initialize Page.
            //memset(newPage, 0, sizeof(BufferPage));
            initializePage(newPage, _schemaMap[schemaId]);
            setSchemaIDPage(newPage, schemaId);
            setSchemaVerPage(newPage, _schemaMap[schemaId].schema->version);

            // optimize: using tailPage.
            BufferPage *tail = _schemaMap[schemaId].tailPage;
            // set nextpage
            // while (getNextPage(tail) != nullptr)
            // {
            //     tail = getNextPage(tail);
            // }
            setPrevPage(newPage, tail);
            setNextPage(newPage, nullptr);
            setNextPage(tail, newPage);
            _schemaMap[schemaId].tailPage = newPage;
            auto al2End = Clock::now();
            auto al2ns = nsec(al2End - al2Start).count();

            // al3
            auto al3Start = Clock::now();

            _freePageList.pop_front();

            auto al3End = Clock::now();
            auto al3ns = nsec(al3End - al3Start).count();
            // K2LOG_I(log::pbrb, "Alloc new page type 1: sid: {}, sname: {}, currPageNum: {}", schemaId, _schemaMap[schemaId].schema->name, _schemaMap[schemaId].curPageNum);
            K2LOG_D(log::pbrb, "Remaining _freePageList size:{}", _freePageList.size());
            K2LOG_D(log::pbrb, "Allocated page for schema: {}, page count: {}, time al1: {}, al2: {}, al3: {}, total: {}", _schemaMap[schemaId].schema->name, _schemaMap[schemaId].curPageNum, al1ns, al2ns, al3ns, al1ns + al2ns + al3ns);

            return newPage;
        }
    };

    BufferPage *AllocNewPageForSchema(SchemaId schemaId, BufferPage *pagePtr) {
        // TODO: validation
        if (_freePageList.empty())
        {
            K2LOG_E(log::pbrb, "No Free Page Now!");
            return nullptr;
        }

        K2ASSERT(log::pbrb, _freePageList.size() > 0, "There is no free page in free page list!");
        BufferPage *newPage = _freePageList.front();
        _schemaMap[schemaId].curPageNum++;

        // Initialize Page.
        //memset(newPage, 0, sizeof(BufferPage));
        initializePage(newPage, _schemaMap[schemaId]);
        setSchemaIDPage(newPage, schemaId);
        setSchemaVerPage(newPage, _schemaMap[schemaId].schema->version);

        // insert behind pagePtr
        // pagePtr -> newPage -> nextPage;

        BufferPage *nextPage = getNextPage(pagePtr);
        // nextPtr != tail
        if (nextPage != nullptr) {
            setNextPage(newPage, nextPage);
            setPrevPage(newPage, pagePtr);
            setNextPage(pagePtr, newPage);
            setPrevPage(nextPage, newPage);
        }
        else {
            setNextPage(newPage, nullptr);
            setPrevPage(newPage, pagePtr);
            setNextPage(pagePtr, newPage);
            _schemaMap[schemaId].tailPage = newPage;
        }

        _freePageList.pop_front();
        // K2LOG_I(log::pbrb, "Alloc new page type 2: sid: {}, sname: {}, currPageNum: {}, fromPagePtr: {}", schemaId, _schemaMap[schemaId].schema->name, _schemaMap[schemaId].curPageNum, (void *)pagePtr);
        K2LOG_D(log::pbrb, "Remaining _freePageList size:{}", _freePageList.size());

        return newPage;
    };

    std::list<BufferPage *> getFreePageList() {
        return _freePageList;
    }

    uint32_t getMaxPageNumber() {
        return _maxPageNumber;
    }

    float totalPageUsage() {
        return 1 - ((float)_freePageList.size() / (float)_maxPageNumber);
    }

    void outputHeader(BufferPage *pagePtr) {
        K2LOG_I(log::pbrb, "in outputHeader getHotRowsNumPage:{}", getHotRowsNumPage(pagePtr));
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

    // Copy the header of row from DataRecord of query to (pagePtr, rowOffset)
    void *cacheRowHeaderFrom(uint32_t schemaId, BufferPage *pagePtr, RowOffset rowOffset, dto::DataRecord *rec);

    // Copy the field of row from DataRecord of query to (pagePtr, rowOffset)
    void *cacheRowFieldFromDataRecord(uint32_t schemaId, BufferPage *pagePtr, RowOffset rowOffset, void *field, size_t strSize, uint32_t fieldID, bool isStr);

    // Copy the payload of row from DataRecord of query to (pagePtr, rowOffset)
    void *cacheRowPayloadFromDataRecord(uint32_t schemaId, BufferPage *pagePtr, RowOffset rowOffset, Payload &rowData);

    // find an empty slot between the beginOffset and endOffset in the page
    RowOffset findEmptySlotInPage(uint32_t schemaID, BufferPage *pagePtr, RowOffset beginOffset, RowOffset endOffset);

    // find an empty slot in the page
    RowOffset findEmptySlotInPage(uint32_t schemaID, BufferPage *pagePtr);

    std::pair<BufferPage *, RowOffset> findCacheRowPosition(uint32_t schemaID, FCRPSlowCaseStatus &stat);

    // Find the page pointer and row offset to cache cold row
    std::pair<BufferPage *, RowOffset> findCacheRowPosition(uint32_t schemaID, dto::Key key);

    // Find Cache Row Position From pagePtr to end
    std::pair<BufferPage *, RowOffset> findCacheRowPosition(uint32_t schemaID, BufferPage *pagePtr, FCRPSlowCaseStatus &stat);

    // store hot row in the empty row
    // void cacheHotRow(uint32_t schemaID, SKVRecord hotRecord);

    // return pagePtr and rowOffset.
    std::pair<BufferPage *, RowOffset> findRowByAddr(void *rowAddr);

    // evict row and return cold addr.
    PLogAddr evictRow(void *rowAddr);

    //mark the row as unoccupied when evicting a hot row
    void removeHotRow(BufferPage *pagePtr, RowOffset offset);

    //release the heap space owned by this hot row when using fixed field row
    void releaseHeapSpace(BufferPage *pagePtr, void *rowAddr);

    //release the heap space owned by this hot row when using payload row
    void releasePayloadHeapSpace(BufferPage *pagePtr, void *rowAddr);

    //split a full page into two pages
    bool splitPage(BufferPage *pagePtr);

    //merge pagePtr2 into pagePtr1, reclaim pagePtr2
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
    //void doBackgroundPBRBGC(mapindexer& _indexer, dto::Timestamp& newWaterMark, Duration& retentionPeriod);

    //void doBackgroundPageListGC(String schemaName, uint32_t schemaID, mapindexer& _indexer, dto::Timestamp& newWaterMark, Duration& retentionPeriod);

    float getAveragePageListUsage(float &maxPageListUsage);

    float getCurPageListUsage(uint32_t schemaID);

    // 4. Debugging Output Function.
    void printFieldsRow(const BufferPage *pagePtr, RowOffset rowOffset) {
        SchemaMetaData smd = _schemaMap[getSchemaIDPage(pagePtr)];
        char buf[4096];
        size_t rowOffsetInPage = _pageHeaderSize + smd.occuBitmapSize + 
                                 smd.rowSize * rowOffset;
        for (size_t idx = 0; idx < smd.fieldsInfo.size(); idx++) {   
            readFromPage(pagePtr, rowOffsetInPage + smd.fieldsInfo[idx].fieldOffset, 
                smd.fieldsInfo[idx].fieldSize, buf);
            auto t = smd.schema->fields[idx].type;
            void *valuePtr = nullptr;
            if (t == k2::dto::FieldType::STRING) {
                String *value = new String(buf);
                valuePtr = static_cast<void *>(&value);
                K2LOG_D(log::pbrb, "Field {}: [field name: {}, data: {}]", idx, smd.schema->fields[idx].name, buf);
            }
            else {
                valuePtr = static_cast<void *>(buf);
                //using FieldType = dto::FieldType;
                //using TypeMismatchException = dto::TypeMismatchException;
                K2_DTO_CAST_APPLY_FIELD_VALUE(printField, smd.schema->fields[idx], valuePtr, idx, smd.schema->fields[idx].name);
            }
        }
    }

    template <typename T>
    void printField(const dto::SchemaField& field, void *valuePtr, int idx, String &fname) {
        (void) field;
        T value{};
        if (valuePtr == nullptr) {
            K2LOG_D(log::pbrb, "Field {}: [nullptr]", idx);
        }
        value = *(static_cast<T *>(valuePtr));
        K2LOG_D(log::pbrb, "Field {}: [field name: {}, data: {}]", idx, fname, value);
    }

    void printHeaderRow(const BufferPage *pagePtr, RowOffset rowOffset) {
        SchemaMetaData smd = _schemaMap[getSchemaIDPage(pagePtr)];
        size_t rowOffsetInPage = _pageHeaderSize + smd.occuBitmapSize + 
                                 smd.rowSize * rowOffset;
        uint8_t *rowAddr = (uint8_t *)(pagePtr) + rowOffsetInPage;
        auto ts = getTimestampRow(rowAddr);
        auto pAddr = getPlogAddrRow(rowAddr);
        auto status = getStatusRow(rowAddr);
        auto isTombstone = getIsTombstoneRow(rowAddr);
        auto request_id = getRequestIdRow(rowAddr);
        std::cout << "\nIn Row: " << rowAddr << std::endl;
        K2LOG_D(log::pbrb, "Timestamp: {}, pAddr: {}, isWriteIntent: {}, isTombstone: {}, request_id: {}", ts, pAddr, status == dto::DataRecord::WriteIntent, isTombstone, request_id);
    }

    void printRowsBySchema(SchemaId sid) {
        SchemaMetaData smd = _schemaMap[sid];
        BufferPage *pagePtr = smd.headPage;
        while (pagePtr) {
            std::cout << "\nIn Page: " << pagePtr << std::endl;
            outputHeader(pagePtr);

            for (uint32_t i = 0; i < smd.maxRowCnt; i++)
                if (isBitmapSet(pagePtr, i)) {
                    std::cout << "Row Offset: " << i << std::endl;
                    printHeaderRow(pagePtr, i);
                    printFieldsRow(pagePtr, i);
                }

            pagePtr = getNextPage(pagePtr);
        }
    }

    void *getAddrByPageAndOffset(uint32_t schemaId, BufferPage *pagePtr, RowOffset offset) {
        //auto sid = getSchemaIDPage(pagePtr);
        assert(_schemaMap.find(schemaId) != _schemaMap.end());
        size_t byteOffsetInPage = _pageHeaderSize + _schemaMap[schemaId].occuBitmapSize + 
                                  _schemaMap[schemaId].rowSize * offset;
        void *address = (void *) ((uint8_t *)pagePtr + byteOffsetInPage);
        return address;
    }

    void printStats() {
        std::cout << std::dec << "Evict Count(s): " << evictCnt << std::endl <<
                     "Split Count(s): " << splitCnt << std::endl;
    }

    uint32_t mapCachedSchema(String schemaName, uint32_t schemaVer){
        for (auto& mapItem : schemaUMap.umap) {
            //K2LOG_I(log::pbrb, "schemaName:{}, name:{}, schemaID:{}, version:{}, field size:{}", schemaName, mapItem.second.name, mapItem.first, mapItem.second.version, mapItem.second.fields.size());
            if(mapItem.second.name == schemaName && mapItem.second.version == schemaVer) {
                return mapItem.first + 1;
            }
        }
        return 0;
    }

    uint32_t addSchemaUMap(SimpleSchema schemaPtr){
        return schemaUMap.addSchema(schemaPtr);
    }

    // Transport Functions:

    // PBRB Row -> SKVRecord
    dto::SKVRecord *generateSKVRecordByRow(uint32_t schemaId, RowAddr rAddr, const String &collName, std::shared_ptr<dto::Schema> schema, bool isPayloadRow, double &totalReadCopyFeildms);
    dto::DataRecord *generateDataRecord(dto::SKVRecord *skvRecord, void *hotAddr);
    // getSchemaVer by hotAddr in SimpleSchema
    uint32_t getSchemaVer(void *hotAddr){
        auto pagePtr = getPageAddr(hotAddr);
        SchemaMetaData &smd = _schemaMap[getSchemaIDPage(pagePtr)];
        return smd.schema->version;
    }

    struct AccStruct {
        String SName;
        BufferPage* addr;
        RowOffset rOff;
        int type;
    };
    std::vector<AccStruct> accessVec;
    void AccessStructAppend(String SName, BufferPage* addr, RowOffset rOff, int type) {
        accessVec.push_back({SName, addr, rOff, type});
    }

    void AccessOutput() {
        std::string _accessFileName = "AccessPattern.csv";
        accessFile.open(_accessFileName);
        for (auto it: accessVec) {
            if (it.type == 0)
                accessFile << it.SName << ",R," << it.addr << "," << it.rOff << std::endl;
            else if (it.type == 1)
                accessFile << it.SName << ",W," << it.addr << "," << it.rOff << std::endl;
        }
        accessFile.close();
    }

    void fcrpOutput()
    {
        ofile.open(_fcrpOutputFileName);
        K2LOG_I(log::pbrb, "======= Output fcrp info =======");
        // fcrp.TotalInPageTime();
        int size = fcrp.idxStep.size();
        ofile << "AccessId,SchemaName,Type,isFound,SearchPageNum,Indexer,Find,FindPrev,FindNext,PageNum,RowNum,TotalTime" << std::endl;
        for (int i = 0; i < size; i++)
        {
            ofile << fcrp.accessId1[i] << ","
                    << fcrp.sname[i] << ","
                    << fcrp.type[i] << ","
                    << fcrp.isFound[i] << ","
                    << fcrp.searchPageNum[i] << ","
                    << fcrp.idxStep[i] << ","
                    << fcrp.findStep[i] << ","
                    << fcrp.prev[i] << ","
                    << fcrp.next[i] << ","
                    // << fcrp.totalInPageTime[i] << ", "
                    << fcrp.pageSize1[i] + 1 << ","
                    << fcrp.rowSize1[i] + 1 << ","
                    << fcrp.idxStep[i] + fcrp.findStep[i] + fcrp.prev[i] + fcrp.next[i]
                    << std::endl;
        }
        ofile.close();

        // ofile.open("fcrp_2.csv");
        // K2LOG_I(log::pbrb, "======= Output fcrp(InPage) info =======");
        // ofile << "AccessId, SchemaName, Type, Time, PageNum, RowNum" << std::endl;
        // int size2 = fcrp.accessIds.size();
        // for (int i = 0; i < size2; i++) {
        //     ofile << fcrp.accessIds[i] << ", "
        //           << fcrp.sname2[i] << ", "
        //           << fcrp.inPageType[i] << ", "
        //           << fcrp.inPageTime[i] << ", "
        //           << fcrp.pageSize2[i] << ", "
        //           << fcrp.rowSize2[i]
        //           << std::endl;
        // }
        // ofile.close();
        K2LOG_I(log::pbrb, "======= Finished output fcrp info =======");

        ofile.open("schema.csv");
        ofile << "SchemaId, SchemaName, RowSize, OccuBitmapSize, MaxRowCnt" << std::endl;
        for (auto &iter : _schemaMap)
        {
            SchemaId id = iter.first;
            auto smd = iter.second;
            ofile << id << ", "
                    << smd.schema->name << ", "
                    << smd.rowSize << ", "
                    << smd.occuBitmapSize << ", "
                    << smd.maxRowCnt << std::endl;
        }

        ofile.close();
        
        ofile.open("LinkedList.txt");
        for (auto &iter : _schemaMap)
        {
            ofile << std::endl;
            auto smd = iter.second;
            ofile << smd.schema->name << std::endl;
            int pageId = 0;
            for (BufferPage *ptr = smd.headPage; ptr!=nullptr; ptr = getNextPage(ptr)) {
                ofile << ptr << " " << pageId++ << std::endl;
            }
        }
        ofile.close();
}

    // Prefetch

    inline void prefetcht2Row(RowAddr rowAddr, size_t size) 
    {
        size_t clsize = sysconf(_SC_LEVEL1_DCACHE_LINESIZE);
        for (size_t off = 0; off < size; off += clsize)
        {
            __builtin_prefetch((uint8_t *)rowAddr + off, 0, 1);
        }
        return;
    }
};

}

#define CAST_APPLY_FIELD_VALUE_WITHOUT_STRING(func, a, ...)                        \
    do {                                                            \
        switch ((a).type) {                                         \
            case k2::dto::FieldType::INT16T: {                      \
                func<int16_t>((a), __VA_ARGS__);                    \
            } break;                                                \
            case k2::dto::FieldType::INT32T: {                      \
                func<int32_t>((a), __VA_ARGS__);                    \
            } break;                                                \
            case k2::dto::FieldType::INT64T: {                      \
                func<int64_t>((a), __VA_ARGS__);                    \
            } break;                                                \
            case k2::dto::FieldType::FLOAT: {                       \
                func<float>((a), __VA_ARGS__);                      \
            } break;                                                \
            case k2::dto::FieldType::DOUBLE: {                      \
                func<double>((a), __VA_ARGS__);                     \
            } break;                                                \
            case k2::dto::FieldType::BOOL: {                        \
                func<bool>((a), __VA_ARGS__);                       \
            } break;                                                \
            case k2::dto::FieldType::DECIMAL64: {                   \
                func<std::decimal::decimal64>((a), __VA_ARGS__);    \
            } break;                                                \
            case k2::dto::FieldType::DECIMAL128: {                  \
                func<std::decimal::decimal128>((a), __VA_ARGS__);   \
            } break;                                                \
            case k2::dto::FieldType::FIELD_TYPE: {                  \
                func<k2::dto::FieldType>((a), __VA_ARGS__);         \
            } break;                                                \
            default:                                                \
                auto msg = fmt::format(                             \
                    "cannot apply field of type {}", (a).type);     \
                throw k2::dto::TypeMismatchException(msg);          \
        }                                                           \
    } while (0)
