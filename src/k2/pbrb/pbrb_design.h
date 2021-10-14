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
#include <k2/indexer/IndexerInterface.h>

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
    64,    // STRING, // NULL characters in string is OK
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
        K2LOG_I(log::pbrb, "^^^^^^^^^^^^^^^Set HeadPagePtr, pagePtr empty:{}", pagePtr==nullptr);
        std::cout << "Set HeadPagePtr: " << headPage << std::endl;
    }

    // Construct from a Schema
    void setInfo(SchemaId schemaId, uint32_t pageSize, uint32_t pageHeaderSize, uint32_t rowHeaderSize) {
        // read from Schema
        K2LOG_I(log::pbrb, "set smd info: rowHeaderSize: {}", rowHeaderSize);
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

            K2LOG_I(log::pbrb, "Current type: {}, offset: {}, currRowOffset: {}, rowHeaderSize :{}, nullableBitmapSize: {}", schema->fields[i].type, fieldObj.fieldOffset, currRowOffset, rowHeaderSize, nullableBitmapSize);
        }
        
        // set rowSize
        rowSize = currRowOffset;
        setOccuBitmapSize(pageSize);
        maxRowCnt = (pageSize - pageHeaderSize - occuBitmapSize) / rowSize;

        K2LOG_I(log::pbrb, "Add new schema:{}", schema->name);
        std::cout << std::dec << "\nGot Schema: " << schemaId << std::endl 
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

    void setOccuBitmapSize(uint32_t pageSize) {
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

    uint32_t splitCnt = 0, evictCnt = 0;
public:
    //SchemaMetaData tempSmeta;
    //int *watermark;
    k2::dto::Timestamp *watermark;
    //Initialize a PBRB cache
    //PBRB(int maxPageNumber, int *wm, Index *indexer)
    PBRB(int maxPageNumber, k2::dto::Timestamp *wm, Index *indexer)
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
        K2LOG_I(log::pbrb, "\nPBRB: Allocated: {} Pages in", maxPageNumber);
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

    // 1.2 row get & set functions.

    // Row Stuct:
    // CRC (4) | Timestamp (16) | PlogAddr (8) | ...
    
    // CRC:
    uint32_t getCRCRow();
    void setCRCRow();

    // Timestamp: (RowAddr + 4)
    dto::Timestamp getTimestampRow(RowAddr rAddr);
    void setTimestampRow(RowAddr rAddr, dto::Timestamp &ts);
    
    // PlogAddr: (RowAddr + 20)
    void *getPlogAddrRow(RowAddr rAddr);
    void setPlogAddrRow(RowAddr rAddr, void *PlogAddr);

    // Debugging Output Function.
    void printFieldsRow(const BufferPage *pagePtr, RowOffset rowOffset) {
        SchemaMetaData smd = _schemaMap[getSchemaIDPage(pagePtr)];
        char buf[4096];
        size_t rowOffsetInPage = _pageHeaderSize + smd.occuBitmapSize + 
                                 smd.rowSize * rowOffset;
        for (size_t idx = 0; idx < smd.fieldsInfo.size(); idx++) {   
            readFromPage(pagePtr, rowOffsetInPage + smd.fieldsInfo[idx].fieldOffset, 
                rowOffsetInPage + smd.fieldsInfo[idx].fieldSize, buf);
            auto t = smd.schema->fields[idx].type;
            void *valuePtr = nullptr;
            if (t == k2::dto::FieldType::STRING) {
                String *value = new String(buf);
                valuePtr = static_cast<void *>(&value);
            }
            using FieldType = dto::FieldType;
            using TypeMismatchException = dto::TypeMismatchException;
            K2_DTO_CAST_APPLY_FIELD_VALUE(printField, smd.schema->fields[idx], valuePtr, idx, smd.schema->fields[idx].name);
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
        K2LOG_I(log::pbrb, "Timestamp: {}, pAddr: {}", ts, pAddr);
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
        uint8_t bit = (pagePtr->content[_pageHeaderSize + byteIndex] >> offset) & 1;
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

        initializePage(pagePtr);
        setSchemaIDPage(pagePtr, schemaId);
        setSchemaVerPage(pagePtr, schemaVer);

        K2LOG_I(log::pbrb, "createCacheForSchema, schemaId: {}, pagePtr empty:{}, _freePageList size:{}, pageSize: {}, smd.rowSize: {}, _schemaMap[0].rowSize: {}", schemaId, pagePtr==nullptr,  _freePageList.size(), sizeof(BufferPage), smd.rowSize, _schemaMap[0].rowSize);

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
            //memset(newPage, 0, sizeof(BufferPage));
            initializePage(newPage);
            setSchemaIDPage(newPage, schemaId);
            setNextPage(newPage, _schemaMap[schemaId].headPage);

            _schemaMap[schemaId].headPage = newPage;
            _freePageList.pop_front();
            K2LOG_I(log::pbrb, "^^^^^^^^^^^^^^^in AllocNewPageForSchema, _freePageList size:{}", _freePageList.size());
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

    // Copy the header of row from DataRecord of query to (pagePtr, rowOffset)
    void *cacheRowHeaderFrom(BufferPage *pagePtr, RowOffset rowOffset, dto::Timestamp& timestamp, void* PlogAddr);

    // Copy the field of row from DataRecord of query to (pagePtr, rowOffset)
    void *cacheRowFieldFromDataRecord(BufferPage *pagePtr, RowOffset rowOffset, void* field, size_t strSize, uint32_t fieldID);
    
    // find an empty slot between the beginOffset and endOffset in the page
    RowOffset findEmptySlotInPage(BufferPage *pagePtr, RowOffset beginOffset, RowOffset endOffset);

    // find an empty slot in the page
    RowOffset findEmptySlotInPage(BufferPage *pagePtr);

    std::pair<BufferPage *, RowOffset> findCacheRowPosition(uint32_t schemaID);

    // Find the page pointer and row offset to cache cold row
    std::pair<BufferPage *, RowOffset> findCacheRowPosition(uint32_t schemaID, String key);
    
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
    void doBackgroundPBRBGC();

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

    void *getAddrByPageAndOffset(BufferPage *pagePtr, RowOffset offset) {
        auto sid = getSchemaIDPage(pagePtr);
        assert(_schemaMap.find(sid) != _schemaMap.end());
        size_t byteOffsetInPage = _pageHeaderSize + _schemaMap[sid].occuBitmapSize + 
                                  _schemaMap[sid].rowSize * offset;
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
    dto::SKVRecord *generateSKVRecordByRow(RowAddr rAddr, const String &collName, std::shared_ptr<dto::Schema> schema);
    dto::DataRecord *generateDataRecord(dto::SKVRecord *skvRecord, KeyValueNode &node, int order, void *hotAddr);
    // getSchemaVer by hotAddr in SimpleSchema
    uint32_t getSchemaVer(void *hotAddr) {
        auto pagePtr = getPageAddr(hotAddr);
        SchemaMetaData &smd = _schemaMap[getSchemaIDPage(pagePtr)];
        return smd.schema->version;
    }
};

}