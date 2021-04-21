#include "pbrb_design.hpp"

void PBRB::cacheRowFromPlog(BufferPage *pagePtr, uint16_t rowOffset, SKVRecord record, PLogAddr pAddress)
{

    // offset of the first row in the page = 64 + 64 = 128
    // CRC32 is 4 bytes
    writeToPage<uint16_t>(pagePtr, 128 + rowOffset * rowSize, &(record.getCRC32()), 4);
    //RowVersion is 16 bytes
    writeToPage<uint16_t>(pagePtr, 128 + rowOffset * rowSize + 4, &(record.getRowVersion()), 16);
    //Location of cold row is 8 bytes
    writeToPage<uint16_t>(pagePtr, 128 + rowOffset * rowSize + 20, &(pAddress), 8);

    uint16_t fieldNumber = record.getFieldNumber();
    for (uint16_t i = 0; i < fieldNumber; i++)
    {
        if (record.isNullField(i))
        { //the i-th field is null, so set the bitmap for nullable fields
            uint16_t byteIndex = i / 8;
            uint16_t Offset = i % 8;
            pagePtr->content[128 + rowOffset * rowSize + 28 + byteIndex] |= 0x1 << Offset;
        }
    }
    //How many bytes of the bitmap?? = fieldNumber/8 + 1 ??
    uint16_t curOffset = 128 + rowOffset * rowSize + 28 + fieldNumber / 8 + 1;
    //set the content of each field, All fields are known fixed length, so the row size is fixed
    for (uint16_t i = 0; i < fieldNumber; i++)
    {
        if (record.isNullField(i))
        { //the i-th field is null
            //if it is a in-place nullable fields, the actually field space is still reserved/occupied but no data ??
            curOffset += record.getFixedFieldSize(i); //assume that each field has a fixed size???
        }
        else
        {
            uint16_t fieldLength = record.getFieldLength(i);
            if (fieldLength > 255)
            {                                                          //keep variable length field that exceeds 255 bytes in the heap (out of place)
                heapAddr = HeapForPBRB.add(record.getFieldContent(i)); //???
                writeToPage<uint16_t>(pagePtr, curOffset, &(heapAddr), 8);
                curOffset += record.getFixedFieldSize(i); // or 8 ??
            }
            else
            { //keep variable length field in place
                writeToPage<uint16_t>(pagePtr, curOffset, &(record.getFieldContent(i)), fieldLength);
                curOffset += record.getFixedFieldSize(i); // or fieldLength??
            }
        }
    }
}


//find an empty slot between the beginOffset and endOffset in the page
uint32_t PBRB::findEmptySlot(BufferPage *pagePtr, uint16_t beginOffset, uint16_t endOffset)
{
    uint32_t schemaId = getSchemaIDPage(pagePtr);
    
    uint32_t maxRowCnt = schemaMap[schemaId].maxRowCnt;
    
    if (endOffset > maxRowCnt)
        return 65534;

    for (uint16_t i = beginOffset + 1; i < endOffset; i++)
    {
        uint32_t byteIndex = i / 8;
        uint8_t curByte = readFromPage<uint8_t>(pagePtr, _pageHeaderSize + byteIndex, 1);
        
        uint32_t Offset = i % 8;
        curByte = curByte >> Offset;
        uint8_t bitValue = curByte & (uint8_t) 0x1;
        if (bitValue == 0)
            return i; //find an empty slot, return the offset
    }
    return 65535; //not find an empty slot
}

//find an empty slot in the page
uint32_t PBRB::findEmptySlot(BufferPage *pagePtr)
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
    return ~0; //not find an empty slot
}

BufferPage *PBRB::findEmptyRow(uint32_t schemaID, String rowKey, uint16_t *rowOffset)
{
    //first search the index to find the keyValueNode that maintains the row
    keyValueNode *kvNode = indexer.findKVNode(rowKey);
    if (kvNode->hasCachedVersion())
    { //1. If there are already cached hot row version(s) for this row
        //1.1)If all versions of the row in KeyValueNode are hot and cached
        if (kvNode->hotVersionNumber() == kvNode->totalVersionNumber())
        {
            //evict the version with the oldest timestamp
            Row *oldestRow = kvNode.getOldestVerAddr();           //get row address of the oldest version
            BufferPage *pagePtr = oldestRow & 0x1111111111110000; //get page address
            *rowOffset = ((int)((void *) oldestRow - (void *) pagePtr) - headerSize - bitmapSize))/rowSize;
            return pagePtr;
        }
        //1.2) if any hot versions that are no longer in data retention window
        else if (kvNode->hasExpiredVersion())
        {
            //evict the version that exceeds data retention window
            Row *expiredRow = kvNode->getExpiredVersion();         //get address of expired row(s)
            BufferPage *pagePtr = expiredRow & 0x1111111111110000; //get page address
            *rowOffset = ((int)((void *) expiredRow - (void *) pagePtr) - headerSize - bitmapSize))/rowSize;
            return pagePtr;
        }
        //1.3) If there are hot version(s) and cold version(s) in the keyValueNode??????
        else
        {
            BufferPage *pagePtr = kvNode->getPageOfLatestHotRow(); //get page address of hot row
            *rowoffset = findEmptySlot(pagePtr);                   //find an empty slot in the page
            return pagePtr;
        }
    }
    else
    {   //2. If it is the first version entry that becomes hot
        //the location for this row can be set based on its neighboring row location
        Row *previousRow = indexer.getPreviousHotKVNode(kvNode);
        Row *nextRow = indexer.getNextHotKVNode(kvNode);
        BufferPage *prevPage = previousRow & 0x1111111111110000;
        BufferPage *nextPage = nextRow & 0x1111111111110000;
        //2.1) if these two addresses are the same page, then pick a empty slot from the same page
        if (prevPage == nextPage)
        {
            prevRowOffset = ((int) ((void *)previousRow - (void *)prevPage) - headerSize - bitmapSize))/rowSize;
            nextRowOffset = ((int) ((void *)nextRow - (void *)nextPage) - headerSize - bitmapSize))/rowSize;
            //try to pick a free slot between previous and next neighbor row in the page
            *rowOffset = findEmptySlot(prevPage, prevRowOffset, nextRowOffset);
            return prevPage;
        }
        //2.2) If these two pages are different, pick an empty slot from the page with lower occupancy rate
        else
        {
            uint16_t prevNum = getHotRowsNumPage(prevPage);
            uint16_t nextNum = getHotRowsNumPage(nextPage);
            if (prevNum > nextNum)
            { // nextPage has lower occupancy rate
                //pick a slot between the beginning of nextPage and the next row
                *rowOffset = findEmptySlot(nextPage, 0, nextRowOffset);
                return nextPage;
            }
            else
            { // prevPage has lower occupancy rate
                //pick a slot between the previous row and the end of prevPage
                *rowOffset = findEmptySlot(prevPage, prevRowOffset, prevPage->getMaxRowNum());
                return prevPage;
            }
        }
    }
}

//store hot row in the empty row
void PBRB::cacheHotRow(uint32_t schemaID, SKVRecord hotRecord)
{
    uint16_t rowOffset;
    //find a empty row in a page, and return the page address and offset in the page
    BufferPage *pagePtr = findEmptyRow(schemaID, hotRecord.getKey(), &rowOffset);
    setRowPage(pagePtr, rowOffset, hotRecord);
    setRowBitMapPage(pagePtr, rowOffset);
    BufferPage *hotRowAddr = (void *)pagePtr + headerSize + bitmapSize + rowOffset * rowSize; //the address of the hot row
    indexer.insertHotVersion(hotRecord.getKey(), hotRowAddr);
}

//mark the row as unoccupied when evicting a hot row
void PBRB::removeHotRow(BufferPage *pagePtr, int offset)
{
    clearRowBitMapPage(pagePtr, offset);
}

int main()
{
    auto aligned_val = std::align_val_t{65536};
    std::byte *ptr = static_cast<std::byte *>(operator new(sizeof(std::byte), aligned_val));
    //memset(ptr,0,sizeof(std::byte)*65536);
    //auto ptr = operator new(sizeof(std::byte), aligned_val);
    //memcpy(ptr, ,2);
    ptr[0] = (std::byte)(0xff & 22);
    ptr[1] = (std::byte)((0xff00 & 22) >> 8);
    ptr[2] = (std::byte)46;
    std::cout << ptr << "," << (int)ptr[0] << "," << (int)ptr[1] << std::endl;
    std::cout << ptr + 2 << (int)ptr[2] << std::endl;
    return 0;
}