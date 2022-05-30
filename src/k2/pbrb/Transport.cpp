#include "pbrb_design.h"
#include <k2/dto/K23SI.h>

namespace k2 {

template <typename T>
void _copyFieldFromHotRow(const dto::SchemaField& field, void *srcAddr, dto::SKVRecord *skvRec, bool& success) {
    (void) field;
    T value{};
    auto ft = field.type;
    if (ft != dto::FieldType::NULL_T && ft != dto::FieldType::STRING &&
        ft != dto::FieldType::FIELD_TYPE && ft != dto::FieldType::NOT_KNOWN && ft != dto::FieldType::NULL_LAST) {
        memcpy(&value, srcAddr, FTSize[static_cast<uint8_t>(field.type)]);
        K2LOG_D(log::pbrb, "get value: {}", value);
        skvRec->serializeNext<T>(value);
        success = true;
    }
    else
        success = false;
}

// PBRB Row -> SKVRecord.
dto::SKVRecord *PBRB::generateSKVRecordByRow(uint32_t schemaId, void *rAddr, const String &collName, std::shared_ptr<dto::Schema> schema, bool isPayloadRow, uint64_t &totalReadCopyFeildms) {

    dto::SKVRecord *record = new dto::SKVRecord(collName, schema);
    //assert(_schemaMap.find(schemaId) != _schemaMap.end());
    const SchemaMetaData &smd = _schemaMap[schemaId];
    
    if(!isPayloadRow){//////
        // Copy Fields.
        assert(smd.schema->fields.size() == smd.fieldsInfo.size() && smd.fieldsInfo.size() > 0);
        //K2LOG_D(log::pbrb, "field(s) count: {}", smd.schema->fields.size());
        for (uint32_t idx = 0; idx < smd.schema->fields.size(); idx++) {
            auto _copyFieldStart = k2::now_nsec_count();
            // TODO: add nullable fields.
            size_t fieldOffset = smd.fieldsInfo[idx].fieldOffset;
            void *srcAddr = (void *)((uint8_t *)rAddr + fieldOffset);
            dto::SchemaField& field = smd.schema->fields[idx];

            if (field.type == dto::FieldType::STRING) {
                size_t maxStrSize = smd.fieldsInfo[idx].fieldSize;
                size_t RowFieldSize;
                memcpy(&RowFieldSize, srcAddr, sizeof(size_t));
                srcAddr = (void *)((uint8_t *)rAddr + fieldOffset + sizeof(size_t));
                char *cstr = (char *)(srcAddr);
                if(RowFieldSize + sizeof(size_t) <= maxStrSize) {
                    String str(cstr, RowFieldSize);
                    // -----! NOTICE: size need -1 here. !-----
                    // Serialize into record
                    record->serializeNext<String>(str);
                } else {
                    //large field whose size exceeds the field size
                    size_t valueSegSize = maxStrSize-sizeof(size_t)-sizeof(void *);
                    String str;
                    str.append(cstr, valueSegSize);
                    void *heapAddr = *((void **)((uint8_t *)rAddr + fieldOffset + sizeof(size_t) + valueSegSize));
                    size_t restValueSize = RowFieldSize - valueSegSize;
                    K2LOG_D(log::pbrb, "@@@RowFieldSize:{}, heapAddr:{}, restValueSize:{}", RowFieldSize, (void *)(uint8_t *)heapAddr, restValueSize);
                    str.append((char *)(heapAddr), restValueSize);
                    // -----! NOTICE: size need -1 here. !-----
                    K2LOG_D(log::pbrb, "--Copy Field {}: (Type: STRING, FieldOffset: {}, FieldSize:{}, RowFieldSize: {}, size: {}", idx, smd.fieldsInfo[idx].fieldOffset, smd.fieldsInfo[idx].fieldSize, RowFieldSize, str.size());
                    record->serializeNext<String>(str);
                }
            }
            else {
                bool success;
                CAST_APPLY_FIELD_VALUE_WITHOUT_STRING(_copyFieldFromHotRow, field, srcAddr, record, success);
                if (!success)
                    K2LOG_W(log::pbrb, "unable to serialize field");
            }

            auto _copyFieldEnd = k2::now_nsec_count();
            totalReadCopyFeildms += _copyFieldEnd - _copyFieldStart;
        }

    } else {/////
        auto _copyFieldStart = k2::now_nsec_count();
        void* srcAddr = (void*) ((uint8_t *)rAddr + smd.fieldsInfo[0].fieldOffset); //row header offset
        size_t payloadRowSize;
        record->storage.fieldData.seek(0);
        memcpy(&payloadRowSize, srcAddr, sizeof(size_t));
        srcAddr = (void*) ((uint8_t *)rAddr + smd.fieldsInfo[0].fieldOffset + sizeof(size_t));
        if ( payloadRowSize + sizeof(size_t) > smd.rowSize - smd.fieldsInfo[0].fieldOffset ) {
            //large Payload whose size exceeds the row size
            size_t inPlaceSize = smd.rowSize - smd.fieldsInfo[0].fieldOffset - sizeof(size_t) - sizeof(void *);
            record->setStorage(srcAddr, inPlaceSize);
            void *heapAddr = *((void **)((uint8_t *)rAddr + smd.rowSize - sizeof(void *) ));
            size_t outPlaceSize = payloadRowSize - inPlaceSize;
            record->setStorage(heapAddr, outPlaceSize);
            K2LOG_D(log::pbrb, "@@@payloadRowSize:{}, heapAddr:{}, outPlaceSize:{}", payloadRowSize, (void *)(uint8_t *)heapAddr, outPlaceSize);
        } else {
            //K2LOG_I(log::pbrb, "####payloadRowSize:{}, srcAddr:{}", payloadRowSize, (void *)(uint8_t *)srcAddr);
            //memcpy((void*)&(record->getStorage().fieldData), srcAddr, payloadRowSize);
            record->setStorage(srcAddr, payloadRowSize);
        }
        auto _copyFieldEnd = k2::now_nsec_count();
        totalReadCopyFeildms += _copyFieldEnd - _copyFieldStart;
    }

    return record;
}

void PBRB::generateSKVRecordByPayloadRow(uint32_t schemaId, void *rAddr, bool isPayloadRow, Payload& rowData, uint64_t &totalReadCopyFeildms) {
    //dto::SKVRecord *record = new dto::SKVRecord(collName, schema);
    const SchemaMetaData &smd = _schemaMap[schemaId];
    
    if(isPayloadRow){//////
        auto _copyFieldStart = k2::now_nsec_count();
        /*void* srcAddr = (void*) ((uint8_t *)rAddr + smd.fieldsInfo[0].fieldOffset); //row header offset
        size_t payloadRowSize;
        //record->storage.fieldData.seek(0);
        memcpy(&payloadRowSize, srcAddr, sizeof(size_t));
        srcAddr = (void*) ((uint8_t *)rAddr + smd.fieldsInfo[0].fieldOffset + sizeof(size_t));
        if ( payloadRowSize + sizeof(size_t) > smd.rowSize - smd.fieldsInfo[0].fieldOffset ) {
            //large Payload whose size exceeds the row size
            size_t inPlaceSize = smd.rowSize - smd.fieldsInfo[0].fieldOffset - sizeof(size_t) - sizeof(void *);
            //record->setStorage(srcAddr, inPlaceSize);
            rowData.write(srcAddr, inPlaceSize);
            void *heapAddr = *((void **)((uint8_t *)rAddr + smd.rowSize - sizeof(void *) ));
            size_t outPlaceSize = payloadRowSize - inPlaceSize;
            //record->setStorage(heapAddr, outPlaceSize);
            rowData.write(heapAddr, outPlaceSize);
            K2LOG_D(log::pbrb, "@@@payloadRowSize:{}, heapAddr:{}, outPlaceSize:{}", payloadRowSize, (void *)(uint8_t *)heapAddr, outPlaceSize);
        } else {
            //K2LOG_I(log::pbrb, "####payloadRowSize:{}, srcAddr:{}", payloadRowSize, (void *)(uint8_t *)srcAddr);
            //memcpy((void*)&(record->getStorage().fieldData), srcAddr, payloadRowSize);
            //record->setStorage(srcAddr, payloadRowSize);
            rowData.write(srcAddr, payloadRowSize);
        }
        */
        
        void* srcAddr = (void*) ((uint8_t *)rAddr + smd.fieldsInfo[0].fieldOffset + sizeof(size_t));
        size_t inPlaceSize = smd.rowSize - smd.fieldsInfo[0].fieldOffset - sizeof(size_t) - sizeof(void *);
        rowData.write(srcAddr, inPlaceSize);
        auto _copyFieldEnd = k2::now_nsec_count();
        totalReadCopyFeildms += _copyFieldEnd - _copyFieldStart;
    }
}

dto::DataRecord *PBRB::generateDataRecord(dto::SKVRecord *skvRecord, void *hotAddr) {
    // validate
    K2EXPECT(log::pbrb, skvRecord != nullptr, true);
    dto::DataRecord *coldVer = static_cast<dto::DataRecord *>(getPlogAddrRow(hotAddr));
    /*dto::DataRecord *prevVer = coldVer->prevVersion;
    dto::DataRecord *record = new dto::DataRecord {
        .value=std::move(skvRecord->storage), 
        .isTombstone=getIsTombstoneRow(hotAddr), 
        .timestamp=getTimestampRow(hotAddr),
        .prevVersion=prevVer, 
        .status=getStatusRow(hotAddr), 
        // TODO: update request_id
        .request_id=getRequestIdRow(hotAddr)
    };
    //K2LOG_D(log::pbrb, "Generated Datarecord: [timestamp = {}, status = {}, value = {}]", record->timestamp, record->status, record->value);
    return record;
    */
    coldVer->value=std::move(skvRecord->storage);
    return coldVer;
}

dto::DataRecord *PBRB::generateDataRecord(void *hotAddr) {
    // validate
    dto::DataRecord *coldVer = static_cast<dto::DataRecord *>(getPlogAddrRow(hotAddr));
    //coldVer->value=std::move(skvRecord->storage);
    return coldVer;
}

}