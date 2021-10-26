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
dto::SKVRecord *PBRB::generateSKVRecordByRow(RowAddr rAddr, const String &collName, std::shared_ptr<dto::Schema> schema) {

    dto::SKVRecord *record = new dto::SKVRecord(collName, schema);
    // read smd to get fields info.
    auto retVal = findRowByAddr(rAddr);
    BufferPage *pagePtr = retVal.first;
    RowOffset rowOffset = retVal.second;

    K2LOG_I(log::pbrb, "Row: {} (Page: {}, Offset: {})", rAddr, (void *)pagePtr, rowOffset);

    // const reference
    SchemaId sid = getSchemaIDPage(pagePtr);
    assert(_schemaMap.find(sid) != _schemaMap.end());
    const SchemaMetaData &smd = _schemaMap[sid];
    
    // Copy Fields.
    assert(smd.schema->fields.size() == smd.fieldsInfo.size() && smd.fieldsInfo.size() > 0);
    
    K2LOG_D(log::pbrb, "field(s) count: {}", smd.schema->fields.size());
    for (uint32_t idx = 0; idx < smd.schema->fields.size(); idx++) {
        // TODO: add nullable fields.
        auto fieldOffset = smd.fieldsInfo[idx].fieldOffset;
        RowAddr srcAddr = static_cast<RowAddr>((uint8_t *)rAddr + fieldOffset);
        auto field = smd.schema->fields[idx];

        if (field.type == k2::dto::FieldType::STRING) {
            char *cstr = (char *)(srcAddr);
            String str(cstr);
            // -----! NOTICE: size need -1 here. !-----
            K2LOG_D(log::pbrb, "Copy Field {}: (Type: STRING, FieldOffset: {}, value: {}, size: {}", idx, smd.fieldsInfo[idx].fieldOffset, str, str.size());
            // Serialize into record
            record->serializeNext<String>(str);
        }
        else {
            bool success;
            CAST_APPLY_FIELD_VALUE_WITHOUT_STRING(_copyFieldFromHotRow, field, srcAddr, record, success);
            if (!success)
                K2LOG_W(log::pbrb, "unable to serialize field");
        }
    }

    return record;
}



dto::DataRecord *PBRB::generateDataRecord(dto::SKVRecord *skvRecord, void *hotAddr) {
    // validate
    K2EXPECT(log::pbrb, skvRecord != nullptr, true);
    dto::DataRecord *coldVer = static_cast<dto::DataRecord *>(getPlogAddrRow(hotAddr));
    dto::DataRecord *prevVer = coldVer->prevVersion;
    dto::DataRecord *record = new dto::DataRecord {
        .value=std::move(skvRecord->storage), 
        .isTombstone=getIsTombstoneRow(hotAddr), 
        .timestamp=getTimestampRow(hotAddr),
        .prevVersion=prevVer, 
        .status=getStatusRow(hotAddr), 
        // TODO: update request_id
        .request_id=getRequestIdRow(hotAddr)
    };
    K2LOG_D(log::pbrb, "Generated Datarecord: [timestamp = {}, status = {}, value = {}]", record->timestamp, record->status, record->value);
    return record;
}

}