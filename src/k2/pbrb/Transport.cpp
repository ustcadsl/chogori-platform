#include "pbrb_design.h"
#include <k2/dto/K23SI.h>

namespace k2 {

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
        auto ft = smd.schema->fields[idx].type;
        if (ft == k2::dto::FieldType::STRING) {
            auto fieldOffset = smd.fieldsInfo[idx].fieldOffset;
            char *cstr = (char *)((uint8_t *)rAddr + fieldOffset);
            String str(cstr);
            // -----! NOTICE: size need -1 here. !-----
            K2LOG_D(log::pbrb, "Copy Field {}: (Type: STRING, FieldOffset: {}, value: {}, size: {}", idx, smd.fieldsInfo[idx].fieldOffset, str, str.size());
            // Serialize into record
            record->serializeNext<String>(str);
        }
    }

    return record;
}



dto::DataRecord *PBRB::generateDataRecord(dto::SKVRecord *skvRecord, KeyValueNode *node, int order, void *hotAddr) {
    // validate
    K2EXPECT(log::pbrb, skvRecord != nullptr, true);
    dto::DataRecord *record = new dto::DataRecord {
        .value=std::move(skvRecord->storage), 
        .isTombstone=node->is_tombstone(order), 
        .timestamp=getTimestampRow(hotAddr),
        .prevVersion=nullptr, 
        .status=node->is_writeintent() ? dto::DataRecord::WriteIntent : dto::DataRecord::Committed, 
        // TODO: update request_id
        .request_id=0
    };
    K2LOG_D(log::pbrb, "Generated Datarecord: [timestamp = {}, status = {}, value = {}]", record->timestamp, record->status, record->value);
    return record;
}

}