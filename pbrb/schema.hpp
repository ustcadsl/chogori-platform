#include <iostream>
#include <cstdlib>
#include <vector>
#include <unordered_map>

using String = std::string;
using SchemaId = uint32_t;
using SchemaVer = uint16_t;

// Field Types
enum FieldType : uint8_t {
    NULL_T = 0,
    STRING,
    INT32T,
};

uint32_t FTSize[256] = {0, 116, 4};

struct SchemaField {
    FieldType type;
    String name;
};

struct SimpleSchema {
    String name;
    uint32_t version = 0;
    std::vector<SchemaField> fields;
};

struct SchemaUMap {
    std::unordered_map<SchemaId, SimpleSchema *> umap; 
    uint32_t currIdx = 0;
    uint32_t addSchema(SimpleSchema *schemaPtr) {
        uint32_t retVal = currIdx;
        umap.insert({currIdx++, schemaPtr});
        return retVal;
    }
} schemaUMap;