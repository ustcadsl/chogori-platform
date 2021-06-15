#include <iostream>
#include <cstdlib>
#include <vector>
#include <cstring>
#include <string>
#include <unordered_map>

#include <k2/dto/ControlPlaneOracle.h>

using String = std::string;
using SchemaId = uint32_t;
using SchemaVer = uint16_t;

// Field Types

struct SchemaField {
    k2::dto::FieldType type;
    String name;
};

struct SimpleSchema {
    String name;
    uint32_t version = 0;
    std::vector<SchemaField> fields;
    String getKey(int id);
};

struct SchemaUMap {
    std::unordered_map<SchemaId, SimpleSchema *> umap; 
    uint32_t currIdx = 0;
    uint32_t addSchema(SimpleSchema *schemaPtr) {
        uint32_t retVal = currIdx;
        umap.insert({currIdx++, schemaPtr});
        return retVal;
    }
};