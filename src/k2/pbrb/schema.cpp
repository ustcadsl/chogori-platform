#include "schema.h"

/*uint32_t FTSize[256] = {
    0,      // NULL_T = 0,
    116,    // STRING, // NULL characters in string is OK
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

SchemaUMap schemaUMap;

String SimpleSchema::getKey(int id) {
    char buf[256];
    sprintf(buf, "%s_%04d", name.c_str(), id);
    return buf;
}*/