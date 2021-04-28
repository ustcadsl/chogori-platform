#include "schema.h"

uint32_t FTSize[256] = {0, 116, 4};

SchemaUMap schemaUMap;

String SimpleSchema::getKey(int id) {
    char buf[256];
    sprintf(buf, "%s_%04d", name.c_str(), id);
    return buf;
}