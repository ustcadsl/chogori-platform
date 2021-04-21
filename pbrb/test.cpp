#include <iostream>
#include "pbrb_design.hpp"

char str[3][124] = {
    "Test01\0",
    "Test02\0",
    "Test03\0"
};

int id[3] = {1, 2, 3};

int main() {
    std::cout << "hello world" << std::endl;
    SimplePlog Plog1;
    SimpleSchema S1({
        "test1", 0, {
            {INT32T, "id"}, {STRING, "str"}
        }
    });
    auto sid1 = schemaUMap.addSchema(&S1);

    for (int i = 0; i < 3; i++) {
        Plog1.writeSId(sid1);
        Plog1.write(&id[i], 4);
        Plog1.write(str[i], 120);
    }

    uint8_t *basePtr = (uint8_t *)Plog1.plog;

    std::cout << "3rd row of plog: "
              << "\t\nSchemaId: " << readFromPlog<uint32_t>(basePtr + 3 * 128, 4)
              << "\t\nid" << readFromPlog<uint32_t>(basePtr + 3 * 128 + 4, 4)
              << std::endl;

    PBRB pbrb(8);
    // auto ptr = pbrb.createCacheForSchema(sid1);
    // pbrb.outputHeader(ptr);

    std::cout << "Cache Row 2 in Plog" << pbrb.cacheColdRow((PLogAddr) (basePtr + 2 * 128));
    return 0;
}

