#include<iostream>

#include <k2/pbrb/pbrb_design.h>
#include <k2/pbrb/indexer.h>

int length = 100;
int timestamp[1000] = {1, 2, 3, 4, 5, 6, 7, 8, 9};
char str[1000][124] = {
    "Test01_01_1\0",
    "Test02_01_2\0",
    "Test03_01_3\0",
    "Test01_02_4\0",
    "Test01_03_5\0",
    "Test02_02_6\0",
    "Test01_04_7\0",
    "Test01_05_8\0",
    "Test03_02_9\0"
};

int uid[1000] = {1, 2, 3, 1, 1, 2, 1, 1, 3};
int ver[1000] = {1, 1, 1, 2, 3, 2, 4, 5, 2};

void initPlog(SimplePlog &Plog1, SchemaId sid) {
    for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 10; j++) {
            int t = i * 10 + j;
            timestamp[t] = t;
            char buf[30];
            sprintf(buf, "Test%04d_%04d_%04d", i, j, t);
            strcpy(str[t], buf);
            uid[t] = j;
            ver[t] = i;
        }
    }

    for (int i = 0; i < length; i++) {
        Plog1.writeSId(sid);
        Plog1.write(&timestamp[i], 4);
        Plog1.write(&uid[i], 4);
        Plog1.write(str[i], 116);
    }
}


// Tests for 100 rows.
int test() {
    Index indexer;
    //int wm = 0;
    k2::dto::Timestamp wm;

    // Plog
    SimplePlog Plog1;
    uint8_t *basePtr = (uint8_t *)Plog1.plog;

    // Schema
    SimpleSchema S1({
        "S001", 0, {
            {k2::dto::FieldType::INT32T, "uid"},
            {k2::dto::FieldType::STRING, "str"}
        }
    });
    auto sid1 = schemaUMap.addSchema(&S1);
    initPlog(Plog1, sid1);

    PBRB pbrb(100, &wm, &indexer);

    auto page1 = pbrb.createCacheForSchema(sid1);
    std::cout << page1 << " " << std::endl;
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < length; i++) {
        std::string newKey = S1.getKey(uid[i]);
        std::cout << "Insert Key: " << newKey << " into indexer." << std::endl;
        
        //*(pbrb.watermark) = i - 40;
        if (indexer.find(newKey) == indexer.end()) {
            KVN kvn;
            indexer.insert({newKey, kvn});
            pbrb.cacheColdRow(basePtr + i * 128, newKey);
        }
        else {
            pbrb.cacheColdRow(basePtr + i * 128, newKey);
        }
    }

    pbrb.printRowsBySchema(sid1);
    pbrb.printStats();
    return 0;
}

int main() {
    test();
    return 0;
}
