#include<iostream>

#include <k2/pbrb/pbrb_design.h>
#include <k2/pbrb/indexer.h>

int length = 1000;
int timestamp[1005];
int uid[1005];
int ver[1005];
char str[1005][124];


void initPlog(SimplePlog &Plog1, SchemaId sid) {
    for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 100; j++) {
            int t = i * 100 + j;
            timestamp[t] = t;
            char buf[50];
            sprintf(buf, "Test%04d_%04d_%04d", j, i, t);
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
    auto sid1 = schemaUMap.addSchema(S1);
    initPlog(Plog1, sid1);

    //PBRB pbrb(100, &wm, &indexer);
    PBRB pbrb(100, &wm);

    auto page1 = pbrb.createCacheForSchema(sid1);
    std::cout << page1 << " " << std::endl;
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < length; i++) {
        std::string newKey = S1.getKey(uid[i]);
        std::cout << std::dec << "idx: " << i << std::endl;
        std::cout << "value: " << str[i] << std::endl;
        *(pbrb.watermark) = -1;
        
        //*(pbrb.watermark) = i - 40;
        if (indexer.find(newKey) == indexer.end()) {
            std::cout << "Insert Key: " << newKey << " into indexer." << std::endl;
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
