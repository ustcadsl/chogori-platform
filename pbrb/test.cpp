#include<iostream>

#include "pbrb_design.h"
#include "indexer.h"

int length = 9;
int timestamp[100] = {1, 2, 3, 4, 5, 6, 7, 8, 9};
char str[100][124] = {
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

int uid[100] = {1, 2, 3, 1, 1, 2, 1, 1, 2};
int ver[100] = {1, 1, 1, 2, 3, 2, 4, 5, 2};

std::string makeKey(std::string schemaName, int uid) {
    const char *sname = schemaName.c_str();
    char str[256];
    sprintf(str, "%s_%04d", sname, uid);
    return str;
}

void initPlog(SimplePlog &Plog1, SchemaId sid) {
    for (int i = 0; i < length; i++) {
        Plog1.writeSId(sid);
        Plog1.write(&timestamp[i], 4);
        Plog1.write(&uid[i], 4);
        Plog1.write(str[i], 116);
    }
}


// Tests for findEmptyRow.
int test1() {
    Index indexer;
    int wm = 0;

    // Plog
    SimplePlog Plog1;
    uint8_t *basePtr = (uint8_t *)Plog1.plog;

    // Schema
    SimpleSchema S1({
        "S001", 0, {
            {INT32T, "uid"},
            {STRING, "str"}
        }
    });
    auto sid1 = schemaUMap.addSchema(&S1);
    initPlog(Plog1, sid1);

    PBRB pbrb(8, &wm, &indexer);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < 3; i++) {
        std::string newKey = makeKey(S1.name, uid[i]);
        std::cout << "Insert Key: " << newKey << " into indexer." << std::endl;
        
        if (indexer.find(newKey) == indexer.end()) {
            KVN kvn;
            kvn.insertRow(basePtr + i * 128, timestamp[i], false, &pbrb);
            indexer.insert({newKey, kvn});
        }
        else {
            KVN &kvn = indexer[newKey];
            kvn.insertRow(basePtr + i * 128, timestamp[i], false, &pbrb);
        }
    }

    std::string testKey1("S001_0001");
    std::string testKey2("S001_0002");
    std::string testKey3("S001_0003");

    auto hot1 = pbrb.cacheRowFromPlog(page1, 2, indexer[testKey1].addr[0]);
    indexer[testKey1].insertRow(hot1, pbrb.getTimestamp(hot1), true, &pbrb);
    auto hot3 = pbrb.cacheRowFromPlog(page1, 5, indexer[testKey3].addr[0]);
    indexer[testKey3].insertRow(hot3, pbrb.getTimestamp(hot3), true, &pbrb);
    pbrb.cacheColdRow(basePtr + 5 * 128, testKey2);
    std::cout << "\n================ " << __func__ << " ================" << std::endl;
    pbrb.printRowsBySchema(sid1);

    return 0;
}

int test2() {
    Index indexer;
    int wm = 0;

    // Plog
    SimplePlog Plog1;
    uint8_t *basePtr = (uint8_t *)Plog1.plog;

    // Schema
    SimpleSchema S1({
        "S001", 0, {
            {INT32T, "uid"},
            {STRING, "str"}
        }
    });
    auto sid1 = schemaUMap.addSchema(&S1);
    initPlog(Plog1, sid1);

    PBRB pbrb(8, &wm, &indexer);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < 4; i++) {
        std::string newKey = makeKey(S1.name, uid[i]);
        std::cout << "Insert Key: " << newKey << " into indexer." << std::endl;
        
        if (indexer.find(newKey) == indexer.end()) {
            KVN kvn;
            kvn.insertRow(basePtr + i * 128, timestamp[i], false, &pbrb);
            indexer.insert({newKey, kvn});
        }
        else {
            KVN &kvn = indexer[newKey];
            kvn.insertRow(basePtr + i * 128, timestamp[i], false, &pbrb);
        }
    }

    std::string testKey1("S001_0001");
    std::string testKey2("S001_0002");
    std::string testKey3("S001_0003");

    auto hot1 = pbrb.cacheRowFromPlog(page1, 1, indexer[testKey1].addr[1]);
    indexer[testKey1].insertRow(hot1, pbrb.getTimestamp(hot1), true, &pbrb);
    auto hot2 = pbrb.cacheRowFromPlog(page1, 2, indexer[testKey1].addr[0]);
    indexer[testKey1].insertRow(hot2, pbrb.getTimestamp(hot2), true, &pbrb);
    auto hot3 = pbrb.cacheRowFromPlog(page2, 4, indexer[testKey3].addr[0]);
    indexer[testKey3].insertRow(hot3, pbrb.getTimestamp(hot3), true, &pbrb);
    pbrb.cacheColdRow(basePtr + 5 * 128, testKey2);
    std::cout << "\n================ " << __func__ << " ================" << std::endl;
    pbrb.printRowsBySchema(sid1);
    return 0;
}

int test3() {
    Index indexer;
    int wm = 0;

    // Plog
    SimplePlog Plog1;
    uint8_t *basePtr = (uint8_t *)Plog1.plog;

    // Schema
    SimpleSchema S1({
        "S001", 0, {
            {INT32T, "uid"},
            {STRING, "str"}
        }
    });
    auto sid1 = schemaUMap.addSchema(&S1);
    initPlog(Plog1, sid1);

    PBRB pbrb(8, &wm, &indexer);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < 4; i++) {
        std::string newKey = makeKey(S1.name, uid[i]);
        std::cout << "Insert Key: " << newKey << " into indexer." << std::endl;
        
        if (indexer.find(newKey) == indexer.end()) {
            KVN kvn;
            kvn.insertRow(basePtr + i * 128, timestamp[i], false, &pbrb);
            indexer.insert({newKey, kvn});
        }
        else {
            KVN &kvn = indexer[newKey];
            kvn.insertRow(basePtr + i * 128, timestamp[i], false, &pbrb);
        }
    }

    std::string testKey1("S001_0001");
    std::string testKey2("S001_0002");
    std::string testKey3("S001_0003");

    auto hot1 = pbrb.cacheRowFromPlog(page2, 1, indexer[testKey1].addr[1]);
    indexer[testKey1].insertRow(hot1, pbrb.getTimestamp(hot1), true, &pbrb);
    auto hot2 = pbrb.cacheRowFromPlog(page1, 2, indexer[testKey1].addr[0]);
    indexer[testKey1].insertRow(hot2, pbrb.getTimestamp(hot2), true, &pbrb);
    auto hot3 = pbrb.cacheRowFromPlog(page2, 4, indexer[testKey3].addr[0]);
    indexer[testKey3].insertRow(hot3, pbrb.getTimestamp(hot3), true, &pbrb);
    pbrb.cacheColdRow(basePtr + 5 * 128, testKey2);
    std::cout << "\n================ " << __func__ << " ================" << std::endl;
    pbrb.printRowsBySchema(sid1);
    return 0;
}

int test4() {
    Index indexer;
    int wm = 0;

    // Plog
    SimplePlog Plog1;
    uint8_t *basePtr = (uint8_t *)Plog1.plog;

    // Schema
    SimpleSchema S1({
        "S001", 0, {
            {INT32T, "uid"},
            {STRING, "str"}
        }
    });
    auto sid1 = schemaUMap.addSchema(&S1);
    initPlog(Plog1, sid1);

    PBRB pbrb(8, &wm, &indexer);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < 4; i++) {
        std::string newKey = makeKey(S1.name, uid[i]);
        std::cout << "Insert Key: " << newKey << " into indexer." << std::endl;
        
        if (indexer.find(newKey) == indexer.end()) {
            KVN kvn;
            kvn.insertRow(basePtr + i * 128, timestamp[i], false, &pbrb);
            indexer.insert({newKey, kvn});
        }
        else {
            KVN &kvn = indexer[newKey];
            kvn.insertRow(basePtr + i * 128, timestamp[i], false, &pbrb);
        }
    }

    std::string testKey1("S001_0001");
    std::string testKey2("S001_0002");
    std::string testKey3("S001_0003");
    
    pbrb.cacheColdRow(indexer[testKey1].findColdAddrByTs(4), testKey1); // cache 4

    pbrb.cacheColdRow(indexer[testKey1].findColdAddrByTs(1), testKey1); // cache 1

    pbrb.cacheColdRow(basePtr + 4 * 128, testKey1); // cache 5

    pbrb.cacheColdRow(basePtr + 5 * 128, testKey2); // cache 6
    pbrb.printRowsBySchema(sid1);

    pbrb.cacheColdRow(basePtr + 6 * 128, testKey1); // evict 1; cache 7
    pbrb.printRowsBySchema(sid1);
    wm = 7;

    pbrb.cacheColdRow(basePtr + 7 * 128, testKey1); // evict 4, 5; cache 8

    std::cout << "\n================ " << __func__ << " ================" << std::endl;
    pbrb.printRowsBySchema(sid1);
    return 0;
}

int main() {
    test1();
    test2();
    test3();
    test4();
    return 0;
}
