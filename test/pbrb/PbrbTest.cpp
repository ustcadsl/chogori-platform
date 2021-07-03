#include<iostream>

#include <k2/pbrb/pbrb_design.h>
#include <k2/pbrb/indexer.h>

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

int uid[100] = {1, 2, 3, 1, 1, 2, 1, 1, 3};
int ver[100] = {1, 1, 1, 2, 3, 2, 4, 5, 2};

void initPlog(SimplePlog &Plog1, SchemaId sid) {
    for (int i = 0; i < length; i++) {
        Plog1.writeSId(sid);
        Plog1.write(&timestamp[i], 4);
        Plog1.write(&uid[i], 4);
        Plog1.write(str[i], 116);
    }
}


// Tests for findEmptyRow.
int insert_test_case_1() {
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

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    std::cout << page1 << " " << page2 << std::endl;
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < 3; i++) {
        std::string newKey = S1.getKey(uid[i]);
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
    std::cout << "\n======== " << __func__ << ": Cache in the same page." << std::endl;
    pbrb.printRowsBySchema(sid1);

    pbrb.cacheColdRow(basePtr + 5 * 128, testKey2);
    std::cout << "\n======== " << __func__ << ": (After insert)" << std::endl;
    pbrb.printRowsBySchema(sid1);

    return 0;
}

int insert_test_case_2() {
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

    PBRB pbrb(8, &wm, &indexer);

    auto page1 = pbrb.createCacheForSchema(sid1);
    pbrb.printRowsBySchema(sid1);

    std::vector<int> testVec{0, 1, 2, 3, 4};
    for (auto i: testVec) {
        std::string newKey = S1.getKey(uid[i]);
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


    auto hot1 = pbrb.cacheRowFromPlog(page1, 0, indexer[testKey1].addr[2]);
    indexer[testKey1].insertRow(hot1, pbrb.getTimestamp(hot1), true, &pbrb);
    auto hot2 = pbrb.cacheRowFromPlog(page1, 1, indexer[testKey1].addr[1]);
    indexer[testKey1].insertRow(hot2, pbrb.getTimestamp(hot2), true, &pbrb);
    auto hot3 = pbrb.cacheRowFromPlog(page1, 2, indexer[testKey1].addr[0]);
    indexer[testKey1].insertRow(hot3, pbrb.getTimestamp(hot3), true, &pbrb);
    std::cout << "\n======== " << __func__ << ": (3 Hot Rows), evict the oldest version and insert." << std::endl;
    pbrb.printRowsBySchema(sid1);
    pbrb.cacheColdRow(basePtr + 6 * 128, testKey1);
    std::cout << "\n======== " << __func__ << ": (After insert)" << std::endl;
    pbrb.printRowsBySchema(sid1);
    return 0;
}

int insert_test_case_3() {
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

    PBRB pbrb(8, &wm, &indexer);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < 4; i++) {
        std::string newKey = S1.getKey(uid[i]);
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
    std::cout << "\n======== " << __func__ << ": (3 Cold Row), Find offset by neighboring cold rows." << std::endl;
    pbrb.printRowsBySchema(sid1);
    pbrb.cacheColdRow(basePtr + 5 * 128, testKey2);
    std::cout << "\n======== " << __func__ << ": (After insert)" << std::endl;
    pbrb.printRowsBySchema(sid1);
    return 0;
}

int test3() {
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

    PBRB pbrb(8, &wm, &indexer);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    pbrb.printRowsBySchema(sid1);

    std::vector<int> testVec{0, 1, 2, 3};
    for (auto i: testVec) {
        std::string newKey = S1.getKey(uid[i]);
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

    PBRB pbrb(8, &wm, &indexer);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    pbrb.printRowsBySchema(sid1);

    std::vector<int> testVec{0, 1, 2, 3, 4, 8};
    for (auto i: testVec) {
        std::string newKey = S1.getKey(uid[i]);
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

    auto hot1 = pbrb.cacheRowFromPlog(page2, 1, indexer[testKey1].addr[2]);
    indexer[testKey1].insertRow(hot1, pbrb.getTimestamp(hot1), true, &pbrb);
    auto hot2 = pbrb.cacheRowFromPlog(page1, 5, indexer[testKey1].addr[1]);
    indexer[testKey1].insertRow(hot2, pbrb.getTimestamp(hot2), true, &pbrb);
    auto hot3 = pbrb.cacheRowFromPlog(page1, 4, indexer[testKey1].addr[0]);
    indexer[testKey1].insertRow(hot3, pbrb.getTimestamp(hot3), true, &pbrb);
    auto hot4 = pbrb.cacheRowFromPlog(page2, 4, indexer[testKey3].addr[1]);
    indexer[testKey3].insertRow(hot4, pbrb.getTimestamp(hot4), true, &pbrb);
    auto hot5 = pbrb.cacheRowFromPlog(page2, 3, indexer[testKey3].addr[0]);
    indexer[testKey3].insertRow(hot5, pbrb.getTimestamp(hot5), true, &pbrb);

    pbrb.cacheColdRow(basePtr + 5 * 128, testKey2);
    std::cout << "\n================ " << __func__ << " ================" << std::endl;
    pbrb.printRowsBySchema(sid1);
    return 0;
}

int test5() {
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

    PBRB pbrb(8, &wm, &indexer);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    pbrb.printRowsBySchema(sid1);
    std::vector<int> testVec{0, 1, 2, 3, 4, 8};
    for (auto i: testVec) {
        std::string newKey = S1.getKey(uid[i]);
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

    auto hot1 = pbrb.cacheRowFromPlog(page1, 2, indexer[testKey1].addr[2]);
    indexer[testKey1].insertRow(hot1, pbrb.getTimestamp(hot1), true, &pbrb);
    auto hot2 = pbrb.cacheRowFromPlog(page1, 5, indexer[testKey1].addr[1]);
    indexer[testKey1].insertRow(hot2, pbrb.getTimestamp(hot2), true, &pbrb);
    auto hot3 = pbrb.cacheRowFromPlog(page1, 4, indexer[testKey1].addr[0]);
    indexer[testKey1].insertRow(hot3, pbrb.getTimestamp(hot3), true, &pbrb);
    auto hot4 = pbrb.cacheRowFromPlog(page2, 0, indexer[testKey3].addr[1]);
    indexer[testKey3].insertRow(hot4, pbrb.getTimestamp(hot4), true, &pbrb);
    auto hot5 = pbrb.cacheRowFromPlog(page2, 1, indexer[testKey3].addr[0]);
    indexer[testKey3].insertRow(hot5, pbrb.getTimestamp(hot5), true, &pbrb);

    pbrb.cacheColdRow(basePtr + 5 * 128, testKey2);
    std::cout << "\n================ " << __func__ << " ================" << std::endl;
    pbrb.printRowsBySchema(sid1);
    return 0;
}

int test6() {
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

    PBRB pbrb(8, &wm, &indexer);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    std::cout << page1 << " " << page2 << std::endl;
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < 4; i++) {
        std::string newKey = S1.getKey(uid[i]);
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

int split_test() {
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

    PBRB pbrb(8, &wm, &indexer);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    std::cout << page1 << " " << page2 << std::endl;
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < length; i++) {
        std::string newKey = S1.getKey(uid[i]);
        std::cout << "Insert Key: " << newKey << " into indexer." << std::endl;
        
        if (indexer.find(newKey) == indexer.end()) {
            KVN kvn;
            kvn.insertRow(basePtr + i * 128, timestamp[i], false, &pbrb);
            indexer.insert({newKey, kvn});
        }
        pbrb.cacheColdRow(basePtr + i * 128, newKey);
    }

    std::cout << "\n======== " << __func__ << ": Before Split" << std::endl;

    pbrb.printRowsBySchema(sid1);

    pbrb.splitPage(page1);
    
    std::cout << "\n======== " << __func__ << ": After Split" << std::endl;
    pbrb.printRowsBySchema(sid1);

    return 0;
}

int merge_test() {
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

    PBRB pbrb(8, &wm, &indexer);

    auto page2 = pbrb.createCacheForSchema(sid1);
    auto page1 = pbrb.AllocNewPageForSchema(sid1);
    pbrb.printRowsBySchema(sid1);

    for (int i = 0; i < 4; i++) {
        std::string newKey = S1.getKey(uid[i]);
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
    std::cout << "\n======== " << __func__ << ": Before Merge" << std::endl;
    pbrb.printRowsBySchema(sid1);
    pbrb.mergePage(page2, page1);
    std::cout << "\n======== " << __func__ << ": After Merge" << std::endl;
    pbrb.printRowsBySchema(sid1);
    return 0;
}

int main() {
    insert_test_case_1();
    insert_test_case_2();
    insert_test_case_3();
    // test3();
    // test4();
    // test5();
    // test6();
    split_test();
    merge_test();
    return 0;
}
