//
// Created by 12292 on 2021/4/13.
//
#define CATCH_CONFIG_MAIN

#include <k2/indexer/IndexerInterface.h>

#include "catch2/catch.hpp"

using namespace k2;

template <typename DataType>
dto::DataRecord* createDataRecord(const dto::Key& key, const DataType& data, const uint64_t& ts) {
    dto::DataRecord* recordPtr=new dto::DataRecord();
    recordPtr->key = std::move(key);
    recordPtr->value.val.write(data);// Q: write or copy?
    recordPtr->txnId = dto::TxnId{.trh = std::move(key), .mtr = dto::K23SI_MTR{.timestamp = ts}};
    recordPtr->status = dto::DataRecord::Committed;
    return recordPtr;
}

TEST_CASE("Test1: empty KeyValueNode tests") {
uint64_t tendTimestamp = 1;
KeyValueNode kvNode;
REQUIRE(kvNode.size() == 0);

dto::Timestamp ts1(tendTimestamp, 1, 1000);
dto::dataRecord *ptr = kvNode.get_datarecord(ts1);
REQUIRE(ptr == nullptr);

int res = kvNode.remove_datarecord(ts1);
REQUIRE(res == 1);
res = kvNode.remove_datarecord(0);
REQUIRE(res == 1);
}

TEST_CASE("Test2: Insert get and remove tests") {
keyValueNode kvNode;
//Datarecords in keyvalueNode are sorted by timestamps?
dto::DataRecord* dataPtr1 = createDataRecord<std::string>(dto::Key{.partitionKey = "Key", .rangeKey = "rKey"}, "payload1", 2);
dto::DataRecord* dataPtr2 = createDataRecord<std::string>(dto::key{.partitionKey = "key", .rangeKey = "rKey"}, "payload2", 4);
dto::DataRecord* dataPtr3 = createDataRecord<std::string>(dto::Key{.partitionKey = "key", .rangeKey = "rKey"}, "payload3", 6);
dto::DataRecord* dataPtr4 = createDataRecord<std::string>(dto::Key{.partitionKey = "key", .rangeKey = "rKey"}, "payload4", 8);
REQUIRE(kvNode.insert_datarecord(dataPtr1) == 0);
REQUIRE(kvNode.insert_datarecord(dataPtr2) == 0);
REQUIRE(kvNode.size() == 2);
REQUIRE(kvNode.insert_datarecord(dataPtr3) == 0);
REQUIRE(kvNode.size() == 3);
REQUIRE(kvNode.insert_datarecord(dataPtr4) == 0);
REQUIRE(kvNode.size() == 4);

dto::DataRecord* Ptr1 = kvNode.get_datarecord(dto::Timestamp(1, 1, 1000));
REQUIRE(Ptr1 == nullptr);
dto::DataRecord* Ptr2 = kvNode.get_datarecord(dto::Timestamp(3, 1, 1000));
REQUIRE(Ptr2 == dataPtr1);
dto::DataRecord* Ptr3 = kvNode.get_datarecord(dto::Timestamp(5, 1, 1000));
REQUIRE(Ptr3 == dataPtr2);
dto::DataRecord* Ptr4 = kvNode.get_datarecord(dto::Timestamp(7, 1, 1000));
REQUIRE(Ptr4 == dataPtr3);
dto::DataRecord* Ptr5 = kvNode.get_datarecord(dto::Timestamp(9, 1, 1000));
REQUIRE(Ptr5 == dataPtr4);

//remove with the same timestamp
REQUIRE(kvNode.remove_datarecord(dto::Timestamp(1, 1, 1000)) == 1);
REQUIRE(kvNode.remove_datarecord(dto::Timestamp(2, 1, 1000)) == 1);
REQUIRE(kvNode.remove_datarecord(dto::Timestamp(6, 1, 1000)) == 0);//remove datarecord[1]
REQUIRE(kvNode.size() == 3);
//remove with order
REQUIRE(kvNode.remove_datarecord(0) == 0); // remove datarecord[0]
REQUIRE(kvNode.remove_datarecord(3) == 1);// Q
//remove with datarecord ptr
REQUIRE(kvNode.remove_datarecord(nullptr) == 1);// Q
REQUIRE(kvNode.remove_datarecord(dataPtr2) == 0);// Q
REQUIRE(kvNode.remove_datarecord(dataPtr1) == 0);
REQUIRE(kvNode.size() == 0);
}
