//
// Created by LCY on 2021/4/13.
//
#define CATCH_CONFIG_MAIN

#include <k2/indexer/IndexerInterface.h>
#include <k2/dto/Timestamp.h>
#include <k2/transport/Payload.h>

#include "catch2/catch.hpp"

using namespace k2;

uint32_t TSOID = 1;
uint32_t TStartDelta = 1000;

template <typename DataType>
dto::DataRecord* createDataRecord(const dto::Key& key, const DataType& data, const uint64_t& tendTimestamp) {
    dto::DataRecord* recordPtr=new dto::DataRecord();
    recordPtr->key = std::move(key);
    recordPtr->value.val = Payload(Payload::DefaultAllocator);
    recordPtr->value.val.write(data);// Q: write or copy?
    dto::Timestamp ts(tendTimestamp, TSOID, TStartDelta);
    recordPtr->txnId = dto::TxnId{.trh = std::move(key), .mtr = dto::K23SI_MTR{.timestamp = ts}};
    recordPtr->status = dto::DataRecord::Committed;
    return recordPtr;
}

TEST_CASE("Test1: empty KeyValueNode tests") {
uint64_t tendTimestamp = 1;
KeyValueNode kvNode;
REQUIRE(kvNode.size() == 0);

dto::DataRecord *ptr = kvNode.get_datarecord(dto::Timestamp(tendTimestamp, TSOID, TStartDelta));
REQUIRE(ptr == nullptr);

int res = kvNode.remove_datarecord(dto::Timestamp(tendTimestamp, TSOID, TStartDelta));
REQUIRE(res == 1);
res = kvNode.remove_datarecord(0);
REQUIRE(res == 1);
}

TEST_CASE("Test2: Insert get and remove tests") {
KeyValueNode kvNode;
//Datarecords in keyvalueNode are sorted by timestamps?
dto::DataRecord* dataPtr1 = createDataRecord<std::string>(dto::Key{.partitionKey = "Key", .rangeKey = "rKey"}, "payload1", 2);
dto::DataRecord* dataPtr2 = createDataRecord<std::string>(dto::Key{.partitionKey = "key", .rangeKey = "rKey"}, "payload2", 4);
dto::DataRecord* dataPtr3 = createDataRecord<std::string>(dto::Key{.partitionKey = "key", .rangeKey = "rKey"}, "payload3", 6);
dto::DataRecord* dataPtr4 = createDataRecord<std::string>(dto::Key{.partitionKey = "key", .rangeKey = "rKey"}, "payload4", 8);
REQUIRE(kvNode.insert_datarecord(dataPtr1) == 0);
REQUIRE(kvNode.insert_datarecord(dataPtr2) == 0);
REQUIRE(kvNode.size() == 2);
REQUIRE(kvNode.insert_datarecord(dataPtr3) == 0);
REQUIRE(kvNode.size() == 3);
REQUIRE(kvNode.insert_datarecord(dataPtr4) == 0);
REQUIRE(kvNode.size() == 4);

dto::DataRecord* Ptr1 = kvNode.get_datarecord(dto::Timestamp(1, TSOID, TStartDelta));
REQUIRE(Ptr1 == nullptr);
dto::DataRecord* Ptr2 = kvNode.get_datarecord(dto::Timestamp(3, TSOID, TStartDelta));
REQUIRE(Ptr2 == dataPtr1);
dto::DataRecord* Ptr3 = kvNode.get_datarecord(dto::Timestamp(5, TSOID, TStartDelta));
REQUIRE(Ptr3 == dataPtr2);
dto::DataRecord* Ptr4 = kvNode.get_datarecord(dto::Timestamp(7, TSOID, TStartDelta));
REQUIRE(Ptr4 == dataPtr3);
dto::DataRecord* Ptr5 = kvNode.get_datarecord(dto::Timestamp(9, TSOID, TStartDelta));
REQUIRE(Ptr5 == dataPtr4);

//remove with the same timestamp
REQUIRE(kvNode.remove_datarecord(dto::Timestamp(1, 1, 1000)) == 1);
REQUIRE(kvNode.remove_datarecord(dto::Timestamp(2, TSOID, TStartDelta)) == 1);
REQUIRE(kvNode.remove_datarecord(dto::Timestamp(6, TSOID, TStartDelta)) == 0);//remove datarecord[1]
REQUIRE(kvNode.size() == 3);
//remove with order
REQUIRE(kvNode.remove_datarecord(0) == 0); // remove datarecord[0]
REQUIRE(kvNode.remove_datarecord(3) == 1);// Q
//remove with datarecord ptr
REQUIRE(kvNode.remove_datarecord(nullptr) == 1);// Q
REQUIRE(kvNode.remove_datarecord(dataPtr2) == 0);
REQUIRE(kvNode.remove_datarecord(dataPtr1) == 0);// Q
REQUIRE(kvNode.size() == 0);
}
