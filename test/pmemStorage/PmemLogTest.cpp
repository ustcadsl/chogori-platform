/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#define CATCH_CONFIG_MAIN

#include <cstdarg>
#include <k2/pmemStorage/PmemLog.h>
#include <k2/pmemStorage/PmemEngine.h>
#include <k2/dto/SKVRecord.h>

#include "catch2/catch.hpp"


using namespace k2;
// TEST the generate function of pmem_log
//
std::string testBaseDir = "/mnt/pmem0/chogori-test";
PmemEngine * engine_ptr = nullptr;

SCENARIO("Generate a new PmemLog") {

    PmemEngineConfig plogConfig;
    plogConfig.chunk_size = 4ULL << 20;
    plogConfig.engine_capacity = 1ULL << 30;
    strcpy(plogConfig.engine_path,testBaseDir.c_str());
    
    if(std::filesystem::exists(testBaseDir)){
        std::filesystem::remove_all(testBaseDir);
    }
    auto status = PmemEngine::open(plogConfig, &engine_ptr);
    REQUIRE(status.is2xxOK());
    delete engine_ptr;
}


SCENARIO("Generate with a existing PmemLog") {
    PmemEngineConfig plogConfig;
    strcpy(plogConfig.engine_path,testBaseDir.c_str());
    auto status = PmemEngine::open(plogConfig, &engine_ptr);
    REQUIRE(plogConfig.chunk_size == 4ULL << 20);
    REQUIRE(plogConfig.engine_capacity == 1ULL << 30);
    delete engine_ptr;
}

SCENARIO("Test append small data to PmemLog") {
    PmemEngineConfig plogConfig;
    strcpy(plogConfig.engine_path,testBaseDir.c_str());
    auto status = PmemEngine::open(plogConfig, &engine_ptr);
    REQUIRE(status.is2xxOK());
    char * testdata = new char[128];
    memset(testdata, 43, 128);
    sprintf(testdata,"str[1]cc");
    Payload p(Payload::DefaultAllocator(128));
    p.write(testdata, 128);
    p.seek(0);
    auto result = engine_ptr->append(p);
    REQUIRE(std::get<0>(result).is2xxOK());
    REQUIRE(std::get<1>(result) == 0); 
    delete engine_ptr;
    delete testdata;
}

SCENARIO("Test append large data to PmemLog") {
    // test write 1KB data once a time and write 64*1024 times, totally 128MB
    // it will generate 32 plog files in totally
    PmemEngineConfig plogConfig;
    strcpy(plogConfig.engine_path,testBaseDir.c_str());
    auto status = PmemEngine::open(plogConfig, &engine_ptr);
    REQUIRE(status.is2xxOK());

    uint32_t times = 64;
    uint32_t data_size = 1024*1024;
    char * testdata = new char[data_size];
    
    std::tuple<Status, PmemAddress> result;
    for (uint32_t i = 0; i < times; i++){
        memset(testdata, 33+i, data_size);
        sprintf(testdata,"str[%u]cc",i);
        Payload p(Payload::DefaultAllocator(1024*1024));
        p.write(testdata, data_size);
        p.seek(0);
        result = engine_ptr->append(p);
    }
    REQUIRE(std::get<0>(result).is2xxOK());
    delete engine_ptr;
    delete testdata;
}

SCENARIO("Test read small data within one chunk from PmemLog") {
    //open existing PmemLog
    PmemEngineConfig plogConfig;
    strcpy(plogConfig.engine_path,testBaseDir.c_str());
    auto status = PmemEngine::open(plogConfig, &engine_ptr);
    REQUIRE(status.is2xxOK());
    //construct read address
    PmemAddress pmemaddr;
    pmemaddr = 0;
    // construct the expect data
    char * expect_data = new char[128];
    memset(expect_data, 43, 128);
    sprintf(expect_data,"str[1]cc");
    //read data
    auto result = engine_ptr->read(pmemaddr);
    REQUIRE(std::get<0>(result).is2xxOK());
    char * payload_data = new char[128];
    std::get<1>(result).read(payload_data, 128);
    REQUIRE(strcmp(expect_data,payload_data) == 0);
    delete engine_ptr;
    delete expect_data;
    delete payload_data;
}
SCENARIO("Test read large data across two chunks from PmemLog") {
    //open existing PmemLog
    PmemEngineConfig plogConfig;
    strcpy(plogConfig.engine_path,testBaseDir.c_str());
    auto status = PmemEngine::open(plogConfig, &engine_ptr);
    REQUIRE(status.is2xxOK());
    //construct read address
    PmemAddress pmemaddr;
    pmemaddr = 128 + 3*1024*1024 + 4*8;
    char * expect_data = new char[1024*1024];
    memset(expect_data, 36, 1024*1024);
    sprintf(expect_data,"str[3]cc");
    auto result = engine_ptr->read(pmemaddr);
    REQUIRE(std::get<0>(result).is2xxOK());
    char * payload_data = new char[1024*1024];
    std::get<1>(result).read(payload_data, 1024*1024);
    REQUIRE(strcmp(expect_data,payload_data) == 0);
    delete engine_ptr;
    delete expect_data;
    delete payload_data;
}
SCENARIO("Test write and read SKVRecord::Storage from PmemLog"){
    PmemEngineConfig plogConfig;
    strcpy(plogConfig.engine_path,testBaseDir.c_str());
    auto status = PmemEngine::open(plogConfig, &engine_ptr);
    REQUIRE(status.is2xxOK());

    k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, false},
            {k2::dto::FieldType::INT32T, "Balance", false, false}
    };
    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName"});
    schema.setRangeKeyFieldsByName(std::vector<k2::String>{"FirstName"});

    k2::dto::SKVRecord doc("collection", std::make_shared<k2::dto::Schema>(schema));
    doc.serializeNull();
    doc.serializeNext<k2::String>("b");
    doc.serializeNext<int32_t>(12);

    // Testing a typical use-case of getSKVKeyRecord where the storage is serialized
    // to payload and then read back later
    k2::dto::SKVRecord key_record = doc.getSKVKeyRecord();
    const k2::dto::SKVRecord::Storage& storage = key_record.getStorage();
    k2::Payload payload(k2::Payload::DefaultAllocator());
    payload.write(storage);
    
    auto writeResult = engine_ptr->append(payload);
    REQUIRE(std::get<0>(writeResult).is2xxOK());

    PmemAddress payloadAddr = std::get<1>(writeResult);

    auto readResult = engine_ptr->read(payloadAddr);
    REQUIRE(std::get<0>(readResult).is2xxOK());

    k2::dto::SKVRecord::Storage read_storage{};
    std::get<1>(readResult).read(read_storage);
    REQUIRE(storage.schemaVersion == read_storage.schemaVersion);
    
    k2::dto::SKVRecord reconstructed("collection", std::make_shared<k2::dto::Schema>(schema),
                                     std::move(read_storage), true);
    std::optional<k2::String> last = reconstructed.deserializeNext<k2::String>();
    REQUIRE(!last.has_value());
    std::optional<k2::String> first = reconstructed.deserializeNext<k2::String>();
    REQUIRE(first.has_value());
    REQUIRE(first == "b");
    std::optional<int32_t> balance = reconstructed.deserializeNext<int32_t>();
    REQUIRE(!balance.has_value());
    delete engine_ptr;

}
SCENARIO("Append then Read data from two chunks")
{
    if(std::filesystem::exists(testBaseDir)){
        std::filesystem::remove_all(testBaseDir);
    }
    PmemEngineConfig plogConfig;
    strcpy(plogConfig.engine_path,testBaseDir.c_str());
    auto status = PmemEngine::open(plogConfig, &engine_ptr);
    REQUIRE(status.is2xxOK());

    // first append data with size [0,    8388534] firt 8 bytes is token by size
    //                                           |
    //  userDataPlog_0.plog        [0,     8388607]   -> 73 left space
    char * previous_data = new char[8388527];
    memset(previous_data, 'f', 8388527);
    Payload p1(Payload::DefaultAllocator(8388527));
    p1.write(previous_data, 8388527);
    p1.seek(0);
    auto write_res = engine_ptr->append(p1);
    REQUIRE(std::get<0>(write_res).is2xxOK());
    
    // last data consume space 8388535
    // second data will consume 103+8 = 111 space
    // so will write 38 bytes to next chunk
    //                                 [8388535, 8388645]                   
    // Plog distribution : [0,     8388607]->[8388608,     16777215]

    char * second_data = new char[103];
    memset(second_data, 's', 103);
    Payload p2(Payload::DefaultAllocator(103));
    p2.write(second_data, 103);
    p2.seek(0);
    auto write_res2 = engine_ptr->append(p2);
    REQUIRE(std::get<0>(write_res2).is2xxOK());

    char * third_data = new char[1024];
    memset(third_data, 't', 1024);
    Payload p3(Payload::DefaultAllocator(1024));
    p3.write(third_data, 1024);
    p3.seek(0);
    auto write_res3 = engine_ptr->append(p3);
    REQUIRE(std::get<0>(write_res3).is2xxOK());

    auto read_result = engine_ptr->read(8388535);
    REQUIRE(std::get<0>(read_result).is2xxOK());
    char * read_second_data = new char[103];
    memset(read_second_data, 'k', 103);
    std::get<1>(read_result).read(read_second_data, 103);
    REQUIRE(strcmp(second_data,read_second_data) == 0);

    delete previous_data;
    delete second_data;
    delete third_data;
    delete read_second_data;
    delete engine_ptr;
  
}                                                                               
SCENARIO("Remove all the test files")
{
    bool status = false;
    if(std::filesystem::exists(testBaseDir)){
        status = std::filesystem::remove_all(testBaseDir);
    }else{
        status = true;
    }
    REQUIRE(status == true);

}
