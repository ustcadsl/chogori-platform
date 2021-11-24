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
    sprintf(testdata,"str[1]fortest");
    Payload p([] { return Binary(128); });
    p.write(testdata, 128);
    p.seek(0);
    auto result = engine_ptr->append(p);
    REQUIRE(std::get<0>(result).is2xxOK());
    REQUIRE(std::get<1>(result).start_offset == 0); 
    REQUIRE(std::get<1>(result).size == 128);
    delete engine_ptr;
    delete testdata;
}

SCENARIO("Test append large data to PmemLog") {
    // test write 1KB data once a time and write 128*1024 times, totally 128MB
    // it will generate 32 plog files in totally
    PmemEngineConfig plogConfig;
    strcpy(plogConfig.engine_path,testBaseDir.c_str());
    auto status = PmemEngine::open(plogConfig, &engine_ptr);
    REQUIRE(status.is2xxOK());

    uint32_t times = 128;
    uint32_t data_size = 1024*1024;
    char * testdata = new char[data_size];
    memset(testdata, 45, data_size);
    std::tuple<Status, PmemAddress> result;
    for (uint32_t i = 0; i < times; i++){
        sprintf(testdata,"str[%u]fortest",i);
        Payload p([] { return Binary(1024*1024); });
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
    pmemaddr.start_offset = 0;
    pmemaddr.size = 128;
    // construct the expect data
    char * expect_data = new char[128];
    memset(expect_data, 43, 128);
    sprintf(expect_data,"str[1]fortest");
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
    pmemaddr.start_offset = 100 + 3* 1024*1024;
    pmemaddr.size = 1024*1024;
    char * expect_data = new char[1024*1024];
    memset(expect_data, 45, 1024*1024);
    sprintf(expect_data,"str[3]fortest");

    auto result = engine_ptr->read(pmemaddr);
    REQUIRE(std::get<0>(result).is2xxOK());
    char * payload_data = new char[1024*1024];
    std::get<1>(result).read(payload_data, 1024*1024);
    REQUIRE(strcmp(expect_data,payload_data));
    delete engine_ptr;
    delete expect_data;
    delete payload_data;
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
