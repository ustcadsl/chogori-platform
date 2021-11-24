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

#pragma once
#include <k2/common/Common.h>
#include <k2/transport/Payload.h>
#include <k2/transport/Status.h>
#include <k2/transport/PayloadSerialization.h>
#include <tuple>
namespace k2
{
    
namespace log {
inline thread_local k2::logging::Logger pmem_storage("k2::pmem_storage");
}

// define the plog address
// 
struct PmemAddress{
    uint64_t start_offset = 0;
    uint64_t size = 0;
    

    K2_PAYLOAD_FIELDS(start_offset, size);
    K2_DEF_FMT(PmemAddress,start_offset, size);
};

// define the pmem engine status
// Open Status
// S201_Created_Engine                     Create engine successfully
// S507_Insufficient_Storage               No enough space for pmem engine
// S403_Forbidden_Invalid_Config           Invalid pmem engine config

// Read status
// S200_OK_Found                           Found target data
// S403_Forbidden_Invalid_Offset           Invalid offset in pmem engine
// S403_Forbidden_Invalid_Size             Invalid size in pmem engine

// Append status
// S200_OK_Append                          Append data successfully           
// S507_Insufficient_Storage_Over_Capcity  Data size is over the engine capcity
// S409_Conflict_Append_Sealed_engine      Append data to a sealed engine

// SealStatus
// S200_OK_Sealed                          Seal the engine successfully
// S200_OK_AlSealed                        Already sealed before sealing

// File operation
// S201_Created_File                       Create pmem engine file successfully
// S200_OK_Map                             Map file to userspace successfully
// S409_Conflict_File_Existed              File exists before creating
// S500_Internal_Server_Error_Create_Fail  Fail to create pmem engine file
// S500_Internal_Server_Error_Map_Fail     Fail to map a file to userspace

struct PmemStatuses {
    // 200 OK 
    // append data in pmem engine sucessfully
    static const inline Status S200_OK_Append{.code=200, .message="Append data to pemem engine successfully!"};

    // 200 OK 
    // find the required data in pmem engine
    static const inline Status S200_OK_Found{.code=200, .message="Found the required data!"};

    // 200 OK
    // map file succeccfully
    static const inline Status S200_OK_Map{.code=200, .message="Map file successfully!"};

    // 201 OK
    // create pmem engine successfully
    static const inline Status S201_Created_Engine{.code=201, .message="Created pmem engine successfully!"};

    // 201 OK
    // create file successfully
    static const inline Status S201_Created_File{.code=201, .message="Created file successfully!"};

    // 200 OK the request is addressed rightly
    // the plog is sealed successfylly
    static const inline Status S200_OK_Sealed{.code=200, .message="Seal the target plog successfully!"};

    // 201 OK the request is addressed rightly
    // the plog is already sealed
    static const inline Status S200_OK_AlSealed{.code=200, .message="The target plog is already sealed!"};

    // 403 Forbidden
    // the offset is invalid in the existing Pmem
    static const inline Status S403_Forbidden_Invalid_Offset{.code=403, .message="Invalid offset in pmem!"};

    // 403 Forbidden
    // the size is invalid in the existing Pmem
    static const inline Status S403_Forbidden_Invalid_Size{.code=403, .message="Invalid size in pmem!"};

    // 409 Conflict
    // the file is already existed, so there is a conflict when creating file
    static const inline Status S409_Conflict_File_Existed{.code=409, .message="The file is already existed before existing!"}; 

    // 409 Conflict
    // the engine is already sealed, so it cannot serve append request
    static const inline Status S409_Conflict_Append_Sealed_engine{.code=409, .message="Try to append data to sealed engine!"}; 

    // 500 internal server error
    // fail to create file
    static const inline Status S500_Internal_Server_Error_Create_Fail{.code=500, .message="Create file failed!"};

    // 507 insufficient storage
    // no enough space for pemem engine to create file
    static const inline Status S507_Insufficient_Storage{.code=507, .message="Insufficien space for pmem engine!"};

    // 500 internal server error
    // fail to map a file
    static const inline Status S500_Internal_Server_Error_Map_Fail{.code=500, .message="Map a file failed!"};



    // 507 insufficient storage
    // the internal data size is over the capacity
    static const inline Status S507_Insufficient_Storage_Over_Capcity{.code=507, .message="Over the preset pmem engine capacity!"};

    // 403 forbidden
    // the configuration is invalid
    static const inline Status S403_Forbidden_Invalid_Config{.code=403, .message="Invalid pmem configuration!"};



};
//　plain old data format, satisfy the assignment with pmemcpy
// pmem storage engine config
struct PmemEngineConfig{

    // upper limit of pmem engine, default 2GB
    uint64_t engine_capacity = 2ULL << 30;

    // engine_path define the path of stored files 
    char engine_path[128] = "/mnt/pmem0/pmem-chogori/";

    // current offset of the plog, this value is persisted 
    // when the plog is sealed or close
    uint64_t tail_offset = 0;

    // control chunk size, default 8MB
    uint64_t chunk_size = 8ULL << 20;

    // number of current chunks
    uint64_t chunk_count = 0;

    // plogId is used to encode the chunk name
    // for example: if the plogId is userDataPlog
    // so the name of first chunk is userDataPlog_chunk0.plog
    //           the second chunk is userDataPlog_chunk1.plog
    char plog_id[128] = "userDataPlog";

    // is_sealed: true means sealed, false means activated 
    bool is_sealed = false;
};


//
//  K2 pmem storage engine interface
//
class PmemEngine{
    public:

    // open function is used to open an existing plog or create a plog
    // the parameter path only refers to the directory of plogs
    // the parameter plogId refers to the name of the plog 
    static Status open(PmemEngineConfig& ,PmemEngine **);

    PmemEngine(){}

    PmemEngine(const PmemEngine &) = delete;

    PmemEngine &operator=(const PmemEngine &) = delete;

    virtual ~PmemEngine(){}    

    virtual Status init(PmemEngineConfig &plog_meta) = 0;

    virtual std::tuple<Status, PmemAddress> append(Payload & ) = 0;

    virtual std::tuple<Status, Payload> read(const PmemAddress &) = 0;

    virtual Status seal() = 0;

    virtual uint64_t getFreeSpace() = 0;

    virtual uint64_t getUsedSpace() = 0;

};

}// ns k2
