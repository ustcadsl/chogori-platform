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
#include <k2/appbase/AppEssentials.h>
#include <k2/common/Timer.h>
#include <k2/common/Common.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/MessageVerbs.h>
#include <libpmem.h>
#include <cmath>
#include <k2/transport/Payload.h>
#include <filesystem>
#include "PmemEngine.h"

namespace k2{



class PmemLog : public PmemEngine
{
public:
    PmemLog(){}

    ~PmemLog() override{
        if (_is_pmem){
            _copyToPmem(_plog_meta_file.pmem_addr, (char*)&_plog_meta,sizeof(_plog_meta));
        }else{
            _copyToNonPmem(_plog_meta_file.pmem_addr, (char*)&_plog_meta,sizeof(_plog_meta));
        }

        pmem_unmap(_plog_meta_file.pmem_addr,sizeof(PmemEngineConfig));
        for (auto & i : _chunk_list){
            pmem_unmap(i.pmem_addr, _plog_meta.chunk_size);
        }
    }


    Status init(PmemEngineConfig &plog_meta) override;

    std::tuple<Status, PmemAddress>  append(Payload &payload) override;

    std::tuple<Status, Payload> read(const PmemAddress &readAddr) override;

    Status seal() override;

    uint64_t getFreeSpace() override;

    uint64_t getUsedSpace() override;



private:

    // private _append function to write srcdata to plog
    Status _append(char *srcdata, size_t len);

    // private _read function to read data from given offset and size
    char* _read(uint64_t offset, uint64_t size);

    // Map an existing file
    //   case 1 : file already exists
    //            return S500_Internal_Server_Error_Map_Fail
    //   case 2 : file is created successfully
    //            return S201_Created_File
    inline std::tuple<Status, char*> _MapExistingFile(String chunkFile){
        char * pmem_addr;
        size_t mapped_len;
        if ((pmem_addr = static_cast<char *>(pmem_map_file(
                        chunkFile.c_str(),
                        0,
                        0,
                        0666, &mapped_len, &_is_pmem))) == NULL) {
            K2LOG_E(log::pmem_storage,"pmem map existing file fail!");
            Status status = PmemStatuses::S500_Internal_Server_Error_Map_Fail;
            return std::tuple<Status, char*>(std::move(status), std::move(pmem_addr));
        }
        Status status = PmemStatuses::S200_OK_Map;
        return std::tuple<Status, char*>(std::move(status), std::move(pmem_addr));
    }

    // Create and Map file to userspace
    //  case 1: no enough space for creating files
    //          return S507_Insufficient_Storage
    //  case 2: targe file already existed
    //          return S409_Conflict_Created_File
    //  case 3: create file successfully
    //          return S201_Created_File
    inline std::tuple<Status, char*> _createThenMapFile(String chunkFile, uint64_t fileSize){
        // check avaliable space in engine data path
        const std::filesystem::space_info si = std::filesystem::space(_plog_meta.engine_path);
        if ( si.available < fileSize){
            Status status = PmemStatuses::S507_Insufficient_Storage;
            return std::tuple<Status, char*>(std::move(status), NULL);
        }

        // create the target file
        char * pmem_addr;
        size_t mapped_len;
        if ((pmem_addr = static_cast<char *>(pmem_map_file(
                        chunkFile.c_str(),
                        fileSize,
                        PMEM_FILE_CREATE | PMEM_FILE_EXCL,
                        0666, &mapped_len, &_is_pmem))) == NULL) {
            K2LOG_E(log::pmem_storage,"pmem create existing file!");
            Status status = PmemStatuses::S409_Conflict_File_Existed;
            return std::tuple<Status, char*>(std::move(status), std::move(pmem_addr));
        }

        Status status = PmemStatuses::S201_Created_File;
        return std::tuple<Status, char*>(std::move(status), std::move(pmem_addr));
    }

    // generate the new chunk name
    inline String _genNewChunk(){
        _active_chunk_id++;
        return fmt::format("{}/{}_{}.plog",_plog_meta.engine_path,_plog_meta.plog_id,_active_chunk_id);
    }

    // generate the metadata file name
    inline String _genMetaFile(){
        return fmt::format("{}/{}.meta",_plog_meta.engine_path,_plog_meta.plog_id);
    }

    // write data to file in nonpmem by memcpying method
    inline void _copyToNonPmem(char *dstaddr, char *srcaddr, size_t len){
        memcpy(dstaddr, srcaddr, len);
        // Flush it
        pmem_msync(dstaddr, len);
    }

    // write data to pmem file by store instruction
    inline void _copyToPmem(char *pmem_addr, char *srcaddr, size_t len){
        pmem_memcpy_persist(pmem_addr, srcaddr, len);
    }

    struct FileInfo{
        String file_name;
        char * pmem_addr;
    };
    // indentify whether the target path is in pmem device
    // indicate when crating or mapping existing file
    int _is_pmem;

    // the current activate chunk id
    int _active_chunk_id = -1;

    // record all the information of chunks
    std::vector<FileInfo> _chunk_list;

   // define the meta file info
    // incluing file_name and pmem_addr
    FileInfo _plog_meta_file;

    // metadata of the plog
    // usually user defined
    PmemEngineConfig _plog_meta;

};

}// ns k2
