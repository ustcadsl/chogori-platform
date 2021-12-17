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

#include "PmemLog.h"

namespace k2 {

Status PmemLog::init(PmemEngineConfig &plog_meta){
    // create the directory if not exist
    if(!std::filesystem::exists(plog_meta.engine_path)){
        bool res = std::filesystem::create_directories(plog_meta.engine_path);
        if (res == false){
            return PmemStatuses::S500_Internal_Server_Error_Create_Fail;
        }
    }
    const std::filesystem::space_info si = std::filesystem::space(plog_meta.engine_path);
    if ( si.available < plog_meta.engine_capacity){
        return PmemStatuses::S507_Insufficient_Storage;
    }
    //get engine_path and plog_id from the input parm
    _plog_meta = plog_meta;
    String meta_file_name = _genMetaFile();
    // check whether the metafile exists
    std::filesystem::path meteaFilePath(meta_file_name);
    bool isMetaFileExisted = std::filesystem::exists(meteaFilePath);
    // if exists, we donn't need to create, and we just map them
    std::tuple<Status, char*> metaFileMapRes;
    if (isMetaFileExisted){
        metaFileMapRes = _MapExistingFile(meta_file_name);
    }
    // if not exists, we need to create those files for them
    else{
        metaFileMapRes = _createThenMapFile(meta_file_name, sizeof(plog_meta));
    }

    // check out the status
    if( !std::get<0>(metaFileMapRes).is2xxOK()){
        return std::get<0>(metaFileMapRes);
    }
    // assign the plog metadata file name and pmem_addr
    _plog_meta_file.file_name = meta_file_name;
    _plog_meta_file.pmem_addr = std::get<1>(metaFileMapRes);

    if (isMetaFileExisted){
        // assign the plog metadata info from pmem space
        _plog_meta = *(PmemEngineConfig *)_plog_meta_file.pmem_addr;
        plog_meta = _plog_meta;
        for(uint64_t i = 0; i < _plog_meta.chunk_count; i++){
            String chunkName = _genNewChunk();
            auto chunkStatus = _MapExistingFile(chunkName);
            if(!std::get<0>(chunkStatus).is2xxOK()){
                return std::get<0>(chunkStatus);
            }
            _chunk_list.push_back({.file_name = std::move(chunkName),
                                    .pmem_addr = std::get<1>(chunkStatus)});
        }
        _active_chunk_id = _plog_meta.tail_offset / _plog_meta.chunk_size;
    }else{
        _plog_meta = plog_meta;
        // write metadata to metaFile
        if (_is_pmem){
            _copyToPmem(_plog_meta_file.pmem_addr, (char*)&_plog_meta,sizeof(_plog_meta));
        }else{
            _copyToNonPmem(_plog_meta_file.pmem_addr, (char*)&_plog_meta,sizeof(_plog_meta));
        }
        //create first chunk
        String firstChunk = _genNewChunk();
        auto chunkStatus = _createThenMapFile(firstChunk, _plog_meta.chunk_size);
        _plog_meta.chunk_count++;
        if(!std::get<0>(chunkStatus).is2xxOK()){
            return std::get<0>(chunkStatus);
        }
        _chunk_list.push_back({.file_name = std::move(firstChunk),
                                    .pmem_addr = std::get<1>(chunkStatus)});
    }
    return PmemStatuses::S201_Created_Engine;
}

std::tuple<Status, PmemAddress> PmemLog::append(Payload &payload){
    PmemAddress pmemAddr;
    pmemAddr = _plog_meta.tail_offset;
    PmemSize appendSize = payload.getSize() + 8;
    // checkout the is_sealed condition
    if( _plog_meta.is_sealed ){
        Status status = PmemStatuses::S409_Conflict_Append_Sealed_engine;
        return std::tuple<Status, PmemAddress>(std::move(status), std::move(pmemAddr));
    }
    // checkout the capacity
    if ( _plog_meta.tail_offset + appendSize > _plog_meta.engine_capacity){
        Status status = PmemStatuses::S507_Insufficient_Storage_Over_Capcity;
        return std::tuple<Status, PmemAddress>(std::move(status), std::move(pmemAddr));
    }
    char * buffer_data = (char*)malloc(appendSize);
    uint64_t payload_data_size = payload.getSize();
    * (PmemSize *)(buffer_data) = payload_data_size;
    payload.seek(0);
    //promise not change the payload content
    payload.read(buffer_data + sizeof (PmemSize), payload_data_size);
    payload.seek(0);
    Status status = _append(buffer_data,appendSize);
    free(buffer_data);
    return std::tuple<Status, PmemAddress>(std::move(status), std::move(pmemAddr));
}

Status PmemLog::_append(char *srcdata, size_t len){
    K2LOG_D(log::pmem_storage,"Append data: offset=>{},len=>{} ", _plog_meta.tail_offset, len);
    // calculate data need to write in the activate chunk
    size_t remaining_space = _plog_meta.chunk_count * _plog_meta.chunk_size - _plog_meta.tail_offset;
    size_t first_write_size = remaining_space <= len? remaining_space : len;
    char * addr = _chunk_list[_active_chunk_id].pmem_addr + _plog_meta.tail_offset%_plog_meta.chunk_size;
    k2::OperationLatencyReporter reporter(_writePmemLatency); // for reporting metrics
    if (_is_pmem){
        _copyToPmem(addr, srcdata, first_write_size);
    }else{
        _copyToNonPmem(addr, srcdata, first_write_size);
    }
    // there is still data need to write to the next chunks
    if (len > first_write_size){
        size_t need_write_size = len - first_write_size;
        size_t need_new_chunk = need_write_size % _plog_meta.chunk_size != 0? 
                                 need_write_size/_plog_meta.chunk_size+1:
                                 need_write_size/_plog_meta.chunk_size;
        K2LOG_D(log::pmem_storage, "Need write size:{} ,need new chunk:{}",need_write_size,need_new_chunk);
        for( size_t i = 1; i <= need_new_chunk; i++){
            String chunkFileName = _genNewChunk();
            auto chunkStatus = _createThenMapFile(chunkFileName, _plog_meta.chunk_size);
            _plog_meta.chunk_count++;
            if(!std::get<0>(chunkStatus).is2xxOK()){
                return std::get<0>(chunkStatus);
            }
            char * newChunkAddr = std::get<1>(chunkStatus);
            struct FileInfo tmp_chunk = {.file_name=std::move(chunkFileName),.pmem_addr=newChunkAddr};
            _chunk_list.push_back(std::move(tmp_chunk));
            // start to write to another chunk
            size_t tmp_write_size =  need_write_size >= _plog_meta.chunk_size?
                                    _plog_meta.chunk_size:
                                    need_write_size;
            K2LOG_D(log::pmem_storage, "tmp write size:{}",tmp_write_size);
            if (_is_pmem){
                _copyToPmem(newChunkAddr, srcdata + len- need_write_size, tmp_write_size);
            }else{
                _copyToNonPmem(newChunkAddr, srcdata + len- need_write_size,tmp_write_size);
            }
            need_write_size -= tmp_write_size;
        }
    }
    _plog_meta.tail_offset += len;
    reporter.report();
    return PmemStatuses::S200_OK_Append;
}

std::tuple<Status, Payload> PmemLog::read(const PmemAddress &readAddr){
    Payload payload(Payload::DefaultAllocator());
    // checkout the effectiveness of start_offset
    if( readAddr > _plog_meta.tail_offset){
        Status status = PmemStatuses::S403_Forbidden_Invalid_Offset;
        return std::tuple<Status,Payload>(std::move(status), std::move(payload));
    }
    PmemSize readSize = *(PmemSize *)_read(readAddr, sizeof(PmemSize));
    // checkout the effectiveness of read size
    if( readAddr + readSize > _plog_meta.tail_offset){
        Status status = PmemStatuses::S403_Forbidden_Invalid_Size;
        return std::tuple<Status,Payload>(std::move(status), std::move(payload));
    }
    char * read_data = _read(readAddr + sizeof(PmemSize), readSize);
    payload.reserve(readSize);
    payload.seek(0);
    payload.write(read_data, readSize);
    payload.seek(0);
    free(read_data);
    Status status = PmemStatuses::S200_OK_Found;
    return std::tuple<Status, Payload>(std::move(status), std::move(payload));
}

 char* PmemLog::_read(uint64_t offset, uint64_t len){
    K2LOG_D(log::pmem_storage, "Read data: offset =>{},len=>{}",offset,len);
    char * buffer_data = (char *)malloc(len);
    k2::OperationLatencyReporter reporter(_readPmemLatency); // for reporting metrics
    // calculate the size need read in the first chunk
    size_t start_chunk_id = offset/_plog_meta.chunk_size;
    size_t start_chunk_offset = offset % _plog_meta.chunk_size;
    size_t start_chunk_remaining_space = _plog_meta.chunk_size - start_chunk_offset;
    size_t first_read_size = start_chunk_remaining_space <= len ? start_chunk_remaining_space: len;
    char * star_addr = _chunk_list[start_chunk_id].pmem_addr + start_chunk_offset;
    memcpy(buffer_data, star_addr, first_read_size);
    // there is still data in next chunks
    if(len > first_read_size){
        size_t need_read_size = len - first_read_size;
        size_t need_read_chunk = need_read_size % _plog_meta.chunk_size != 0? 
                                 need_read_size/_plog_meta.chunk_size + 1:
                                 need_read_size/_plog_meta.chunk_size;
        K2LOG_D(log::pmem_storage, "Need read size:{} ,need read anthor chunk:{}",need_read_size,need_read_chunk);
        for(size_t i = 1; i <= need_read_chunk; i++){
            char * chunk_addr = _chunk_list[start_chunk_id+i].pmem_addr;
            size_t tmp_read_size =  need_read_size >= _plog_meta.chunk_size? 
                                    _plog_meta.chunk_size:
                                    need_read_size;
            K2LOG_D(log::pmem_storage, "tmp read size:{}",tmp_read_size);
            memcpy(buffer_data + len - need_read_size ,chunk_addr,tmp_read_size);
            need_read_size -= tmp_read_size;
        }

    }
    reporter.report();
    return buffer_data;
}

Status PmemLog::seal(){
    if (_plog_meta.is_sealed == false){
        return PmemStatuses::S200_OK_Sealed;
    }else{
        return PmemStatuses::S200_OK_AlSealed;
    }
}
uint64_t PmemLog::getFreeSpace(){
    return _plog_meta.engine_capacity - _plog_meta.tail_offset;
}

uint64_t PmemLog::getUsedSpace(){
    return _plog_meta.tail_offset;
};

} // namespace k2
