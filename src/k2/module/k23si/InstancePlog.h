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
#include "Config.h"
#include "Log.h"


namespace k2{
struct ChunkInfo{
    String _chunk_path;
    char * _pmemaddr;
};
struct PlogAddress{
    uint64_t _address;
};
class LocalPlog
{
public:
   LocalPlog(uint64_t plog_id, String plog_path): 
   _plog_id(plog_id),
   _plog_path(plog_path)
   {
       String chunkFileName = genNewChunkFileName();
       char * chunkFileAddr = createNewChunkFile(chunkFileName);
       struct ChunkInfo first_chunk = {._chunk_path=std::move(chunkFileName),._pmemaddr=chunkFileAddr};
       K2LOG_D(log::skvsvr,"Created plog :{} ", plogid);
   }

    void append(char *srcdata, size_t len){
        size_t remaining_space = _chunk_max_size - _tail_offset%_chunk_max_size;
        size_t write_size = remaining_space <= len? remaining_space : len;
        char * pemaddr = _chunk_list[_active_chunk_id]._pmemaddr + _tail_offset%_chunk_max_size;

        if (_is_pmem){
            do_copy_to_pmem(pemaddr, srcdata, write_size);
        }else{
            do_copy_to_non_pmem(pemaddr, srcdata, write_size);
        }

        if (len > remaining_space){
            size_t need_new_chunk= ceil((len-remaining_space)/_chunk_max_size);
            for( size_t i = 0; i < need_new_chunk; i++){
                String chunkFileName = genNewChunkFileName();
                char * chunkFileAddr = createNewChunkFile(chunkFileName);
                struct ChunkInfo tmp_chunk = {._chunk_path=std::move(chunkFileName),._pmemaddr=chunkFileAddr};
                _chunk_list.push_back(std::move(tmp_chunk));
                if( i == need_new_chunk-1){
                    if (_is_pmem){
                        do_copy_to_pmem(chunkFileAddr, srcdata+remaining_space + i*_chunk_max_size, (len-remaining_space)%_chunk_max_size);
                    }else{
                        do_copy_to_non_pmem(chunkFileAddr, srcdata+remaining_space + i*_chunk_max_size, (len-remaining_space)%_chunk_max_size);
                    }
                }else{
                    if (_is_pmem){
                        do_copy_to_pmem(chunkFileAddr, srcdata+remaining_space + i*_chunk_max_size, _chunk_max_size);
                    }else{
                        do_copy_to_non_pmem(chunkFileAddr, srcdata+remaining_space + i*_chunk_max_size, _chunk_max_size);
                    }
                }
            }
        }
        _tail_offset += len;
    }

private:
    char * createNewChunkFile(String newChunkFileName){
        char * pmemaddr;
        size_t mapped_len;
        if ((pmemaddr = static_cast<char *>(pmem_map_file(newChunkFileName,
            _chunk_max_size,
            PMEM_FILE_CREATE|PMEM_FILE_EXCL,
            0666, &mapped_len, &_is_pmem))) == NULL) {
            K2LOG_E(log::skvsvr,"pmem_map_file fail");
            exit(1);
        }
    }
    String genNewChunkFileName(){
        return fmt::format("{}/{}_{}.plog",_plog_path,_plog_id,_active_chunk_id);
    }
    void do_copy_to_non_pmem(char *addr, char *srcaddr, size_t len){
        char *startaddr = addr;
        memcpy(addr, srcaddr, len);
        /* Flush it */
        if (pmem_msync(startaddr, len) < 0) {
            perror("pmem_msync");
            exit(1);
        }
    
    }
    void do_copy_to_pmem(char *pmemaddr, char *srcaddr, size_t len){

        pmem_memcpy_nodrain(pmemaddr, srcaddr, len);
        /* Perform final flush step */
        pmem_drain();

    }

    int _is_pmem;
    String _plog_path;
    uint64_t _plog_id;
    uint64_t _tail_offset = 0;
    bool sealed = false;

    size_t _plog_max_size;
    size_t _chunk_max_size = 4096 * 1024;
    size_t _active_chunk_id = 0;
    std::vector<ChunkInfo> _chunk_list;
};

}// ns k2