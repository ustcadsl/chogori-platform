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

#include "InstancePlog.h"

namespace k2 {
    void LocalPlog::append(std::unique_ptr<Payload>& request_payload){
        if( _sealed ){
            return ;
        }
        size_t payload_data_size = request_payload->getSize();
        char * buffer_data = (char*)malloc(payload_data_size * sizeof(char));
        request_payload->read(buffer_data, payload_data_size);
        append(buffer_data,payload_data_size);
        free(buffer_data);
        return;
    }
    void LocalPlog::append(char *srcdata, size_t len){
        K2LOG_D(log::skvsvr,"Append data: offset=>{} size=>{} ", _tail_offset, len);
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
                String chunkFileName = gen_new_chunk_filename();
                char * chunkFileAddr = create_new_chunk(chunkFileName);
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
} // namespace k2
