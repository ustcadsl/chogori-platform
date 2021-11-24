
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
#include "PmemEngine.h"
#include "PmemLog.h"

namespace k2 {
    
Status PmemEngine::open(PmemEngineConfig &plog_meta, PmemEngine ** engine_ptr){
    if ( plog_meta.chunk_size > plog_meta.engine_capacity
        || plog_meta.is_sealed == true ){
        return PmemStatuses::S403_Forbidden_Invalid_Config;
    }
    PmemLog * engine = new PmemLog;
    Status s = engine->init(plog_meta);
    if (!s.is2xxOK()){
        K2LOG_E(log::pmem_storage,"Create PmemLog Failed!");
        delete engine;
    }else{
        *engine_ptr = engine;
    }
    return s;
}

}
