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

#include <benchmark/benchmark.h>
#include <cstdarg>
#include <filesystem>
#include <k2/dto/SKVRecord.h>

#include "TPCCRawDataGenerator.h"
//  this is a copy from the datagen in tpcc

// test the raw memcpy latency
static void BM_memcpy(benchmark::State& state) {
  char* src = new char[state.range(0)];
  char* dst = new char[2U << 30];
  memset(src, 'x', state.range(0));
  size_t offset = 0;
  for (auto _ : state){
    memcpy(dst+offset, src, state.range(0));
    offset = (offset+state.range(0)) % (2U<<30);
  }
  state.SetBytesProcessed(int64_t(state.iterations()) *
                          int64_t(state.range(0)));
  delete[] src;
  delete[] dst;
}
BENCHMARK(BM_memcpy)->Arg(64)->Arg(128)->Arg(256)->Arg(512)->Arg(1<<10)->Arg(8<<10);

static void BM_pmemcpy(benchmark::State& state) {
  char* src = new char[state.range(0)];
  char* dst; size_t mappedLen; int isPmem;
  std::string filePath =  "/mnt/pmem0/pmemlog-bench/tmp.file";
  if(std::filesystem::exists(filePath)){
        std::filesystem::remove_all(filePath);
    }
  dst = static_cast<char *>(pmem_map_file(
                        filePath.c_str(),
                        2U<<30,
                        PMEM_FILE_CREATE | PMEM_FILE_EXCL,
                        0666, &mappedLen, &isPmem));
  if (dst == NULL){
    std::cout << "error map " << std::endl;
    return;
  }
  memset(src, 'x', state.range(0));
  size_t offset = 0;
  for (auto _ : state){
    pmem_memcpy_persist(dst+offset, src, state.range(0));
    offset = (offset+state.range(0)) % (2U<<30);
  }
  state.SetBytesProcessed(int64_t(state.iterations()) *
                          int64_t(state.range(0)));
  delete[] src;
  pmem_unmap(dst, 8<<20);
}
BENCHMARK(BM_pmemcpy)->Arg(64)->Arg(128)->Arg(256)->Arg(512)->Arg(1<<10)->Arg(8<<10);

static void BM_Convert_Payload_Item_Data(benchmark::State& state) {
  PmemDataLoader pmemLoader;
  TPCCRawDataGen tpccDataGen(TPCCDataType::ItemData);
  tpccDataGen.configItemData(100000);
  setupSchemaPointers();
  auto tpccRawData = tpccDataGen.generateData();
  size_t skv_space = tpccRawData.size();
  int i = 0;
  for (auto _ : state) {
      i = (i+1)%skv_space;
      pmemLoader.testOperation(*tpccRawData[i], TestOperation::PayloadConvert);
  }
  state.SetItemsProcessed(int64_t(state.iterations()));
}
BENCHMARK(BM_Convert_Payload_Item_Data);

static void BM_Write_Item_Data(benchmark::State& state) {
  PmemDataLoader pmemLoader;
  TPCCRawDataGen tpccDataGen(TPCCDataType::ItemData);
  tpccDataGen.configItemData(100000);
  setupSchemaPointers();
  auto tpccRawData = tpccDataGen.generateData();
  size_t skv_space = tpccRawData.size();
  int i = 0;
  for (auto _ : state) {
      i = (i+1)%skv_space;
      pmemLoader.testOperation(*tpccRawData[i], TestOperation::PmemLoad);
  }
  state.SetItemsProcessed(int64_t(state.iterations()));
}
BENCHMARK(BM_Write_Item_Data);

static void BM_Convert_Payload_Warehouse_Data(benchmark::State& state) {
  PmemDataLoader pmemLoader;
  TPCCRawDataGen tpccDataGen(TPCCDataType::WarehouseData);
  tpccDataGen.configWarehouseData(1,2);
  setupSchemaPointers();
  auto tpccRawData = tpccDataGen.generateData();
  size_t skv_space = tpccRawData.size();
  int i = 0;
  for (auto _ : state) {
      i = (i+1)%skv_space;
      pmemLoader.testOperation(*tpccRawData[i], TestOperation::PayloadConvert);
  }
  state.SetItemsProcessed(int64_t(state.iterations()));
}
BENCHMARK(BM_Convert_Payload_Warehouse_Data);

static void BM_Write_Warehouse_Data(benchmark::State& state) {
  PmemDataLoader pmemLoader;
  TPCCRawDataGen tpccDataGen(TPCCDataType::WarehouseData);
  tpccDataGen.configWarehouseData(1,2);
  setupSchemaPointers();
  auto tpccRawData = tpccDataGen.generateData();
  size_t skv_space = tpccRawData.size();
  int i = 0;
  for (auto _ : state) {
      i = (i+1)%skv_space;
      pmemLoader.testOperation(*tpccRawData[i], TestOperation::PmemLoad);
  }
  state.SetItemsProcessed(int64_t(state.iterations()));
}
BENCHMARK(BM_Write_Warehouse_Data);


int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
}