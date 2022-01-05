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

#include <k2/pmemStorage/PmemLog.h>
#include <k2/pmemStorage/PmemEngine.h>
#include <libpmem.h>
#include "TPCCRawDataSchema.h"

enum TestOperation{
    SKVRecordConvert,
    PayloadConvert,
    PmemLoad,
}; 
class PmemDataLoader{
  public:
  PmemDataLoader(){
    std::string benchDir = "/mnt/pmem0/pmemlog-bench";
    _pmemEngineConfig.chunk_size = 16ULL << 20;
    _pmemEngineConfig.engine_capacity = 1ULL << 30;
    strcpy(_pmemEngineConfig.engine_path,benchDir.c_str());
    
    if(std::filesystem::exists(benchDir)){
        std::filesystem::remove_all(benchDir);
    }
    PmemEngine::open(_pmemEngineConfig, &_enginePtr);
  }
  ~PmemDataLoader(){
    delete _enginePtr;
  }
  
  template <class T>
  void testOperation(T& t, TestOperation testOp){
 
      dto::SKVRecord skv_record(t.collectionName, t.schema);
      t.__writeFields(skv_record);
     
     if( testOp == TestOperation::PayloadConvert){
          Payload payload(Payload::DefaultAllocator());
          payload.write(skv_record.getStorage());
          payload.seek(0);
      }
      if( testOp == TestOperation::PmemLoad){
          Payload payload(Payload::DefaultAllocator());
          payload.write(skv_record.getStorage());
          payload.seek(0);
          _enginePtr->append(payload);
      }
      
  }

  private:
    PmemEngine *_enginePtr = nullptr;
    PmemEngineConfig _pmemEngineConfig;

};


typedef std::vector<std::function<void(PmemDataLoader* loader, TestOperation testOp)>> TPCCRawData;
enum TPCCDataType{
    ItemData,
    WarehouseData,
};

struct TPCCRawDataGen {
public:

    TPCCRawDataGen(TPCCDataType tpccDataType = TPCCDataType::ItemData){
        _tpccDataType = tpccDataType;
    }

    TPCCRawData generateData(){
        if(_tpccDataType == TPCCDataType::ItemData){
            return generateItemData();
        }else if (_tpccDataType == TPCCDataType::WarehouseData){
            return generateWarehouseData();
        }
        return generateItemData();
    }
    bool hasData(){
        if(_tpccDataType == TPCCDataType::ItemData){
            return hasItem();
        }else if (_tpccDataType == TPCCDataType::WarehouseData){
            return hasWarehouse();
        }
        return hasItem();
    }
    TPCCRawDataGen configItemData(uint32_t itemDataCount = 100000){
        _itemDataCount = itemDataCount;
        _itemDataCurr = 0;
        return *this;
    }

    TPCCRawData generateItemData()
    {
        TPCCRawData data;
        data.reserve(_itemDataCount);
        RandomContext random(0);

        for (uint32_t i=1; i<=_itemDataCount; ++i) {
            auto item = Item(random, i);
            data.push_back([_item=std::move(item)] (PmemDataLoader* loader, TestOperation testOp) mutable {
              loader->testOperation(_item, testOp);
            });
        }
        _itemDataCurr += _itemDataCount;
        return data;
    }
    
    bool hasItem(){
        return _itemDataCurr < _itemDataCount;
    }

    void generateCustomerData(TPCCRawData& data, RandomContext& random, uint32_t w_id, uint16_t d_id)
    {
        for (uint16_t i=1; i <= _customers_per_district; ++i) {
            auto customer = Customer(random, w_id, d_id, i);

            // populate secondary index idx_customer_name
            auto idx_customer_name = IdxCustomerName(customer.WarehouseID.value(), customer.DistrictID.value(),
                customer.LastName.value(), customer.CustomerID.value());
            data.push_back([_idx_customer_name=std::move(idx_customer_name)] (PmemDataLoader* loader, TestOperation testOp) mutable {
                loader->testOperation(_idx_customer_name, testOp);
            });

            data.push_back([_customer=std::move(customer)] (PmemDataLoader* loader, TestOperation testOp) mutable {
                loader->testOperation(_customer, testOp);
            });

            auto history = History(random, w_id, d_id, i);
            data.push_back([_history=std::move(history)] (PmemDataLoader* loader, TestOperation testOp) mutable {
                loader->testOperation(_history, testOp);
            });
        }
    }

    void generateOrderData(TPCCRawData& data, RandomContext& random, uint32_t w_id, uint16_t d_id)
    {
        std::deque<uint32_t> permutationQueue(_customers_per_district);
        for (uint16_t i=0; i< _customers_per_district; ++i) {
            permutationQueue[i] = i + 1;
        }

        for (uint16_t i=1; i <= _customers_per_district; ++i) {
            uint32_t permutationIdx = random.UniformRandom(0, permutationQueue.size()-1);
            uint32_t c_id = permutationQueue[permutationIdx];
            permutationQueue.erase(permutationQueue.begin()+permutationIdx);

            auto order = Order(random, w_id, d_id, c_id, i);

            for (int j=1; j<=order.OrderLineCount; ++j) {
                auto order_line = OrderLine(random, order, j);
                data.push_back([_order_line=std::move(order_line)] (PmemDataLoader* loader, TestOperation testOp) mutable {
                    loader->testOperation(_order_line, testOp);
                });
            }

            if (i >= 2101) {
                auto new_order = NewOrder(order);
                data.push_back([_new_order=std::move(new_order)] (PmemDataLoader* loader, TestOperation testOp) mutable {
                    loader->testOperation(_new_order, testOp);
                });
            }

            auto idx_order_customer = IdxOrderCustomer(order.WarehouseID.value(), order.DistrictID.value(),
                                     order.CustomerID.value(), order.OrderID.value());

            data.push_back([_order=std::move(order)] (PmemDataLoader* loader, TestOperation testOp) mutable {
                loader->testOperation(_order, testOp);
            });

            // populate secondary index idx_order_customer
            data.push_back([_idx_order_customer=std::move(idx_order_customer)] (PmemDataLoader* loader, TestOperation testOp) mutable {
                loader->testOperation(_idx_order_customer, testOp);
            });
        }
    }
    TPCCRawDataGen configWarehouseData(uint32_t id_start, uint32_t id_end){
        _wareHouseDataIdStart = id_start;
        _wareHouseDataIdEnd = id_end;
        _wareHouseDataIdCurr = id_start;
        return *this;
    }

    bool hasWarehouse(){
        return _wareHouseDataIdCurr != _wareHouseDataIdEnd;
    }
    TPCCRawData generateWarehouseData(uint32_t step = 10)
    {
        TPCCRawData data;
        uint32_t _step = _wareHouseDataIdEnd - _wareHouseDataIdCurr;
        _step = _step <= step? _step:step;

        uint32_t num_warehouses = _step;
        size_t reserve_space = 0;
        // the warehouse data
        reserve_space += num_warehouses;
        // the stock data
        reserve_space += num_warehouses*100000;
        // the district data
        reserve_space += num_warehouses*_districts_per_warehouse;
        // customer data: customers_per_district: 3000 ,IdxCustomerName,Customer,History
        reserve_space += num_warehouses*_districts_per_warehouse*_customers_per_district*3;
        // order data: order,IdxOrderCustomer
        reserve_space += num_warehouses*_districts_per_warehouse*_customers_per_district*2;
        // order data: orderLine (on average)
        reserve_space += num_warehouses*_districts_per_warehouse*_customers_per_district*10;
        // order data: NewOrder
        reserve_space += num_warehouses*_districts_per_warehouse*(_customers_per_district - 2100);

        data.reserve(reserve_space);

        RandomContext random(_wareHouseDataIdCurr);

        for(uint32_t i = _wareHouseDataIdCurr; i < _wareHouseDataIdCurr + num_warehouses; ++i ){
            auto warehouse = Warehouse(random, i);

            data.push_back([_warehouse=std::move(warehouse)] (PmemDataLoader* loader, TestOperation testOp) mutable {
                loader->testOperation(_warehouse, testOp);
            });

            for (uint32_t j=1; j<100001; ++j) {
                auto stock = Stock(random, i, j);
                data.push_back([_stock=std::move(stock)] (PmemDataLoader* loader, TestOperation testOp) mutable {
                    loader->testOperation(_stock, testOp);
                });
            }

            for (uint16_t j=1; j <= _districts_per_warehouse; ++j) {
                auto district = District(random, i, j);
                data.push_back([_district=std::move(district)] (PmemDataLoader* loader, TestOperation testOp) mutable {
                    loader->testOperation(_district, testOp);
                });

                generateCustomerData(data, random, i, j);
                generateOrderData(data, random, i, j);
            }
        }
        _wareHouseDataIdCurr += num_warehouses;
	
        return data;
    }

private:
    TPCCDataType _tpccDataType;
    uint32_t _itemDataCount = 100000;
    uint32_t _itemDataCurr = 0;
    uint32_t _wareHouseDataIdCurr = 1;
    uint32_t _wareHouseDataIdStart = 1;
    uint32_t _wareHouseDataIdEnd = 2;
 
    int16_t _districts_per_warehouse = 10;
    uint32_t _customers_per_district = 3000;
};