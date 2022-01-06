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
#include <utility>
#include <iostream>
#include <k2/pmemStorage/PmemLog.h>
#include <k2/pmemStorage/PmemEngine.h>
#include <libpmem.h>
#include "TPCCRawDataSchema.h"

enum TestOperation{
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
  

  int64_t testOperation(dto::SKVRecord& skv, TestOperation testOp){
      Payload payload(Payload::DefaultAllocator());
      payload.write(skv.getStorage());
      payload.seek(0);
      if( testOp == TestOperation::PmemLoad){
          _enginePtr->append(payload);
      }
      return payload.getSize();
      
  }

  private:
    PmemEngine *_enginePtr = nullptr;
    PmemEngineConfig _pmemEngineConfig;

};


typedef std::vector<std::shared_ptr<dto::SKVRecord>> TPCCRawData;

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
            auto skv_item = std::make_shared<dto::SKVRecord>(item.collectionName, item.schema);
            item.__writeFields(*skv_item.get());
            data.push_back(skv_item);
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

            auto skv_idx_customer_name = std::make_shared<dto::SKVRecord>(idx_customer_name.collectionName, idx_customer_name.schema);
            idx_customer_name.__writeFields(*skv_idx_customer_name);            
            data.push_back(skv_idx_customer_name);

            auto skv_customer = std::make_shared<dto::SKVRecord>(customer.collectionName, customer.schema);
            customer.__writeFields(*skv_customer);
            data.push_back(skv_customer);

            auto history = History(random, w_id, d_id, i);
            auto skv_history = std::make_shared<dto::SKVRecord>(history.collectionName, history.schema);
            history.__writeFields(*skv_history);
            data.push_back(skv_history);
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
                auto skv_order_line = std::make_shared<dto::SKVRecord>(order_line.collectionName, order_line.schema);
                order_line.__writeFields(*skv_order_line);
                data.push_back(skv_order_line);
            }

            if (i >= 2101) {
                auto new_order = NewOrder(order);
                auto skv_new_order = std::make_shared<dto::SKVRecord>(new_order.collectionName, new_order.schema);
                new_order.__writeFields(*skv_new_order);
                data.push_back(skv_new_order);
            }

            auto idx_order_customer = IdxOrderCustomer(order.WarehouseID.value(), order.DistrictID.value(),
                                     order.CustomerID.value(), order.OrderID.value());

            auto skv_order = std::make_shared<dto::SKVRecord>(order.collectionName, order.schema);
            order.__writeFields(*skv_order);            
            data.push_back(skv_order);
            
            auto skv_idx_order_customer = std::make_shared<dto::SKVRecord>(idx_order_customer.collectionName, idx_order_customer.schema);
            idx_order_customer.__writeFields(*skv_idx_order_customer);    
            // populate secondary index idx_order_customer
            data.push_back(skv_idx_order_customer);
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

            auto skv_warehouse = std::make_shared<dto::SKVRecord>(warehouse.collectionName, warehouse.schema);
            warehouse.__writeFields(*skv_warehouse);    
            data.push_back(skv_warehouse);

            for (uint32_t j=1; j<100001; ++j) {
                auto stock = Stock(random, i, j);

                auto skv_stock = std::make_shared<dto::SKVRecord>(stock.collectionName, stock.schema);
                stock.__writeFields(*skv_stock);    
                data.push_back(skv_stock);
            }

            for (uint16_t j=1; j <= _districts_per_warehouse; ++j) {
                auto district = District(random, i, j);

                auto skv_district = std::make_shared<dto::SKVRecord>(district.collectionName, district.schema);
                district.__writeFields(*skv_district);    
                data.push_back(skv_district);

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