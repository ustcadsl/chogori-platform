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

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/module/k23si/Module.h>
#include <k2/cpo/client/CPOClient.h>
#include <seastar/core/sleep.hh>

#include <k2/dto/K23SI.h>
#include <k2/dto/K23SIInspect.h>
#include <k2/dto/Collection.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include "Log.h"

#include <iostream>

namespace k2 {
using namespace dto;
struct DataRec {
    String f1;
    String f2;

    bool operator==(const DataRec& o) {
        return f1 == o.f1 && f2 == o.f2;
    }
    K2_DEF_FMT(DataRec, f1, f2);
};

std::atomic<uint32_t> cores_finished = 0;

class WriteAsyncTest {

public:  // application lifespan
    WriteAsyncTest() { K2LOG_I(log::k23si, "ctor");}
    ~WriteAsyncTest(){ K2LOG_I(log::k23si, "dtor");}

    seastar::future<dto::Timestamp> getTimeNow() {
        auto nsecsSinceEpoch = sys_now_nsec_count();
        return seastar::make_ready_future<dto::Timestamp>(dto::Timestamp(nsecsSinceEpoch, 1550647543, 1000));
    }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::k23si, "stop");
        return std::move(_testFuture);
    }

    seastar::future<> start(){
        
        _schemaName.append(std::to_string(seastar::this_shard_id()));
        // _collName.append(std::to_string(seastar::this_shard_id()));
        
        K2LOG_I(log::k23si, "start, schemaName={}, collname={}", _schemaName, _collName);
        // K2EXPECT(log::k23si, _k2ConfigEps().size(), 3);
        K2LOG_I(log::k23si, "nodepool size: {}", _k2ConfigEps().size());
        for (auto& ep: _k2ConfigEps()) {
            _k2Endpoints.push_back(RPC().getTXEndpoint(ep));
        }

        _cpo_client.init(_cpoConfigEp());
        _cpoEndpoint = RPC().getTXEndpoint(_cpoConfigEp());
        _persistence = std::make_shared<Persistence>();
        _testTimer.set_callback([this] {
            // _testFuture = seastar::make_ready_future()
            _testFuture = _persistence->start()
            .then([this] {
                K2LOG_I(log::k23si, "Creating test collection {}...", _collName);
                auto request = dto::CollectionCreateRequest{
                    .metadata{
                        .name = _collName,
                        .hashScheme = dto::HashScheme::HashCRC32C,
                        .storageDriver = dto::StorageDriver::K23SI,
                        .capacity{
                            .dataCapacityMegaBytes = 1000,
                            .readIOPs = 1000000,
                            .writeIOPs = 1000000
                        },
                        .retentionPeriod = Duration(1h)*90*24
                    },
                    .clusterEndpoints = _k2ConfigEps(),
                    .rangeEnds{}
                };
                return RPC().callRPC<dto::CollectionCreateRequest,dto::CollectionCreateResponse>
                        (dto::Verbs::CPO_COLLECTION_CREATE, request, *_cpoEndpoint, 1s);
            })
            .then([](auto&& response) {
                // response for collection create
                auto& [status, resp] = response;
                // K2EXPECT(log::k23si, status, Statuses::S201_Created);
                K2LOG_I(log::k23si, "Collection create response {}...", status);
                // wait for collection to get assigned
                return seastar::sleep(100ms);
            })
            .then([this] {
                // check to make sure the collection is assigned
                auto request = dto::CollectionGetRequest{.name = _collName};
                K2LOG_I(log::k23si, "Checking test collection {}...", _collName);
                return RPC().callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>
                    (dto::Verbs::CPO_COLLECTION_GET, request, *_cpoEndpoint, 100ms);
            })
            .then([this](auto&& response) {
                // check collection was assigned
                auto& [status, resp] = response;
                K2EXPECT(log::k23si, status, Statuses::S200_OK);
                _pgetter = dto::PartitionGetter(std::move(resp.collection));
            })
            .then([this] () {
                _schema.name = _schemaName;
                _schema.version = 1;
                _schema.fields = std::vector<dto::SchemaField> {
                        {dto::FieldType::STRING, "partition", false, false},
                        {dto::FieldType::STRING, "range", false, false},
                        {dto::FieldType::STRING, "f1", false, false},
                        {dto::FieldType::STRING, "f2", false, false},
                };

                _schema.setPartitionKeyFieldsByName(std::vector<String>{"partition"});
                _schema.setRangeKeyFieldsByName(std::vector<String> {"range"});

                K2LOG_I(log::k23si, "Creating schema {} in collection {}...", _schemaName, _collName);
                dto::CreateSchemaRequest request{ _collName, _schema };
                return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s);
            })
            .then([] (auto&& response) {
                auto& [status, resp] = response;
                K2EXPECT(log::k23si, status, Statuses::S200_OK);
            })
            .then([this] {return testPersistence(); })
            // .then([this] { return runScenario00(); })
            // .then([this] { return runScenario01(); })
            // .then([this] { return runScenario02(); })
            // .then([this] { return runScenario03(); })
            // .then([this] { return runScenario04(); })
            // .then([this] { return runScenario05(); })
            // .then([this] { return runScenario06(); })
            // .then([this] { return runScenario07(); })
            // .then([this] { return runScenario08(); })
            .then([this] { return runScenario09(); })
            // .then([this] { return runScenario09(); })
            // .then([this] { return runScenario09(); })
            // .then([this] { return runScenario09(); })
            // .then([this] { return runScenario09(); })
            // .then([this] { return runScenario10(); })
            // .then([this] { return runScenario11(); })    // for performance test
            // .then([this] { return runScenario12(); })    // for performance test
            // .then([this] { return runScenario13(); })
            // .then([this] { return runScenario14(); })
            .then([this] {
                K2LOG_I(log::k23si, "======= All tests passed ========");
                cores_finished++;
                return seastar::sleep(3s).discard_result();
            })
            .then([this] {
                if (cores_finished == seastar::smp::count && seastar::this_shard_id() == 0) {
                    seastar::engine().exit(0);
                }
            })
            .handle_exception([this](auto exc) {
                try {
                    std::rethrow_exception(exc);
                } catch (RPCDispatcher::RequestTimeoutException& exc) {
                    K2LOG_E(log::k23si, "======= Test failed due to timeout ========");
                    seastar::engine().exit(-1);
                } catch (std::exception& e) {
                    K2LOG_E(log::k23si, "======= Test failed with exception [{}] ========", e.what());
                    seastar::engine().exit(-1);
                }
            });
            // .finally([this] {
            //     K2LOG_I(log::k23si, "======= Test ended ========");
            //     seastar::engine().exit(exitcode);
            // });
        });

        _testTimer.arm(0ms);
        return seastar::make_ready_future();
    }

private:
    int exitcode = -1;
    ConfigVar<std::vector<String>> _k2ConfigEps{"k2_endpoints"};
    ConfigVar<String> _cpoConfigEp{"cpo_endpoint"};
    ConfigVar<uint32_t> _concurrentNum{"concurrent_num"};
    ConfigVar<uint32_t> _keySpace{"key_space"};
    ConfigVar<uint32_t> _txnsCount{"txns_count"};
    ConfigVar<uint32_t> _keysCount{"keys_count"};
    ConfigVar<bool> _singlePartition{"single_partition"};
    ConfigVar<bool> _writeAsync{"write_async"};
    ConfigVar<bool> _countLatency{"count_latency"};
    ConfigVar<bool> _enableConcurrentWrite{"enable_concurrent_write"};
    ConfigVar<bool> _clientTracking{"client_tracking"};

    std::vector<std::unique_ptr<k2::TXEndpoint>> _k2Endpoints;
    std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;

    seastar::timer<> _testTimer;
    seastar::future<> _testFuture = seastar::make_ready_future();

    CPOClient _cpo_client;
    std::shared_ptr<Persistence> _persistence;
    dto::PartitionGetter _pgetter;
    dto::Schema _schema;
    std::string _collName = "write_async_collection";
    std::string _schemaName = "schema";

    // request_id
    uint64_t id = 0;

    // latency
    // std::vector<long> _writeLatency;
    // std::vector<long> _endLatency;
    sm::metric_groups _metric_groups;
    k2::ExponentialHistogram _endLatency;
    k2::ExponentialHistogram _writeLatency;
    k2::ExponentialHistogram _txnLatency;

    void registerMetrics() {
        _metric_groups.clear();
        std::vector<sm::label_instance> labels;
        labels.push_back(sm::label_instance("total_cores", seastar::smp::count));

        _metric_groups.add_group("TestWriteAsync", {
            sm::make_histogram("end_latency", [this]{ return _endLatency.getHistogram();},
                    sm::description("Latency of end transactions"), labels),
            sm::make_histogram("write_latency", [this]{ return _writeLatency.getHistogram();},
                    sm::description("Latency of write Operations"), labels),
            sm::make_histogram("txn_latency", [this]{ return _txnLatency.getHistogram();},
                    sm::description("Latency of txn"), labels),                    
        });
    }

    seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
    doWrite(const dto::Key& key, const DataRec& data, const dto::K23SI_MTR& mtr, const dto::Key& trh, const String& cname, bool isDelete, bool isTRH, bool writeAsync=false, std::unordered_map<dto::Key, uint64_t>&& writeKeyIds={}, bool clientTracking=false) {

        SKVRecord record(cname, std::make_shared<k2::dto::Schema>(_schema));
        record.serializeNext<String>(key.partitionKey);
        record.serializeNext<String>(key.rangeKey);
        record.serializeNext<String>(data.f1);
        record.serializeNext<String>(data.f2);
        K2LOG_D(log::k23si, "cname={}, key={}, phash={}", cname, key, key.partitionHash())
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SIWriteRequest request {
            .pvid = part.partition->keyRangeV.pvid,
            .collectionName = cname,
            .mtr = mtr,
            .trh = trh,
            .trhCollection = cname,
            .isDelete = isDelete,
            .designateTRH = isTRH,
            .precondition = dto::ExistencePrecondition::None,
            .request_id = id++,
            .key = key,
            .value = std::move(record.storage),
            .fieldsForPartialUpdate = std::vector<uint32_t>(),
            .writeAsync = writeAsync
        };
        writeKeyIds[key] = request.request_id;
        if (clientTracking) {
            dto::DataRecord rec{.value=request.value.copy(), .timestamp=request.mtr.timestamp, .isTombstone=request.isDelete};
            _persistence->append(std::move(rec));
        }
        return RPC().callRPC<dto::K23SIWriteRequest, dto::K23SIWriteResponse>(dto::Verbs::K23SI_WRITE, request, *part.preferredEndpoint, 1000ms);
    }

    seastar::future<std::tuple<Status, DataRec>>
    doRead(const dto::Key& key, const dto::K23SI_MTR& mtr, const String& cname) {
        K2LOG_D(log::k23si, "key={}, phash={}", key, key.partitionHash())
        auto& part = _pgetter.getPartitionForKey(key);
        dto::K23SIReadRequest request {
            .pvid = part.partition->keyRangeV.pvid,
            .collectionName = cname,
            .mtr =mtr,
            .key=key
        };

        return RPC().callRPC<dto::K23SIReadRequest, dto::K23SIReadResponse>
            (dto::Verbs::K23SI_READ, request, *part.preferredEndpoint, 100ms)
        .then([this] (auto&& response) {
            auto& [status, resp] = response;
            if (!status.is2xxOK()) {
                return std::make_tuple(std::move(status), DataRec{});
            }

            SKVRecord record(_collName, std::make_shared<k2::dto::Schema>(_schema), std::move(resp.value), true);
            record.seekField(2);
            DataRec rec = { *(record.deserializeNext<String>()), *(record.deserializeNext<String>()) };
            return std::make_tuple(std::move(status), std::move(rec));
        });
    }

    seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
    doEnd(dto::Key trh, dto::K23SI_MTR mtr, const String& cname, bool isCommit, std::vector<dto::Key> writeKeys, std::unordered_map<dto::Key, uint64_t> writeKeyIds) {
        K2LOG_D(log::k23si, "key={}, phash={}", trh, trh.partitionHash())
        auto& part = _pgetter.getPartitionForKey(trh);
        std::unordered_map<String, std::unordered_set<dto::KeyRangeVersion>> writeRanges;

        for (auto& key: writeKeys) {
            auto& krv = _pgetter.getPartitionForKey(key).partition->keyRangeV;
            writeRanges[cname].insert(krv);
        }

        std::unordered_map<String, std::unordered_map<dto::Key, uint64_t>> writeIds;
        writeIds[cname] = std::move(writeKeyIds);

        dto::K23SITxnEndRequest request;
        request.pvid = part.partition->keyRangeV.pvid;
        request.collectionName = cname;
        request.mtr = mtr;
        request.key = trh;
        request.action = isCommit ? dto::EndAction::Commit : dto::EndAction::Abort;
        request.writeRanges = std::move(writeRanges);
        request.writeIds = std::move(writeIds);
        return RPC().callRPC<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>(dto::Verbs::K23SI_TXN_END, request, *part.preferredEndpoint, 1000ms);
    }


    seastar::future<std::tuple<Status, dto::K23SIInspectRecordsResponse>>
    doRequestRecords(dto::Key key) {
        auto* request = new dto::K23SIInspectRecordsRequest {
            dto::PVID{}, // Will be filled in by PartitionRequest
            k2::String(_collName),
            std::move(key)
        };

        return _cpo_client.partitionRequest
            <dto::K23SIInspectRecordsRequest, dto::K23SIInspectRecordsResponse, dto::Verbs::K23SI_INSPECT_RECORDS>
            (Deadline<>(1s), *request).
            finally([request] () { delete request; });
    }

    seastar::future<std::tuple<Status, dto::K23SIInspectTxnResponse>>
    doRequestTRH(dto::Key trh, dto::K23SI_MTR mtr) {
        auto* request = new dto::K23SIInspectTxnRequest {
            dto::PVID{}, // Will be filled in by PartitionRequest
            k2::String(_collName),
            std::move(trh),
            mtr.timestamp
        };

        return _cpo_client.partitionRequest
            <dto::K23SIInspectTxnRequest, dto::K23SIInspectTxnResponse, dto::Verbs::K23SI_INSPECT_TXN>
            (Deadline<>(1s), *request).
            finally([request] () { delete request; });
    }

    seastar::future<> clearContext() {
        id = 0;
        return seastar::sleep(1s);
        // return seastar::make_ready_future<>();
    }

    std::string join(std::vector<long> latency, std::string delem = ", ") {
        std::string res = "";
        for (auto& s : latency) {
            res += (std::to_string(s) + delem);
        }
        return res;
    }

public: // tests

seastar::future<> move_param(dto::Key&& key) {
    K2LOG_I(log::k23si, "key : {}", key);
    return seastar::make_ready_future<>();
}

seastar::future<> testPersistence() {
    K2LOG_I(log::k23si, "starting testPersistence...");
    String data1 = "hello world";
    String data2 = "test persistence";
    auto fut1 = _persistence->append_cont(data1).then([data1=std::move(data1)] (auto&& status) {
        K2LOG_I(log::k23si, "data1 = {} has been flushed with status = {}.", data1, status);
        return seastar::make_ready_future(); 
    });
    auto fut2 = _persistence->append_cont(data2).then([data2=std::move(data2)] (auto&& status) {
        K2LOG_I(log::k23si, "data2 = {} has been flushed with status = {}.", data2, status);
        return seastar::make_ready_future();
    });    
    return _persistence->flush().then([this] (auto&& status) {
        K2LOG_I(log::k23si, "flush status = {}", status);
        return seastar::make_ready_future();
    });
}

seastar::future<> runTransactionsWithClientTracking(String prefix = "run_trans", bool writeAsync = false, int keysCount = 30, bool singlePartition = false, bool countLatency = false, int keySpace = 30) {
    return seastar::make_ready_future()
        .then([this] {
            return getTimeNow();
        })
        .then([&] (dto::Timestamp&& ts) {
            k2::OperationLatencyReporter reporter(_txnLatency);
            std::unordered_map<dto::Key, uint64_t> writeKeyIds;
            return seastar::do_with(
                dto::K23SI_MTR{
                    .timestamp = std::move(ts),
                    .priority = dto::TxnPriority::Medium},
                dto::Key{.schemaName = _schemaName, .partitionKey = prefix + "pkey1", .rangeKey = "rKey1"},
                dto::Key{.schemaName = _schemaName, .partitionKey = prefix + "pkey1", .rangeKey = "rKey1"},
                DataRec{.f1="field1", .f2="field2"},
                std::move(reporter),
                std::move(writeKeyIds),
                prefix,
                writeAsync,
                keysCount,
                singlePartition,
                countLatency,
                keySpace,
                [this] (dto::K23SI_MTR& mtr, dto::Key& key1, dto::Key& trh, DataRec& rec, auto& txnReporter, auto& writeKeyIds, String& prefix, auto& writeAsync, auto& keysCount, auto& singlePartition, auto& countLatency, auto& keySpace) mutable {
                    // K2LOG_I(log::k23si, "prefix={}, writeAsync={}, keysCount={}, singlePartition={}", prefix, writeAsync, keysCount, singlePartition);
                    // k2::OperationLatencyReporter reporter(_writeLatency);
                    (void) key1;
                    // (void) keySpace;
                    // (void) txnReporter;
                    seastar::future<Status> fut = seastar::make_ready_future<Status>(dto::K23SIStatus::Created);
                    for (int i = 1; i <= keysCount; i++) {
                        int rd = rand() % keySpace;
                        // int rd = i % keySpace;
                        dto::Key key{
                            .schemaName = _schemaName,
                            .partitionKey = prefix + (singlePartition ? "pkey1" : "pkey" + std::to_string(i)),
                            .rangeKey = "rkey" + std::to_string(rd)
                        };
                        bool isTrh = i == 1;
                        fut = fut.then([this, key=std::move(key), &rec, &mtr, &trh, &writeAsync, &countLatency, &writeKeyIds, isTrh=std::move(isTrh)] (auto&& status) {
                            // K2LOG_I(log::k23si, "write key: {}", key);
                            if (!status.is2xxOK()) {
                                return seastar::make_ready_future<Status>(status);
                            }
                            k2::OperationLatencyReporter reporter(_writeLatency);
                            return doWrite(key, rec, mtr, trh, _collName, false, isTrh, writeAsync, std::move(writeKeyIds), true)
                                .then([this, reporter=std::move(reporter)] (auto&& response) mutable {
                                    auto& [status, resp] = response;
                                    // K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                                    reporter.report();
                                    return seastar::make_ready_future<Status>(status);
                                });
                        });
                    }
                    return fut.then([&] (auto&& status) {
                        return _persistence->flush().then([this, status=std::move(status)] (auto&& flushStatus) {
                            K2EXPECT(log::k23si, flushStatus, dto::K23SIStatus::OK);
                            return seastar::make_ready_future<Status>(status);
                        });
                    }).then([&] (auto&& status) {
                        K2LOG_D(log::k23si, "write status = {} for mtr = {}", status, mtr);
                        std::vector<dto::Key> endKeys;
                        for (auto& it : writeKeyIds) {
                            endKeys.push_back(it.first);
                        }
                        k2::OperationLatencyReporter reporter(_endLatency);
                        // K2LOG_I(log::k23si, "writeKeyIds={}", writeKeyIds); 
                        // K2LOG_I(log::k23si, "canCommit={}", status.is2xxOK());
                        return doEnd(trh, mtr, _collName, status.is2xxOK(), endKeys, writeKeyIds)
                            .then([this, reporter=std::move(reporter)] (auto&& response) mutable {
                                auto& [status, resp] = response;
                                // K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                                // K2LOG_D(log::k23si, "end status = {}", status);
                                if (!status.is2xxOK()) {
                                    K2LOG_D(log::k23si, "end status = {}", status);
                                }
                                reporter.report();            
                            });
                    }).then([&] () {
                        txnReporter.report();
                        return seastar::make_ready_future<>();
                    });
            });
    });
}

seastar::future<> runTransactionsWithoutConcurrentWrite(String prefix = "run_trans", bool writeAsync = false, int keysCount = 30, bool singlePartition = false, bool countLatency = false, int keySpace = 30) {
    return seastar::make_ready_future()
        .then([this] {
            return getTimeNow();
        })
        .then([&] (dto::Timestamp&& ts) {
            k2::OperationLatencyReporter reporter(_txnLatency);
            std::unordered_map<dto::Key, uint64_t> writeKeyIds;
            return seastar::do_with(
                dto::K23SI_MTR{
                    .timestamp = std::move(ts),
                    .priority = dto::TxnPriority::Medium},
                dto::Key{.schemaName = _schemaName, .partitionKey = prefix + "pkey1", .rangeKey = "rKey1"},
                dto::Key{.schemaName = _schemaName, .partitionKey = prefix + "pkey1", .rangeKey = "rKey1"},
                DataRec{.f1="field1", .f2="field2"},
                std::move(reporter),
                std::move(writeKeyIds),
                prefix,
                writeAsync,
                keysCount,
                singlePartition,
                countLatency,
                keySpace,
                [this] (dto::K23SI_MTR& mtr, dto::Key& key1, dto::Key& trh, DataRec& rec, auto& txnReporter, auto& writeKeyIds, String& prefix, auto& writeAsync, auto& keysCount, auto& singlePartition, auto& countLatency, auto& keySpace) mutable {
                    // K2LOG_I(log::k23si, "prefix={}, writeAsync={}, keysCount={}, singlePartition={}", prefix, writeAsync, keysCount, singlePartition);
                    // k2::OperationLatencyReporter reporter(_writeLatency);
                    (void) key1;
                    // (void) keySpace;
                    // (void) txnReporter;
                    seastar::future<Status> fut = seastar::make_ready_future<Status>(dto::K23SIStatus::Created);
                    for (int i = 1; i <= keysCount; i++) {
                        int rd = rand() % keySpace;
                        dto::Key key{
                            .schemaName = _schemaName,
                            .partitionKey = prefix + (singlePartition ? "pkey1" : "pkey" + std::to_string(i)),
                            .rangeKey = "rkey" + std::to_string(rd)
                        };
                        bool isTrh = i == 1;
                        fut = fut.then([this, key=std::move(key), &rec, &mtr, &trh, &writeAsync, &countLatency, &writeKeyIds, isTrh=std::move(isTrh)] (auto&& status) {
                            // K2LOG_I(log::k23si, "write key: {}", key);
                            if (!status.is2xxOK()) {
                                return seastar::make_ready_future<Status>(status);
                            }
                            k2::OperationLatencyReporter reporter(_writeLatency);
                            return doWrite(key, rec, mtr, trh, _collName, false, isTrh, writeAsync, std::move(writeKeyIds))
                                .then([this, reporter=std::move(reporter)] (auto&& response) mutable{
                                    auto& [status, resp] = response;
                                    // K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                                    reporter.report();
                                    return seastar::make_ready_future<Status>(status);
                                });
                        });
                    }
                    return fut.then([&] (auto&& status) {
                        K2LOG_D(log::k23si, "write status = {} for mtr = {}", status, mtr);
                        std::vector<dto::Key> endKeys;
                        for (auto& it : writeKeyIds) {
                            endKeys.push_back(it.first);
                        }
                        k2::OperationLatencyReporter reporter(_endLatency);
                        // K2LOG_I(log::k23si, "writeKeyIds={}", writeKeyIds); 
                        // K2LOG_I(log::k23si, "canCommit={}", status.is2xxOK());                              
                        return doEnd(trh, mtr, _collName, status.is2xxOK(), endKeys, writeKeyIds)
                            .then([this, reporter=std::move(reporter)] (auto&& response) mutable{
                                auto& [status, resp] = response;
                                // K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                                // K2LOG_D(log::k23si, "end status = {}", status);
                                if (!status.is2xxOK()) {
                                    K2LOG_D(log::k23si, "end status = {}", status);
                                }
                                reporter.report();            
                            });
                    }).then([&] () {
                        txnReporter.report();
                        return seastar::make_ready_future<>();
                    });
            });
    });
}

seastar::future<> runTransactionsWithConcurrentWrite(String prefix = "run_trans", bool writeAsync = false, int keysCount = 30, bool singlePartition = false, bool countLatency = false, int keySpace = 30) {
    return seastar::make_ready_future()
        .then([this] {
            return getTimeNow();
        })
        .then([&] (dto::Timestamp&& ts) {
            k2::OperationLatencyReporter reporter(_txnLatency);
            std::unordered_map<dto::Key, uint64_t> writeKeyIds;
            return seastar::do_with(
                dto::K23SI_MTR{
                    .timestamp = std::move(ts),
                    .priority = dto::TxnPriority::Medium},
                dto::Key{.schemaName = _schemaName, .partitionKey = prefix + "pkey1", .rangeKey = "rKey1"},
                dto::Key{.schemaName = _schemaName, .partitionKey = prefix + "pkey1", .rangeKey = "rKey1"},
                DataRec{.f1="field1", .f2="field2"},
                std::move(reporter),
                std::move(writeKeyIds),
                prefix,
                writeAsync,
                keysCount,
                singlePartition,
                countLatency,
                keySpace,
                [this] (dto::K23SI_MTR& mtr, dto::Key& key1, dto::Key& trh, DataRec& rec, auto& txnReporter, auto& writeKeyIds, String& prefix, auto& writeAsync, auto& keysCount, auto& singlePartition, auto& countLatency, auto& keySpace) mutable {
                    // K2LOG_I(log::k23si, "prefix={}, writeAsync={}, keysCount={}, singlePartition={}", prefix, writeAsync, keysCount, singlePartition);
                    // k2::OperationLatencyReporter reporter(_writeLatency);
                    (void) key1;
                    // (void) keySpace;
                    std::vector<seastar::future<Status>> writes;
                    for (int i = 1; i <= keysCount; i++) {
                        int rd = rand() % keySpace;
                        dto::Key key{
                            .schemaName = _schemaName,
                            .partitionKey = prefix + (singlePartition ? "pkey1" : "pkey" + std::to_string(i)),
                            .rangeKey = "rkey" + std::to_string(rd)
                        };
                        seastar::future<Status> fut = seastar::make_ready_future<Status>(dto::K23SIStatus::Created);
                        bool isTrh = i == 1;
                        fut = fut.then([this, key=std::move(key), &rec, &mtr, &trh, &writeAsync, &countLatency, &writeKeyIds, isTrh=std::move(isTrh)] (auto&& status) {
                            // K2LOG_I(log::k23si, "write key: {}", key);
                            (void) status;
                            k2::OperationLatencyReporter reporter(_writeLatency);
                            return doWrite(key, rec, mtr, trh, _collName, false, isTrh, writeAsync, std::move(writeKeyIds))
                                .then([this, reporter=std::move(reporter)] (auto&& response) mutable {
                                    auto& [status, resp] = response;
                                    // K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                                    reporter.report();
                                    return seastar::make_ready_future<Status>(status); 
                                });
                        });
                        writes.push_back(std::move(fut));
                    }
                    return seastar::when_all_succeed(writes.begin(), writes.end())
                        .then([&] (auto&& doneStatuses) {
                            // K2LOG_I(log::k23si, "issuing the end request");
                            bool canCommit = true;
                            for (auto& status : doneStatuses) {
                                // auto& [status, resp] = doneStatus;
                                if (!status.is2xxOK()) {
                                    canCommit = false;
                                    break;
                                }
                            }
                            std::vector<dto::Key> endKeys;
                            for (auto& it : writeKeyIds) {
                                endKeys.push_back(it.first);
                            }
                            k2::OperationLatencyReporter reporter(_endLatency);
                            // K2LOG_I(log::k23si, "writeKeyIds={}", writeKeyIds);  
                            // K2LOG_I(log::k23si, "canCommit={}", canCommit);                              
                            return doEnd(trh, mtr, _collName, canCommit, endKeys, writeKeyIds)
                                .then([&, reporter=std::move(reporter)] (auto&& response) mutable {
                                    auto& [status, resp] = response;
                                    // K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                                    if (!status.is2xxOK()) {
                                        K2LOG_D(log::k23si, "end status = {}", status);
                                    }
                                    reporter.report();
                                });
                        })
                        .then([&] () {
                            txnReporter.report();
                            // return seastar::sleep(10ms);
                        });                    
            });
    });
}

seastar::future<> runScenario09() {
    K2LOG_I(log::k23si, "Scenario 09: test ops with multiple transactions");
    registerMetrics();
    auto startTp = k2::Clock::now();
    
    K2LOG_I(log::k23si, "startTp: {}", startTp.time_since_epoch().count());
    int concurrentNum = _concurrentNum();
    int keySpace = _keySpace();
    int transactionsCount = _txnsCount();
    int keysCount = _keysCount();
    bool singlePartition = _singlePartition();
    bool writeAsync = _writeAsync();
    bool countLatency = _countLatency();
    bool enableConcurrentWrite = _enableConcurrentWrite();
    bool clientTracking = _clientTracking();
    return seastar::do_with(
        std::move(concurrentNum),
        std::move(transactionsCount),
        std::move(keysCount),
        std::move(singlePartition),
        std::move(writeAsync),
        std::move(countLatency),
        std::move(keySpace),
        std::move(enableConcurrentWrite),
        std::move(clientTracking),
        std::move(startTp),
        [this] (auto& concurrentNum, auto& transactionsCount, auto& keysCount, auto& singlePartition, auto& writeAsync, auto& countLatency, auto& keySpace, auto& enableConcurrentWrite, auto& clientTracking, auto& startTp) {
            std::vector<seastar::future<>> futs;
            int shard_id = seastar::this_shard_id();
            // int cpu_count = seastar::smp::count;
            for (int num = 0; num < concurrentNum; num += 1) {
                seastar::future<> fut = seastar::make_ready_future<>();
                for (int i = 0; i < transactionsCount; i++) {
                    // IMPORTANT: no conflict
                    // String prefix {"test_ops_" + std::to_string(shard_id) + "_" + std::to_string(num) + "_" + std::to_string(i)};
                    String prefix {"test_ops"};
                    (void) shard_id;
                    fut = fut.then([&, prefix=std::move(prefix)] () {
                        if (clientTracking) {
                            return runTransactionsWithClientTracking(prefix, writeAsync, keysCount, singlePartition, countLatency, keySpace);
                        } else if (enableConcurrentWrite) {
                            return runTransactionsWithConcurrentWrite(prefix, writeAsync, keysCount, singlePartition, countLatency, keySpace);
                        }
                        return runTransactionsWithoutConcurrentWrite(prefix, writeAsync, keysCount, singlePartition, countLatency, keySpace);
                    });
                }
                futs.push_back(std::move(fut));
            }
            return seastar::when_all(futs.begin(), futs.end()).discard_result().then([&] () {
                K2LOG_I(log::k23si, "endTp: {}, shard_id: {}", k2::Clock::now().time_since_epoch().count(), seastar::this_shard_id());
                auto duration = k2::Clock::now() - startTp;
                auto totalsecs = ((double)k2::msec(duration).count()) / 1000.0;
                auto totalKeys = keysCount * transactionsCount * concurrentNum;
                K2LOG_I(log::k23si, "time spent for testing ops with multiple transactions: {} secs, write ops: {}", totalsecs, totalKeys / totalsecs);
                // K2LOG_I(log::k23si, "write latency size = {}, write latency = [{}]", _writeLatency.size(), _writeLatency);
                // K2LOG_I(log::k23si, "end latency size = {}, end latency = {}", _endLatency.size(), _endLatency);
                return clearContext();
            });
        });

    // return seastar::do_with(
    //     std::move(startTp),
    //     std::move(fut),
    //     std::move(transactionsCount * keysCount),
    //     [&] (auto& startTp, auto& fut, auto& totalKeys) {
    //         return fut.then([&] () {        
    //             K2LOG_I(log::k23si, "endTp: {}", k2::Clock::now().time_since_epoch().count());
    //             auto duration = k2::Clock::now() - startTp;
    //             auto totalsecs = ((double)k2::msec(duration).count()) / 1000.0;
    //             K2LOG_I(log::k23si, "time spent for testing ops with multiple transactions: {} secs, write ops: {}", totalsecs, totalKeys / totalsecs);
    //             return clearContext();
    //         });
    //     }
    // );

}



};  // class WriteAsyncTest
} // ns k2

int main(int argc, char** argv) {
    k2::App app("WriteAsyncTest");
    app.addOptions()("k2_endpoints", bpo::value<std::vector<k2::String>>()->multitoken(), "The endpoints of the k2 cluster");
    app.addOptions()("cpo_endpoint", bpo::value<k2::String>(), "The endpoint of the CPO");
    app.addOptions()("concurrent_num", bpo::value<uint32_t>()->default_value(1), "how many txns concurrently sent");
    app.addOptions()("key_space", bpo::value<uint32_t>()->default_value(30), "control the conflict rate, the bigger key_space, the less conflict");
    app.addOptions()("txns_count", bpo::value<uint32_t>()->default_value(100), "how many txns to test");
    app.addOptions()("keys_count", bpo::value<uint32_t>()->default_value(50), "how many writes to test per txn");
    app.addOptions()("single_partition", bpo::value<bool>()->default_value(true), "whether to write in one partition only");
    app.addOptions()("write_async", bpo::value<bool>()->default_value(true), "whether to write in async manner");
    app.addOptions()("count_latency", bpo::value<bool>()->default_value(false), "whether to count latency of write and end opration");
    app.addOptions()("enable_concurrent_write", bpo::value<bool>()->default_value(false), "whether to enable concurrent write in a txn");
    app.addOptions()("client_tracking", bpo::value<bool>()->default_value(false), "whether to enable client tracking");
    app.addOptions()("k23si_persistence_endpoints", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A space-delimited list of k2 persistence endpoints, each core will pick one endpoint");
    app.addApplet<k2::WriteAsyncTest>();
    return app.start(argc, argv);
}
