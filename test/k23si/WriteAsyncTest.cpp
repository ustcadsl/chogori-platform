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
        _testTimer.set_callback([this] {
            _testFuture = seastar::make_ready_future()
            .then([this] {
                K2LOG_I(log::k23si, "Creating test collection {}...", _collName);
                auto request = dto::CollectionCreateRequest{
                    .metadata{
                        .name = _collName,
                        .hashScheme = dto::HashScheme::HashCRC32C,
                        .storageDriver = dto::StorageDriver::K23SI,
                        .capacity{
                            .dataCapacityMegaBytes = 1000,
                            .readIOPs = 100000,
                            .writeIOPs = 100000
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
    ConfigVar<uint32_t> _txnsCount{"txns_count"};
    ConfigVar<uint32_t> _keysCount{"keys_count"};
    ConfigVar<bool> _singlePartition{"single_partition"};
    ConfigVar<bool> _writeAsync{"write_async"};
    ConfigVar<bool> _countLatency{"count_latency"};

    std::vector<std::unique_ptr<k2::TXEndpoint>> _k2Endpoints;
    std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;

    seastar::timer<> _testTimer;
    seastar::future<> _testFuture = seastar::make_ready_future();

    CPOClient _cpo_client;
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
    doWrite(const dto::Key& key, const DataRec& data, const dto::K23SI_MTR& mtr, const dto::Key& trh, const String& cname, bool isDelete, bool isTRH, bool writeAsync=false) {

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
        if (writeAsync) {
            auto& trhPart = _pgetter.getPartitionForKey(trh);
            dto::K23SIWriteKeyRequest writeKeyRequest {
                .pvid = trhPart.partition->keyRangeV.pvid,
                .collectionName = cname,
                .mtr = mtr,
                .key = trh,
                .writeKey = key,
                .writeRange = part.partition->keyRangeV,
                .request_id = request.request_id
            };
            return seastar::when_all(
                RPC().callRPC<dto::K23SIWriteRequest, dto::K23SIWriteResponse>(dto::Verbs::K23SI_WRITE, request, *part.preferredEndpoint, 1000ms),
                RPC().callRPC<dto::K23SIWriteKeyRequest, dto::K23SIWriteKeyResponse>(dto::Verbs::K23SI_WRITE_KEY, writeKeyRequest, *trhPart.preferredEndpoint, 1000ms)
            ).then([this, request=std::move(request), &key] (auto&& response) {
                auto& [writeResp, writeKeyResp] = response;
                auto [writeKeyStatus, _] = writeKeyResp.get0();
                auto [writeStatus, writeRes] = writeResp.get0();

                if (!writeKeyStatus.is2xxOK()) {
                    K2LOG_E(log::k23si, "write key failed with key {}, status {}", key, writeKeyStatus);
                    return seastar::make_ready_future<std::tuple<Status, dto::K23SIWriteResponse>>(writeKeyStatus, dto::K23SIWriteResponse{});
                }
                return seastar::make_ready_future<std::tuple<Status, dto::K23SIWriteResponse>>(writeStatus, dto::K23SIWriteResponse{});
            });
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
    doEnd(dto::Key trh, dto::K23SI_MTR mtr, const String& cname, bool isCommit, std::vector<dto::Key> writeKeys) {
        K2LOG_D(log::k23si, "key={}, phash={}", trh, trh.partitionHash())
        auto& part = _pgetter.getPartitionForKey(trh);
        std::unordered_map<String, std::unordered_set<dto::KeyRangeVersion>> writeRanges;

        for (auto& key: writeKeys) {
            auto& krv = _pgetter.getPartitionForKey(key).partition->keyRangeV;
            writeRanges[cname].insert(krv);
        }
        dto::K23SITxnEndRequest request;
        request.pvid = part.partition->keyRangeV.pvid;
        request.collectionName = cname;
        request.mtr = mtr;
        request.key = trh;
        request.action = isCommit ? dto::EndAction::Commit : dto::EndAction::Abort;
        request.writeRanges = std::move(writeRanges);
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

seastar::future<> runScenario00() {
    K2LOG_I(log::k23si, "Scenario 00: feature test");
    return seastar::make_ready_future()
        .then([this] {
            return dto::Key {
                .schemaName = _schemaName,
                .partitionKey = "pKey1",
                .rangeKey = "rKey1"
            };
        })
        .then([&] (dto::Key&& k) {
            dto::Key key{};
            key.schemaName = k.schemaName;
            key.partitionKey = k.partitionKey;
            key.rangeKey = k.rangeKey;
            k.schemaName.append("append", 6);
            K2LOG_I(log::k23si, "key : {}", key);
            K2LOG_I(log::k23si, "k : {}", k);
            std::cout << &k.schemaName << " " << &key.schemaName << std::endl;
            std::cout << &k.partitionKey << " " << &key.partitionKey << std::endl;
            // return move_param(std::move(k));
            return move_param(std::move(k)).then([&] () {
                return seastar::make_ready_future<dto::Key>(std::move(key));
            });
        })
        .then([&] (auto&& key) {
            K2LOG_I(log::k23si, "key now: {}", key);        
        });
}

seastar::future<> runScenario01() {
    K2LOG_I(log::k23si, "Scenario 01: commit a transaction with multiple async writes");

    return seastar::make_ready_future()
        .then([this] {
            return getTimeNow();
        })
        .then([this] (dto::Timestamp&& ts) {
            return seastar::do_with(
                dto::K23SI_MTR{
                    .timestamp = std::move(ts),
                    .priority = dto::TxnPriority::Medium},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s01-pKey1", .rangeKey = "rKey1"},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s01-pKey1", .rangeKey = "rKey1"},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s01-pKey2", .rangeKey = "rKey2"},
                DataRec{.f1="field1", .f2="field2"},
                [this] (dto::K23SI_MTR& mtr, dto::Key& key1, dto::Key& trh, dto::Key& key2, DataRec& rec) {
                    return doWrite(key1, rec, mtr, trh, _collName, false, true, true)
                        .then([this](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                        })
                        // Verify there is one WI on node
                        .then([this, &key1] {
                            return doRequestRecords(key1)
                                .then([this] (auto&& response) {
                                    auto& [status, k2response] = response;
                                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                                    K2EXPECT(log::k23si, k2response.records.size(), 1);
                                    return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            return doWrite(key2, rec, mtr, trh, _collName, false, false, true)
                                .then([this](auto&& response) {
                                    auto& [status, resp] = response;
                                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                                });
                        })
                        .then([&] {
                            return doRequestRecords(key2)
                                .then([this] (auto&& response) {
                                    auto& [status, k2response] = response;
                                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                                    K2EXPECT(log::k23si, k2response.records.size(), 1);
                                    return seastar::make_ready_future<>();
                            });
                        })
                        // Verify the Txn is InProgress
                        .then([this, &trh, &mtr] {
                            return doRequestTRH(trh, mtr)
                                .then([this] (auto&& response) {
                                    auto& [status, k2response] = response;
                                    K2EXPECT(log::k23si, k2response.writeInfos[_collName].size(), 2);
                                    for (auto&& [key, info]: k2response.writeInfos[_collName]) {
                                        K2LOG_I(log::k23si, "write key: {}, info: {}", key, info);
                                    }
                                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                                    K2EXPECT(log::k23si, k2response.state, k2::dto::TxnRecordState::InProgress);
                                    return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            K2LOG_I(log::k23si, "issuing the end request");
                            return doEnd(trh, mtr, _collName, true, {key1, key2});
                        })
                        .then([this, &key1, &mtr](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            return doRead(key1, mtr, _collName);
                        })
                        .then([&](auto&& response) {
                            auto& [status, value] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, value, rec);
                            return clearContext();
                        });
            });
    });
}

seastar::future<> runScenario02() {
    K2LOG_I(log::k23si, "Scenario 02: abort a transaction with multiple async writes");

    return seastar::make_ready_future()
        .then([this] {
            return getTimeNow();
        })
        .then([this] (dto::Timestamp&& ts) {
            return seastar::do_with(
                dto::K23SI_MTR{
                        .timestamp = std::move(ts),
                        .priority = dto::TxnPriority::Medium},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s02-pkey1", .rangeKey = "rKey1"},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s02-pkey1", .rangeKey = "rKey1"},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s02-pkey2", .rangeKey = "rKey2"},
                DataRec{.f1="field1", .f2="field2"},
                [this] (dto::K23SI_MTR& mtr, dto::Key& key1, dto::Key& trh, dto::Key& key2, DataRec& rec) {
                    return doWrite(key1, rec, mtr, trh, _collName, false, true, true)
                        .then([this](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                        })
                        // Verify there is one WI on node
                        .then([this, &key1] {
                            return doRequestRecords(key1)
                                .then([this] (auto&& response) {
                                    auto& [status, k2response] = response;
                                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                                    K2EXPECT(log::k23si, k2response.records.size(), 1);
                                    return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            return doWrite(key2, rec, mtr, trh, _collName, false, false, true)
                                .then([this](auto&& response) {
                                    auto& [status, resp] = response;
                                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                                });
                        })
                        .then([&] {
                            return doRequestRecords(key2)
                                .then([this] (auto&& response) {
                                    auto& [status, k2response] = response;
                                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                                    K2EXPECT(log::k23si, k2response.records.size(), 1);
                                    return seastar::make_ready_future<>();
                            });
                        })
                        // Verify the Txn is InProgress
                        .then([this, &trh, &mtr] {
                            return doRequestTRH(trh, mtr)
                                .then([this] (auto&& response) {
                                    auto& [status, k2response] = response;
                                    K2EXPECT(log::k23si, k2response.writeInfos[_collName].size(), 2);
                                    for (auto&& [key, info]: k2response.writeInfos[_collName]) {
                                        K2LOG_I(log::k23si, "write key: {}, info: {}", key, info);
                                    }                                
                                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                                    K2EXPECT(log::k23si, k2response.state, k2::dto::TxnRecordState::InProgress);
                                    return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            K2LOG_I(log::k23si, "issuing the end request");
                            return doEnd(trh, mtr, _collName, false, {key1, key2});
                        })
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            return seastar::sleep(100ms).then([this, &key1, &mtr] () {
                                return doRead(key1, mtr, _collName);
                            });
                        })
                        .then([this](auto&& response) {
                            auto& [status, value] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::KeyNotFound);
                            return clearContext();
                        });
                });
        });
}

seastar::future<> runScenario03() {
    K2LOG_I(log::k23si, "Scenario 03: force abort a transaction with async write");

    return seastar::make_ready_future()
        .then([this] () {
            return seastar::do_with(
                dto::K23SI_MTR{},
                dto::K23SI_MTR{},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s03-pkey1", .rangeKey = "rKey1"},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s03-pkey2", .rangeKey = "rKey2"},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s03-pkey1", .rangeKey = "rKey1"},
                DataRec{.f1="field1-mtr1", .f2="field2-mtr1"},
                DataRec{.f1="field1-mtr2", .f2="field2-mtr2"},
                [this] (dto::K23SI_MTR& mtr1, dto::K23SI_MTR& mtr2, dto::Key& key, dto::Key& key2, dto::Key& trh, DataRec& rec1, DataRec& rec2) {
                    return getTimeNow()
                        .then([&] (dto::Timestamp&& ts) {
                            mtr1.timestamp = ts;
                            mtr1.priority = dto::TxnPriority::Medium;
                            return doWrite(key, rec1, mtr1, trh, _collName, false, true, true);
                        })
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                            return doWrite(key2, rec1, mtr1, trh, _collName, false, true, true);
                        })
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                        })
                        // Verify there is one WI on node
                        .then([this, &key] {
                            return doRequestRecords(key)
                                .then([this] (auto&& response) {
                                    auto& [status, k2response] = response;
                                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                                    K2EXPECT(log::k23si, k2response.records.size(), 1);
                                    return seastar::make_ready_future<>();
                            });
                        })
                        // Verify the Txn is InProgress
                        .then([this, &trh, &mtr1] {
                            return doRequestTRH(trh, mtr1)
                                .then([this] (auto&& response) {
                                    auto& [status, k2response] = response;
                                    K2EXPECT(log::k23si, k2response.writeInfos[_collName].size(), 2);
                                    for (auto&& [key, info]: k2response.writeInfos[_collName]) {
                                        K2LOG_I(log::k23si, "write key: {}, info: {}", key, info);
                                    }
                                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                                    K2EXPECT(log::k23si, k2response.state, k2::dto::TxnRecordState::InProgress);
                                    return getTimeNow();
                            });
                        })
                        .then([&] (dto::Timestamp&& ts){
                            mtr2.timestamp = ts;
                            mtr2.priority = dto::TxnPriority::Highest;
                            return doWrite(key, rec2, mtr2, trh, _collName, false, true, true)
                                .then([this](auto&& response) {
                                    auto& [status, resp] = response;
                                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                                });
                        })
                        // Verify the Txn is ForceAborted
                        .then([this, &trh, &mtr1] {
                            return doRequestTRH(trh, mtr1)
                                .then([this] (auto&& response) {
                                    auto& [status, k2response] = response;
                                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                                    K2EXPECT(log::k23si, k2response.state, k2::dto::TxnRecordState::ForceAborted);
                                    return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            K2LOG_I(log::k23si, "issuing the end request for mtr {}", mtr1);
                            return doEnd(trh, mtr1, _collName, false, {key});
                        })
                        .then([&](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                        })
                        .then([&] {
                            K2LOG_I(log::k23si, "issuing the end request for mtr {}", mtr2);
                            return doEnd(trh, mtr2, _collName, true, {key});
                        })
                        .then([&] (auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            return seastar::sleep(100ms).then([this, &key, &mtr2] () {
                                return doRead(key, mtr2, _collName);
                            });
                        })
                        .then([&](auto&& response) {
                            auto& [status, value] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, value, rec2);
                            return clearContext();
                        });
                });
        });
}

seastar::future<> runScenario04() {
    K2LOG_I(log::k23si, "Scenario 04: concurrent transactions same keys in write async manner");

    return seastar::do_with(
        dto::K23SI_MTR{},
        dto::Key{_schemaName, "s04-pkey1", "rkey1"},
        dto::K23SI_MTR{},
        dto::Key{_schemaName, "s04-pkey1", "rkey1"},
        [this](auto& m1, auto& k1, auto& m2, auto& k2) {
            return getTimeNow()
                .then([&](dto::Timestamp&& ts) {
                    m1.timestamp = ts;
                    m1.priority = dto::TxnPriority::Medium;
                    return doWrite(k1, {"fk1", "f2"}, m1, k1, _collName, false, true, true);
                })
                .then([&](auto&& result) {
                    auto& [status, r] = result;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                    return getTimeNow();
                })
                .then([&](dto::Timestamp&& ts) {
                    m2.timestamp = ts;
                    m2.priority = dto::TxnPriority::Medium;
                    return doWrite(k2, {"fk2", "f2"}, m2, k2, _collName, false, true, true);
                })
                .then([&](auto&& result) {
                    auto& [status, r] = result;
                    K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                    return seastar::make_ready_future<>();
                })
                .then([&] () {
                    return doRequestRecords(k2);
                })
                .then([&] (auto&& response) {
                    // Verify there is a single WI for key
                    auto& [status, k2response] = response;
                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                    K2EXPECT(log::k23si, k2response.records.size(), 1);
                    return doRequestTRH(k2, m2);
                })
                .then([&] (auto&& response) {
                    // Verify newer txn is still InProgress
                    auto& [status, k2response] = response;
                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                    K2EXPECT(log::k23si, k2response.state, k2::dto::TxnRecordState::InProgress);
                    return seastar::when_all(doEnd(k1, m1, _collName, true, {k1}), doEnd(k2, m2, _collName, true, {k2}));
                })
                .then([&](auto&& result) mutable {
                    auto& [r1, r2] = result;
                    // apparently, have to move these out of the incoming futures since get0() returns an rvalue
                    auto [status1, result1] = r1.get0();
                    auto [status2, result2] = r2.get0();
                    // first txn gets aborted in this scenario since on push, the newer txn wins. The status should not be OK
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::OperationNotAllowed);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                    // do end for first txn with Abort
                    return doEnd(k1, m1, _collName, false, {k1});
                    // return seastar::when_all(doRead(k1, m1, _collName), doRead(k2, m2, _collName));
                })
                .then([&](auto&& result) {
                    auto& [status, resp] = result;
                    K2LOG_I(log::k23si, "{}", status);
                    return seastar::when_all(doRead(k1, m1, _collName), doRead(k2, m2, _collName));
                })
                .then([&](auto&& result) mutable {
                    auto& [r1, r2] = result;
                    auto [status1, value1] = r1.get0();
                    auto [status2, value2] = r2.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                    DataRec d2{"fk2", "f2"};
                    K2EXPECT(log::k23si, value2, d2);
                    return clearContext();
                });
        });
}

seastar::future<> runScenario05() {
    K2LOG_I(log::k23si, "Scenario 05: test write keys which fall into multiple partitions");

    return seastar::make_ready_future()
        .then([this] {
            return getTimeNow();
        })
        .then([&] (dto::Timestamp&& ts) {
            return seastar::do_with(
                dto::K23SI_MTR{
                    .timestamp = std::move(ts),
                    .priority = dto::TxnPriority::Medium},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s05-pkey1", .rangeKey = "rKey1"},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s05-pkey1", .rangeKey = "rKey1"},
                DataRec{.f1="field1", .f2="field2"},
                [this] (dto::K23SI_MTR& mtr, dto::Key& key1, dto::Key& trh, DataRec& rec) {
                    return doWrite(key1, rec, mtr, trh, _collName, false, true, true)
                        .then([this](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                        })
                        .then([&] {
                            dto::Key key{
                                .schemaName = _schemaName,
                                .partitionKey = "s05-pkey8",
                                .rangeKey = "rkey8"
                            };
                            return doWrite(key, rec, mtr, trh, _collName, false, false, true).then([&] (auto&& response) {
                                auto& [status, resp] = response;
                                K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                            });
                        })
                        // Verify the Txn is InProgress
                        .then([this, &trh, &mtr] {
                            return doRequestTRH(trh, mtr).
                                    then([this] (auto&& response) {
                                auto& [status, k2response] = response;
                                K2EXPECT(log::k23si, k2response.writeInfos[_collName].size(), 2);
                                for (auto&& [key, info]: k2response.writeInfos[_collName]) {
                                    K2LOG_I(log::k23si, "write key: {}, info: {}", key, info);
                                }
                                K2EXPECT(log::k23si, status, Statuses::S200_OK);
                                K2EXPECT(log::k23si, k2response.state, k2::dto::TxnRecordState::InProgress);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            K2LOG_I(log::k23si, "issuing the end request");
                            std::vector<dto::Key> endKeys;
                            dto::Key key{
                                .schemaName = _schemaName,
                                .partitionKey = "s10-pkey8",
                                .rangeKey = "rkey8"
                            };
                            endKeys.push_back(key);
                            endKeys.push_back(key1);                            
                            return doEnd(trh, mtr, _collName, true, endKeys);
                        })
                        .then([this, &key1, &mtr](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            return doRead(key1, mtr, _collName);
                        })
                        .then([&](auto&& response) {
                            auto& [status, value] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, value, rec);
                            return clearContext();
                        });
            });
    });
}

seastar::future<> runScenario06() {
    K2LOG_I(log::k23si, "Scenario 06: test write ops within the same partition");

    // log_level WARN k2::skv_server=WARN
    // To verify the write async optimization within the same partition, count the time spent for 400 write operations

    return seastar::make_ready_future()
        .then([this] {
            return getTimeNow();
        })
        .then([&] (dto::Timestamp&& ts) {
            return seastar::do_with(
                dto::K23SI_MTR{
                    .timestamp = std::move(ts),
                    .priority = dto::TxnPriority::Medium},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s06-pkey1", .rangeKey = "rKey1"},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s06-pkey1", .rangeKey = "rKey1"},
                DataRec{.f1="field1", .f2="field2"},
                k2::Clock::now(),
                [this] (dto::K23SI_MTR& mtr, dto::Key& key1, dto::Key& trh, DataRec& rec, k2::TimePoint& startTp) {
                    // change writeAsync to see different mamner
                    return doWrite(key1, rec, mtr, trh, _collName, false, true, true)
                        .then([this](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                        })
                        .then([&] {
                            seastar::future<> fut = seastar::make_ready_future<>();
                            for (int i = 2; i <= 400; i++) {
                                dto::Key key{
                                    .schemaName = _schemaName,
                                    .partitionKey = "s06-pkey1",
                                    .rangeKey = "rkey" + std::to_string(i)
                                };
                                fut = fut.then([this, key=std::move(key), &rec, &mtr, &trh] {
                                    // K2LOG_I(log::k23si, "write key: {}", key);
                                    return doWrite(key, rec, mtr, trh, _collName, false, false, true).then([&] (auto&& response) {
                                        auto& [status, resp] = response;
                                        K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                                    });
                                });
                            }
                            return fut;
                        })
                        // Verify the Txn is InProgress
                        .then([this, &trh, &mtr] {
                            return doRequestTRH(trh, mtr).
                                    then([&] (auto&& response) {
                                auto& [status, k2response] = response;
                                // K2EXPECT(log::k23si, k2response.writeInfos[_collName].size(), 400);
                                // for (auto&& [key, info]: k2response.writeInfos[_collName]) {
                                //     K2LOG_D(log::k23si, "write key: {}, info: {}", key, info);
                                // }
                                K2EXPECT(log::k23si, status, Statuses::S200_OK);
                                K2EXPECT(log::k23si, k2response.state, k2::dto::TxnRecordState::InProgress);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            K2LOG_I(log::k23si, "issuing the end request");
                            std::vector<dto::Key> endKeys;
                            for (int i = 1; i <= 400; i++) {
                                dto::Key key{
                                    .schemaName = _schemaName,
                                    .partitionKey = "s06-pkey1",
                                    .rangeKey = "rkey" + std::to_string(i)
                                };
                                endKeys.push_back(key);
                            }                                
                            return doEnd(trh, mtr, _collName, true, endKeys);
                        })
                        .then([this, &key1, &mtr](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            return doRead(key1, mtr, _collName);
                        })
                        .then([&](auto&& response) {
                            auto& [status, value] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, value, rec);
                            auto duration = k2::Clock::now() - startTp;
                            auto totalsecs = ((double)k2::msec(duration).count()) / 1000.0;
                            K2LOG_I(log::k23si, "time spent: {} secs", totalsecs);
                            return clearContext();
                        });
            });
    });
}

seastar::future<> runScenario07() {
    K2LOG_I(log::k23si, "Scenario 07: test write ops in multiple partitions");

    // log_level WARN k2::skv_server=WARN
    // To verify the write async optimization in multiple partitions, count the time spent for 200 write operations

    return seastar::make_ready_future()
        .then([this] {
            return getTimeNow();
        })
        .then([&] (dto::Timestamp&& ts) {
            return seastar::do_with(
                dto::K23SI_MTR{
                    .timestamp = std::move(ts),
                    .priority = dto::TxnPriority::Medium},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s07-pkey1", .rangeKey = "rKey1"},
                dto::Key{.schemaName = _schemaName, .partitionKey = "s07-pkey1", .rangeKey = "rKey1"},
                DataRec{.f1="field1", .f2="field2"},
                k2::Clock::now(),
                [this] (dto::K23SI_MTR& mtr, dto::Key& key1, dto::Key& trh, DataRec& rec, k2::TimePoint& startTp) {
                    return doWrite(key1, rec, mtr, trh, _collName, false, true, true)
                        .then([this](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                        })
                        .then([&] {
                            seastar::future<> fut = seastar::make_ready_future<>();
                            for (int i = 2; i <= 200; i++) {
                                dto::Key key{
                                    .schemaName = _schemaName,
                                    .partitionKey = "s07-pkey" + std::to_string(i), 
                                    .rangeKey = "rkey" + std::to_string(i)
                                };
                                fut = fut.then([this, key=std::move(key), &rec, &mtr, &trh] {
                                    // K2LOG_I(log::k23si, "write key: {}", key);
                                    return doWrite(key, rec, mtr, trh, _collName, false, false, true).then([&] (auto&& response) {
                                        auto& [status, resp] = response;
                                        K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                                    });
                                });
                            }
                            return fut;
                        })
                        // Verify the Txn is InProgress
                        .then([this, &trh, &mtr] {
                            return doRequestTRH(trh, mtr).
                                    then([this] (auto&& response) {
                                auto& [status, k2response] = response;
                                // K2EXPECT(log::k23si, k2response.writeInfos[_collName].size(), 10);
                                // for (auto&& [key, info]: k2response.writeInfos[_collName]) {
                                //     K2LOG_D(log::k23si, "write key: {}, info: {}", key, info);
                                // }
                                K2EXPECT(log::k23si, status, Statuses::S200_OK);
                                K2EXPECT(log::k23si, k2response.state, k2::dto::TxnRecordState::InProgress);
                                return seastar::make_ready_future<>();
                            });
                        })
                        .then([&] {
                            K2LOG_I(log::k23si, "issuing the end request");
                            std::vector<dto::Key> endKeys;
                            for (int i = 1; i <= 200; i++) {
                                dto::Key key{
                                    .schemaName = _schemaName,
                                    .partitionKey = "s07-pkey" + std::to_string(i),
                                    .rangeKey = "rkey" + std::to_string(i)
                                };
                                endKeys.push_back(key);
                            }                                
                            return doEnd(trh, mtr, _collName, true, endKeys);
                        })
                        .then([this, &key1, &mtr](auto&& response) {
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            return doRead(key1, mtr, _collName);
                        })
                        .then([&](auto&& response) {
                            auto& [status, value] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                            K2EXPECT(log::k23si, value, rec);
                            auto duration = k2::Clock::now() - startTp;
                            auto totalsecs = ((double)k2::msec(duration).count()) / 1000.0;
                            K2LOG_I(log::k23si, "time spent: {} secs", totalsecs);
                            return clearContext();
                        });
            });
    });
}

seastar::future<> runScenario08() {
    K2LOG_I(log::k23si, "Scenario 08: concurrent transactions same keys in write async manner(using when_all)");

    // log_level INFO k2::skv_server=DEBUG
    // In this scenario, inProgressPIP -> onAbort may happen in twim, 
    // which eventually leads to a challenger timeout, for issue #187

    return seastar::do_with(
        dto::K23SI_MTR{},
        dto::Key{_schemaName, "s08-pkey1", "rkey1"},
        dto::K23SI_MTR{},
        dto::Key{_schemaName, "s08-pkey1", "rkey1"},
        [this](auto& m1, auto& k1, auto& m2, auto& k2) {
            return getTimeNow()
                .then([&](dto::Timestamp&& ts) {
                    m1.timestamp = ts;
                    m1.priority = dto::TxnPriority::Medium;
                    return getTimeNow();
                })
                .then([&](dto::Timestamp&& ts) {
                    m2.timestamp = ts;
                    m2.priority = dto::TxnPriority::Medium;
                    return when_all(
                        doWrite(k2, {"fk2", "f2"}, m2, k2, _collName, false, true, true),
                        doWrite(k1, {"fk1", "f2"}, m1, k1, _collName, false, true, true)).discard_result();
                })
                .then([&] () {
                    return doRequestRecords(k2);
                })
                .then([&] (auto&& response) {
                    // Verify there is a single WI for key
                    auto& [status, k2response] = response;
                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                    K2EXPECT(log::k23si, k2response.records.size(), 1);

                    return doRequestTRH(k2, m2);
                })
                .then([&] (auto&& response) {
                    // Verify newer txn is still InProgress
                    auto& [status, k2response] = response;
                    K2EXPECT(log::k23si, status, Statuses::S200_OK);
                    K2EXPECT(log::k23si, k2response.state, k2::dto::TxnRecordState::InProgress);
                    K2LOG_I(log::k23si, "issuing the end request using when_all");
                    return seastar::when_all(doEnd(k1, m1, _collName, false, {k1}), doEnd(k2, m2, _collName, true, {k2}));
                })
                .then([&](auto&& result) mutable {
                    auto& [r1, r2] = result;
                    // apparently, have to move these out of the incoming futures since get0() returns an rvalue
                    auto [status1, result1] = r1.get0();
                    auto [status2, result2] = r2.get0();
                    // first txn gets aborted in this scenario since on push, the newer txn wins. The status should not be OK
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::OK);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                })
                .then([&] {
                    return seastar::when_all(doRead(k1, m1, _collName), doRead(k2, m2, _collName));
                })
                .then([&](auto&& result) mutable {
                    auto& [r1, r2] = result;
                    auto [status1, value1] = r1.get0();
                    auto [status2, value2] = r2.get0();
                    K2EXPECT(log::k23si, status1, dto::K23SIStatus::KeyNotFound);
                    K2EXPECT(log::k23si, status2, dto::K23SIStatus::OK);
                    DataRec d2{"fk2", "f2"};
                    K2EXPECT(log::k23si, value2, d2);
                    return clearContext();
                });
        });
}

seastar::future<> runTransactions(String prefix = "run_trans", bool writeAsync = false, int keysCount = 100, bool singlePartition = false, bool countLatency = false) {
    return seastar::make_ready_future()
        .then([this] {
            return getTimeNow();
        })
        .then([&] (dto::Timestamp&& ts) {
            k2::OperationLatencyReporter reporter(_txnLatency);
            return seastar::do_with(
                dto::K23SI_MTR{
                    .timestamp = std::move(ts),
                    .priority = dto::TxnPriority::Medium},
                dto::Key{.schemaName = _schemaName, .partitionKey = prefix + "pkey1", .rangeKey = "rKey1"},
                dto::Key{.schemaName = _schemaName, .partitionKey = prefix + "pkey1", .rangeKey = "rKey1"},
                DataRec{.f1="field1", .f2="field2"},
                std::move(reporter),
                prefix,
                writeAsync,
                keysCount,
                singlePartition,
                countLatency,
                [this] (dto::K23SI_MTR& mtr, dto::Key& key1, dto::Key& trh, DataRec& rec, auto& txnReporter, String& prefix, auto& writeAsync, auto& keysCount, auto& singlePartition, auto& countLatency) {
                    // K2LOG_I(log::k23si, "prefix={}, writeAsync={}, keysCount={}, singlePartition={}", prefix, writeAsync, keysCount, singlePartition);
                    k2::OperationLatencyReporter reporter(_writeLatency);
                    return doWrite(key1, rec, mtr, trh, _collName, false, true, writeAsync)
                        .then([this, reporter=std::move(reporter)] (auto&& response) mutable{
                            auto& [status, resp] = response;
                            K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                            reporter.report();
                        })
                        .then([&] {
                            seastar::future<> fut = seastar::make_ready_future<>();
                            for (int i = 2; i <= keysCount; i++) {
                                dto::Key key{
                                    .schemaName = _schemaName,
                                    .partitionKey = prefix + (singlePartition ? "pkey1" : "pkey" + std::to_string(i)),
                                    .rangeKey = "rkey" + std::to_string(i)
                                };
                                fut = fut.then([this, key=std::move(key), &rec, &mtr, &trh, &writeAsync, &countLatency] {
                                    // K2LOG_I(log::k23si, "write key: {}", key);
                                    k2::OperationLatencyReporter reporter(_writeLatency);
                                    return doWrite(key, rec, mtr, trh, _collName, false, false, writeAsync)
                                        .then([this, reporter=std::move(reporter)] (auto&& response) mutable{
                                            auto& [status, resp] = response;
                                            K2EXPECT(log::k23si, status, dto::K23SIStatus::Created);
                                            reporter.report();            
                                        });
                                });
                            }
                            return fut;
                        })
                        .then([&] {
                            // K2LOG_I(log::k23si, "issuing the end request");
                            std::vector<dto::Key> endKeys;
                            for (int i = 1; i <= keysCount; i++) {
                                dto::Key key{
                                    .schemaName = _schemaName,
                                    .partitionKey = prefix + (singlePartition ? "pkey1" : "pkey" + std::to_string(i)),
                                    .rangeKey = "rkey" + std::to_string(i)
                                };
                                endKeys.push_back(key);
                            }   
                            k2::OperationLatencyReporter reporter(_endLatency);                                  
                            return doEnd(trh, mtr, _collName, true, endKeys)
                                .then([this, reporter=std::move(reporter)] (auto&& response) mutable{
                                    auto& [status, resp] = response;
                                    K2EXPECT(log::k23si, status, dto::K23SIStatus::OK);
                                    reporter.report();            
                                });
                        })
                        .then([&] () {
                            txnReporter.report();
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
    int transactionsCount = _txnsCount();
    int keysCount = _keysCount();
    bool singlePartition = _singlePartition();
    bool writeAsync = _writeAsync();
    bool countLatency = _countLatency();
    return seastar::do_with(
        std::move(concurrentNum),
        std::move(transactionsCount),
        std::move(keysCount),
        std::move(singlePartition),
        std::move(writeAsync),
        std::move(countLatency),
        std::move(startTp),
        [this] (auto& concurrentNum, auto& transactionsCount, auto& keysCount, auto& singlePartition, auto& writeAsync, auto& countLatency, auto& startTp) {
            std::vector<seastar::future<>> futs;
            int shard_id = seastar::this_shard_id();
            // int cpu_count = seastar::smp::count;
            for (int num = 0; num < concurrentNum; num += 1) {
                seastar::future<> fut = seastar::make_ready_future<>();
                for (int i = 0; i < transactionsCount; i++) {
                    String prefix {"test_ops_" + std::to_string(shard_id) + "_" + std::to_string(num) + "_" + std::to_string(i)};
                    fut = fut.then([&, prefix=std::move(prefix)] () {
                        return runTransactions(prefix, writeAsync, keysCount, singlePartition, countLatency);
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
    app.addOptions()("txns_count", bpo::value<uint32_t>()->default_value(100), "how many txns to test");
    app.addOptions()("keys_count", bpo::value<uint32_t>()->default_value(50), "how many writes to test per txn");
    app.addOptions()("single_partition", bpo::value<bool>()->default_value(true), "whether to write in one partition only");
    app.addOptions()("write_async", bpo::value<bool>()->default_value(true), "whether to write in async manner");
    app.addOptions()("count_latency", bpo::value<bool>()->default_value(false), "whether to count latency of write and end opration");
    app.addApplet<k2::WriteAsyncTest>();
    return app.start(argc, argv);
}
