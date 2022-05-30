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

#include <chrono>
#include "Module.h"

#include <k2/appbase/AppEssentials.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/infrastructure/APIServer.h>

namespace k2 {
namespace dto {
// we want the read cache to determine ordering based on certain comparison so that we have some stable
// ordering even across different nodes and TSOs
const Timestamp& max(const Timestamp& a, const Timestamp& b) {
    return a.compareCertain(b) == Timestamp::LT ? b : a;
}
} // ns dto

// ********************** Validators
bool K23SIPartitionModule::_validateRetentionWindow(const dto::Timestamp& ts) const {
    bool result = ts.compareCertain(_retentionTimestamp) >= 0;
    K2LOG_D(log::skvsvr, "retention validation {}, {} vs {}",
            (result ? "passed" : "failed"), _retentionTimestamp, ts);
    return result;
}


template <typename T, typename = void>
struct has_key_field : std::false_type {};
template <typename T>
struct has_key_field<T, std::void_t<decltype(T::key)>>: std::true_type {};

template<typename RequestT>
bool K23SIPartitionModule::_validateRequestPartition(const RequestT& req) const {
    auto result = req.collectionName == _cmeta.name && req.pvid == _partition().keyRangeV.pvid;
    // validate partition owns the requests' key.
    // 1. common case assumes RequestT a Read request;
    // 2. now for the other cases, only Query request is implemented.
    if constexpr (std::is_same<RequestT, dto::K23SIQueryRequest>::value) {
        result = result && _partition.owns(req.key, req.reverseDirection);
    } else if constexpr(has_key_field<RequestT>::value) {
        result = result && _partition.owns(req.key);
    }
    else {
        result = result && _partition().keyRangeV.pvid == req.pvid;
    }
    K2LOG_D(log::skvsvr, "partition validation {}, for request={}", (result ? "passed" : "failed"), req);
    return result;
}

template <typename RequestT>
Status K23SIPartitionModule::_validateStaleWrite(const RequestT& request, const KeyValueNode& KVNode) {
    if (!_validateRetentionWindow(request.mtr.timestamp)) {
        // the request is outside the retention window
        return dto::K23SIStatus::AbortRequestTooOld("write request is outside retention window");
    }
    // check read cache for R->W conflicts
    auto ts = _readCache->checkInterval(request.key, request.key);
    if (request.mtr.timestamp.compareCertain(ts) < 0) {
        // this key range was read more recently than this write
        K2LOG_D(log::skvsvr, "read cache validation failed for key: {}, transaction timestamp: {}, < readCache key timestamp: {}, readcache min_TimeStamp: {}", request.key, request.mtr.timestamp, ts, _readCache->min_TimeStamp());
        bool belowReadCacheWaterMark = (request.mtr.timestamp.compareCertain(_readCache->min_TimeStamp()) <= 0);
        if (belowReadCacheWaterMark) {
            return dto::K23SIStatus::AbortRequestTooOld("write request cannot be allowed as this transaction is too old (cache watermark).");
        } else {
            return dto::K23SIStatus::AbortRequestTooOld("write request cannot be allowed as this key (or key range) has been observed by another transaction.");
        }
    }

    // check if we have a committed value newer than the request.
    // NB(1) if we try to place a WI over a committed value from different transaction with same ts.end,
    // reject the incoming write in order to avoid weird read-my-write problem for in-progress transactions
    // NB(2) we cannot allow writes past a committed value since a write has to imply a read causality, so
    // if a txn committed a value at time T5, then we must also assume they did a read at time T5
    // NB(3) This code does not care if there is a WI. If there is a WI, then this check can help avoid
    // an unnecessary PUSH.
    dto::DataRecord* latestRec = const_cast<k2::KeyValueNode&>(KVNode).begin();
    //NodeVerMetadata verMD = KVNode.getNodeVerMetaData(0, pbrb);
    if (const_cast<k2::KeyValueNode&>(KVNode).is_inmem(0)){
        latestRec = static_cast<dto::DataRecord *>(pbrb->getPlogAddrRow(latestRec));
    }

    //while(latestRec!=nullptr && latestRec->status == dto::DataRecord::WriteIntent) latestRec=latestRec->prevVersion;
    if (latestRec != nullptr && latestRec->status == dto::DataRecord::Committed &&
        request.mtr.timestamp.compareCertain(latestRec->timestamp) <= 0) {
        // newest version is the latest committed and its newer than the request
        // or committed version from same transaction is found (e.g. bad retry on a write came through after commit)
        K2LOG_D(log::skvsvr, "failing write older than latest commit for key {}", request.key);
        return dto::K23SIStatus::AbortRequestTooOld("write request cannot be allowed as we have a newer committed write for this key from another transaction.");
    }
    // Note that we could also check the request id against the WI request id if it exists, and enforce
    // that it is non-decreasing. This would only catch a problem where: there is a bug in the client or
    // application code and the client does parallel writes to the same key. If the client wants to order
    // writes to the same key they must be done in serial.

    K2LOG_D(log::skvsvr, "stale write check passed for key {}", request.key);
    return dto::K23SIStatus::OK;
}

template <typename RequestT>
bool K23SIPartitionModule::_validateRequestPartitionKey(const RequestT& req) const {
    K2LOG_D(log::skvsvr, "Request: {}", req);

    if constexpr (std::is_same<RequestT, dto::K23SIQueryRequest>::value) {
        // Query is allowed to have empty partition key which means start or end of schema set
        return true;
    }
    else {
        return !req.key.partitionKey.empty();
    }
}

template <class RequestT>
Status K23SIPartitionModule::_validateReadRequest(const RequestT& request) const {
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return dto::K23SIStatus::RefreshCollection("collection refresh needed in read-type request");
    }
    if (!_validateRequestPartitionKey(request)) {
        // do not allow empty partition key
        return dto::K23SIStatus::BadParameter("missing partition key in read-type request");
    }
    if (!_validateRetentionWindow(request.mtr.timestamp)) {
        // the request is outside the retention window
        return dto::K23SIStatus::AbortRequestTooOld("request too old in read-type request");
    }
    if (_schemas.find(request.key.schemaName) == _schemas.end()) {
        // server does not have schema
        return dto::K23SIStatus::OperationNotAllowed("schema does not exist in read-type request");
    }
    K2LOG_D(log::skvsvr, "Validate Read Request SUCCESS");
    return dto::K23SIStatus::OK;
}

Status K23SIPartitionModule::_validateWriteRequest(const dto::K23SIWriteRequest& request, const KeyValueNode& KVNode) {
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return dto::K23SIStatus::RefreshCollection("collection refresh needed in read-type request");
    }

    if (!_validateRequestPartitionKey(request)) {
        // do not allow empty partition key
        return dto::K23SIStatus::BadParameter("missing partition key in write");
    }

    auto schemaIt = _schemas.find(request.key.schemaName);
    if (schemaIt == _schemas.end()) {
        return dto::K23SIStatus::OperationNotAllowed("schema does not exist");
    }
    if (schemaIt->second.find(request.value.schemaVersion) == schemaIt->second.end()) {
        // server does not have schema
        return dto::K23SIStatus::OperationNotAllowed("schema version does not exist");
    }

    if (auto* twim = _twimMgr.getTxnWIMeta(request.mtr.timestamp); twim != nullptr) {
        if (twim->isAborted()) {
            return dto::K23SIStatus::AbortConflict("The transaction has been aborted");
        }
        else if (twim->isCommitted()) {
            return dto::K23SIStatus::BadParameter("The transaction has been committed");
        }
    }

    return _validateStaleWrite(request, KVNode);
}
// ********************** Validators

K23SIPartitionModule::K23SIPartitionModule(dto::CollectionMetadata cmeta, dto::Partition partition) :
    _cmeta(std::move(cmeta)),
    _partition(std::move(partition), _cmeta.hashScheme) {
    uint32_t pageNum = _config.totalNumberofPage();
    K2LOG_I(log::skvsvr, "---------Partition: {}, pageNum:{}", _partition, pageNum);
    //pbrb = new PBRB(pageNum, &_retentionTimestamp, &indexer);
    pbrb = new PBRB(pageNum, &_retentionTimestamp);
    enablePBRB = _config.enablePBRB();
    for(int i=0; i < 12; i++){
        hitRatio[i] = 1.0;
    }
    // config the pmem engine 
    std::string pmemPath = fmt::format("{}/shard{}",_config.pmemEnginePath(),seastar::this_shard_id());
    strcpy(_engineConfig.engine_path,pmemPath.c_str());
    _engineConfig.engine_capacity = _config.pmemEngineCapacity();
    _engineConfig.chunk_size = _config.pmemEngineChunkSize();

    // open the engine
    auto status = PmemEngine::open(_engineConfig,&_enginePtr);
    if ( !status.is2xxOK()){
        K2LOG_E(log::skvsvr,"------Partition: {} fail to create pmem engine: {}" ,
         _partition,status.message);
    }

    //init the queuePool
    for (int i = 0; i < queuePoolSize; i++) {
        requestEntry *rEntry = new struct requestEntry;
        queuePool.push(rEntry);
    }

#ifdef OUTPUT_ACCESS_PATTERN  
    ofile.open("RWresults.txt");
#endif
    K2LOG_I(log::skvsvr, "ctor for cname={}, part={}", _cmeta.name, _partition);
}

seastar::future<> K23SIPartitionModule::_registerVerbs() {
    K2LOG_D(log::skvsvr, "Starting for partition: {}", _partition);

    APIServer& api_server = AppBase().getDist<APIServer>().local();

    RPC().registerRPCObserver<dto::K23SIReadRequest, dto::K23SIReadResponse>
    (dto::Verbs::K23SI_READ, [this](dto::K23SIReadRequest&& request) {
        k2::OperationLatencyReporter reporter(_readLatency); // for reporting metrics
        return handleRead(std::move(request), FastDeadline(_config.readTimeout()))
               .then([this, reporter=std::move(reporter)](auto&& response) mutable {
                    reporter.report();
                    return std::move(response);
               });
    });

    RPC().registerRPCObserver<dto::K23SIQueryRequest, dto::K23SIQueryResponse>
    (dto::Verbs::K23SI_QUERY, [this](dto::K23SIQueryRequest&& request) {
        k2::OperationLatencyReporter reporter(_queryPageLatency); // for reporting metrics
        return handleQuery(std::move(request), dto::K23SIQueryResponse{}, FastDeadline(_config.readTimeout()))
                .then([this, reporter=std::move(reporter)] (auto&& response) mutable {
                    reporter.report();
                    return std::move(response);
               });
    });

    RPC().registerRPCObserver<dto::K23SIWriteRequest, dto::K23SIWriteResponse>
    (dto::Verbs::K23SI_WRITE, [this](dto::K23SIWriteRequest&& request) {
        k2::OperationLatencyReporter reporter(_writeLatency); // for reporting metrics
        return handleWrite(std::move(request), FastDeadline(_config.writeTimeout()))
            .then([this, reporter=std::move(reporter)] (auto&& resp) mutable {
                return _respondAfterFlush(std::move(resp))
                        .then([this, reporter=std::move(reporter)] (auto&& response) mutable {
                            reporter.report();
                            return std::move(response);
                        });
            });
    });

    RPC().registerRPCObserver<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse>
    (dto::Verbs::K23SI_TXN_PUSH, [this](dto::K23SITxnPushRequest&& request) {
        k2::OperationLatencyReporter reporter(_pushLatency); // for reporting metrics
        return handleTxnPush(std::move(request))
                .then([this, reporter=std::move(reporter)] (auto&& response) mutable {
                    reporter.report();
                    return std::move(response);
               });
    });

    RPC().registerRPCObserver<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>
    (dto::Verbs::K23SI_TXN_END, [this](dto::K23SITxnEndRequest&& request) {
        K2LOG_D(log::skvsvr, "Enter End Observer");
        return handleTxnEnd(std::move(request))
            .then([this] (auto&& resp) { return _respondAfterFlush(std::move(resp));});
    });

    RPC().registerRPCObserver<dto::K23SITxnHeartbeatRequest, dto::K23SITxnHeartbeatResponse>
    (dto::Verbs::K23SI_TXN_HEARTBEAT, [this](dto::K23SITxnHeartbeatRequest&& request) {
        return handleTxnHeartbeat(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SITxnFinalizeRequest, dto::K23SITxnFinalizeResponse>
    (dto::Verbs::K23SI_TXN_FINALIZE, [this](dto::K23SITxnFinalizeRequest&& request) {
        return handleTxnFinalize(std::move(request))
                .then([this] (auto&& resp) {
                    return _respondAfterFlush(std::move(resp));
                });
    });

    RPC().registerRPCObserver<dto::K23SIPushSchemaRequest, dto::K23SIPushSchemaResponse>
    (dto::Verbs::K23SI_PUSH_SCHEMA, [this](dto::K23SIPushSchemaRequest&& request) {
        return handlePushSchema(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SIInspectRecordsRequest, dto::K23SIInspectRecordsResponse>
    (dto::Verbs::K23SI_INSPECT_RECORDS, [this](dto::K23SIInspectRecordsRequest&& request) {
        return handleInspectRecords(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SIInspectTxnRequest, dto::K23SIInspectTxnResponse>
    (dto::Verbs::K23SI_INSPECT_TXN, [this](dto::K23SIInspectTxnRequest&& request) {
        return handleInspectTxn(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SIInspectWIsRequest, dto::K23SIInspectWIsResponse>
    (dto::Verbs::K23SI_INSPECT_WIS, [this](dto::K23SIInspectWIsRequest&& request) {
        return handleInspectWIs(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SIInspectAllTxnsRequest, dto::K23SIInspectAllTxnsResponse>
    (dto::Verbs::K23SI_INSPECT_ALL_TXNS, [this](dto::K23SIInspectAllTxnsRequest&& request) {
        return handleInspectAllTxns(std::move(request));
    });

    RPC().registerRPCObserver<dto::K23SIInspectAllKeysRequest, dto::K23SIInspectAllKeysResponse>
    (dto::Verbs::K23SI_INSPECT_ALL_KEYS, [this](dto::K23SIInspectAllKeysRequest&& request) {
        return handleInspectAllKeys(std::move(request));
    });
    api_server.registerAPIObserver<dto::K23SIInspectAllKeysRequest, dto::K23SIInspectAllKeysResponse>
    ("InspectAllKeys", "Returns ALL keys on the partition", [this](dto::K23SIInspectAllKeysRequest&& request) {
        return handleInspectAllKeys(std::move(request));
    });

    return seastar::make_ready_future();
}

void K23SIPartitionModule::_unregisterVerbs() {
    APIServer& api_server = AppBase().getDist<APIServer>().local();

    RPC().registerMessageObserver(dto::Verbs::K23SI_READ, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_QUERY, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_WRITE, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_TXN_PUSH, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_TXN_END, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_TXN_HEARTBEAT, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_TXN_FINALIZE, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_PUSH_SCHEMA, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_INSPECT_RECORDS, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_INSPECT_TXN, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_INSPECT_WIS, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_INSPECT_ALL_TXNS, nullptr);
    RPC().registerMessageObserver(dto::Verbs::K23SI_INSPECT_ALL_KEYS, nullptr);

    api_server.deregisterAPIObserver("InspectAllKeys");
}

void K23SIPartitionModule::_registerMetrics() {
    _metric_groups.clear();
    std::vector<sm::label_instance> labels;
    labels.push_back(sm::label_instance("total_cores", seastar::smp::count));

    _metric_groups.add_group("Nodepool", {
        sm::make_gauge("indexer_keys",[this]{ return _indexer.size();},
                        sm::description("Number of keys in indexer"), labels),
        sm::make_counter("total_WI", _totalWI, sm::description("Number of WIs created"), labels),
        sm::make_counter("finalized_WI", _finalizedWI, sm::description("Number of WIs finalized"), labels),
        sm::make_gauge("record_versions", _recordVersions, sm::description("Number of record versions over all records"), labels),
        sm::make_counter("total_committed_payload", _totalCommittedPayload, sm::description("Total size of committed payloads"), labels),
        sm::make_histogram("read_latency", [this]{ return _readLatency.getHistogram();},
                sm::description("Latency of Read Operations"), labels),
        sm::make_histogram("read_PmemLog_latency", [this]{ return _readPmemLogLatency.getHistogram();},
                sm::description("Latency of Read PmemLog Operations"), labels),
        sm::make_histogram("read_Pmem_latency", [this]{ return _enginePtr->getPmemReadLantency();},
                sm::description("Latency of Read Persisent Memory Operations"), labels),
        sm::make_histogram("write_latency", [this]{ return _writeLatency.getHistogram();},
                sm::description("Latency of Write Operations"), labels),
        sm::make_histogram("write_PmemLog_latency", [this]{ return _writePmemLogLatency.getHistogram();},
                sm::description("Latency of Write PmemLog Operations"), labels),        
        sm::make_histogram("write_Pmem_latency", [this]{ return _enginePtr->getPmemAppendLantency();},
                sm::description("Latency of Write Persisent Memory Operations"), labels),        
        sm::make_histogram("query_page_latency", [this]{ return _queryPageLatency.getHistogram();},
                sm::description("Latency of Query Page Operations"), labels),
        sm::make_histogram("push_latency", [this]{ return _pushLatency.getHistogram();},
                sm::description("Latency of Pushes"), labels),
        sm::make_histogram("query_page_scans", [this]{ return _queryPageScans.getHistogram();},
                sm::description("Number of records scanned by query page operations"), labels),
        sm::make_histogram("query_page_returns", [this]{ return _queryPageReturns.getHistogram();},
                sm::description("Number of records returned by query page operations"), labels)
    });
}

seastar::future<> K23SIPartitionModule::start() {

    _registerMetrics();

    _cpo.init(_config.cpoEndpoint());
    if (_cmeta.retentionPeriod < _config.minimumRetentionPeriod()) {
        K2LOG_W(log::skvsvr,
            "Requested retention({}) is lower than minimum({}). Extending retention to minimum",
            _cmeta.retentionPeriod, _config.minimumRetentionPeriod());
        _cmeta.retentionPeriod = _config.minimumRetentionPeriod();
    }

    // todo call TSO to get a timestamp
    return getTimeNow()
        .then([this](dto::Timestamp&& watermark) {
            K2LOG_D(log::skvsvr, "Cache watermark: {}, period={}", watermark, _cmeta.retentionPeriod);
            _retentionTimestamp = watermark - _cmeta.retentionPeriod;

            _startTs = watermark;
            _readCache = std::make_unique<ReadCache<dto::Key, dto::Timestamp>>(watermark, _config.readCacheSize());

            _retentionUpdateTimer.setCallback([this] {
                K2LOG_D(log::skvsvr, "Partition {}, refreshing retention timestamp", _partition);
                return getTimeNow()
                    .then([this](dto::Timestamp&& ts) {
                        // set the retention timestamp (the time of the oldest entry we should keep)
                        _retentionTimestamp = ts - _cmeta.retentionPeriod;
                        _txnMgr.updateRetentionTimestamp(_retentionTimestamp);
                        _twimMgr.updateRetentionTimestamp(_retentionTimestamp);
                        
                        if(isDonePBRBGC && isDoneCache){
                            doBackgroundPBRBGC(pbrb, _indexer, _retentionTimestamp,  _cmeta.retentionPeriod);
                        }
                    });
            });
            _retentionUpdateTimer.armPeriodic(_config.retentionTimestampUpdateInterval());

#ifdef ASYNC
            _dealRequestQueueTimer.setCallback([this] {
                if(isDoneCache && isDonePBRBGC){
                    isDoneCache = false;
                    //uniqueLock.lock();
                    int currentIndex = (queueIndex+1)%2;
                    if(queueIndex==1){
                        //K2LOG_I(log::skvsvr, "--requestQueue size:{},currentIndex:{}", requestQueue1.size(), currentIndex);                       
                        while(!requestQueue1.empty() && isDonePBRBGC){
                        //while(!requestQueue1.empty()){
                            struct requestEntry *curEntry = requestQueue1.front();                    
                            cacheKVRecordtoPBRB(curEntry->SMapIndex, curEntry->rec, curEntry->nodePtr, curEntry->indexFlag);
                            //delete curEntry->rec;
                            //curEntry->rec = nullptr;                               
                            queuePool.push(curEntry);                         
                            requestQueue1.pop();                                                                  
                        }
                    } else {
                        //K2LOG_I(log::skvsvr, "--requestQueue size:{},currentIndex:{}", requestQueue.size(), currentIndex);                            
                        while(!requestQueue.empty() && isDonePBRBGC){
                        //while(!requestQueue.empty()){
                            struct requestEntry *curEntry = requestQueue.front();                        
                            cacheKVRecordtoPBRB(curEntry->SMapIndex, curEntry->rec, curEntry->nodePtr, curEntry->indexFlag);
                            //delete curEntry->rec;
                            //curEntry->rec = nullptr;                                 
                            queuePool.push(curEntry);   
                            requestQueue.pop();
                        }
                    }
                    queueIndex = currentIndex;
                    //uniqueLock.unlock();                         
                    isDoneCache = true;
                }                   
                //});
                return getTimeNow().then([this](dto::Timestamp&& ts) {
                    K2LOG_D(log::skvsvr, "--ts:{}", ts);
                });
            });
            _dealRequestQueueTimer.armPeriodic(_config.dealRequestQueueInterval());
#endif

            _persistence = std::make_shared<Persistence>();
            return _persistence->start()
                .then([this] {
                    return _twimMgr.start(_retentionTimestamp, _persistence);
                })
                .then([this] {
                    return _txnMgr.start(_cmeta.name, _retentionTimestamp, _cmeta.heartbeatDeadline, _persistence);
                })
                .then([this] {
                    return _recovery();
                })
                .then([this] {
                    return _registerVerbs();
                });
        });
}

K23SIPartitionModule::~K23SIPartitionModule() {
    K2LOG_I(log::skvsvr, "dtor for cname={}, part={}", _cmeta.name, _partition);
}

seastar::future<> K23SIPartitionModule::gracefulStop() {
    K2LOG_I(log::skvsvr, "stop for cname={}, part={}", _cmeta.name, _partition);
    if(_enginePtr!=nullptr) delete _enginePtr;
    if(pbrb!=nullptr) delete pbrb;

    while(!queuePool.empty()){
        struct requestEntry *curEntry = queuePool.front();
        delete curEntry;   
        queuePool.pop();
    }

    _dealRequestQueueTimer.stop().then([this] {
        pbrb->fcrpOutput();
    });
    return _retentionUpdateTimer.stop()
        .then([this] {
            return _txnMgr.gracefulStop();
        })
        .then([this] {
            return _twimMgr.gracefulStop();
        })
        .then([this] {
            return _persistence->stop();
        })
        .then([this] {
            _unregisterVerbs();
            K2LOG_I(log::skvsvr, "stopped");

        #ifdef OUTPUT_READ_INFO
            for(int i=0; i<12; i++){
                K2LOG_I(log::skvsvr, "-----i:{}, totalUpdateCache:{} us, totalSerachTree:{} us, totalUpdateTree:{} us, totalIndex:{} us, totalGetRecordAddr:{} us, allocatePayloadns:{}, totalReadCopyFeild:{} us, totalGenRecord:{} us, totalFindPosition:{} us, totalCheckBitmap:{} us, totalIteratorPage:{} us, totalHeader:{} us, totalCopyFeild:{} us, totalUpdateKVNode:{} us, totalReadPBRB:{} us, totalReadNVM:{} us, totalRead:{} us, pbrbHitNum:{}, NvmReadNum:{}", i, totalUpdateCachens[i]/1000, totalSerachTreens[i]/1000, totalUpdateTreens[i]/1000,  totalIndexns[i]/1000, totalGetAddrns[i]/1000, allocatePayloadns[i]/1000, totalReadCopyFeildns[i]/1000, totalGenRecordns[i]/1000, totalFindPositionns[i]/1000, totalCheckBitmap[i]/1000, totalIteratorPage[i]/1000, totalHeaderns[i]/1000, totalCopyFeildns[i]/1000, totalUpdateKVNodens[i]/1000, totalReadPBRBns[i]/1000, totalReadNVMns[i]/1000, totalReadns[i]/1000, pbrbHitNum[i], NvmReadNum[i]);
            }
            //K2LOG_I(log::skvsvr, "pbrbHitNum:{}, NvmReadNum:{}", pbrbHitNum, NvmReadNum);
            K2LOG_I(log::skvsvr, "-----read count, item:{}, Warehouse: {}, Stock:{}, District:{}, Customer:{}, History:{}, OrderLine:{}, NewOrder:{}, Order:{}, Other:{}", 
            readCount[0], readCount[1], readCount[2], readCount[3], readCount[4], readCount[5], readCount[6], readCount[7], readCount[8], readCount[9]);
            if(readCount[0]>0 && readCount[1]>0 && readCount[2]>0 && readCount[3]>0 && readCount[4]>0){
                K2LOG_I(log::skvsvr, "-----average read size, item:{}, Warehouse: {}, Stock:{}, District:{}, Customer:{}", 
            (int)totalReadSize[0]/readCount[0], (int)totalReadSize[1]/readCount[1], (int)totalReadSize[2]/readCount[2], (int)totalReadSize[3]/readCount[3], (int)totalReadSize[4]/readCount[4]);
            }
            /*
            K2LOG_I(log::skvsvr, "-----write count, item:{}, Warehouse: {}, Stock:{}, District:{}, Customer:{}, History:{}, OrderLine:{}, NewOrder:{}, Order:{}, idx_customer_name:{}, idx_order_customer:{}, Other:{}", 
            writeCount[0], writeCount[1], writeCount[2], writeCount[3], writeCount[4], writeCount[5], writeCount[6], writeCount[7], writeCount[8], writeCount[9], writeCount[10], writeCount[11]);

            K2LOG_I(log::skvsvr, "-----total write size, item:{}, Warehouse: {}, Stock:{}, District:{}, Customer:{}, History:{}, OrderLine:{}, NewOrder:{}, Order:{}, idx_customer_name:{}, idx_order_customer:{}, Other:{}", 
            totalReadSize[0], totalReadSize[1], totalReadSize[2], totalReadSize[3], totalReadSize[4], totalReadSize[5], totalReadSize[6], totalReadSize[7], totalReadSize[8], totalReadSize[9], totalReadSize[10], totalReadSize[11]);
            */
        #endif

            //Read
            auto readnsecs = (double)k2::nsec(_readSum).count();
            auto readCachensecs = (double)k2::nsec(_readCacheSum).count();
            auto readIndexernsecs = (double)k2::nsec(_readIndexerSum).count();
            auto readItornsecs = (double)k2::nsec(_readItorSum).count();
            auto readNodensecs = (double)k2::nsec(_readNodeSum).count();
            K2LOG_W(log::skvsvr, "completedReadOp={}", _totalRead);
            _totalRead = _totalRead ? _totalRead : 1;
            K2LOG_W(log::skvsvr, "Average Read Time={} ns", (double)readnsecs/_totalRead);
            K2LOG_W(log::skvsvr, "Average Update Read Cache Time={} ns", (double)readCachensecs/_totalRead);
            K2LOG_W(log::skvsvr, "Average Read Indexer Time={} ns", (double)readIndexernsecs/_totalRead);
            K2LOG_W(log::skvsvr, "Average Extract from itor Time={} ns", (double)readItornsecs/_totalRead);
            K2LOG_W(log::skvsvr, "Average Read KeyValueNode Time={} ns", (double)readNodensecs/_totalRead);

            //Insert
            auto insertnsecs = (double)k2::nsec(_insertSum).count();
            auto insertIndexernsecs = (double)k2::nsec(_insertIndexerSum).count();
            auto validatensecs = (double)k2::nsec(_writeValidateSum).count();
            auto findnsecs = (double)k2::nsec(_writeFindSum).count();
            auto createWInsecs = (double)k2::nsec(_createWISum).count();
            K2LOG_W(log::skvsvr, "completedInsertOp={}", _totalInsert);
            K2LOG_W(log::skvsvr, "Average Insert Time={} ns",(double)insertnsecs/_totalInsert);
            K2LOG_W(log::skvsvr, "Average Lookup indexer Time={} ns",(double)findnsecs/_totalInsert);
            K2LOG_W(log::skvsvr, "Average Insert Indexer Time={} ns",(double)insertIndexernsecs/_totalInsert);
            K2LOG_W(log::skvsvr, "Average write validate Time={} ns",(double)validatensecs/_totalInsert);
            K2LOG_W(log::skvsvr, "Average create WI Time={} ns",(double)createWInsecs/_totalInsert);

            // auto updatensecs = (double)k2::nsec(_partialUpdateSum).count();
            // auto updateIndexernsecs = (double)k2::nsec(_partialUpdateIndexerSum).count();
            // auto perUpdateTime = (double)updatensecs/_totalPartialUpdate;
            // auto perUpdateIndexerTime = (double)updateIndexernsecs/_totalPartialUpdate;
            // K2LOG_I(log::skvsvr, "completed Partial Update Op={}", _totalPartialUpdate);
            // K2LOG_I(log::skvsvr, "Average PartialUpdate Time={} ns", perUpdateTime);
            // K2LOG_I(log::skvsvr, "Average PartialUpdate Indexer Time={} ns", perUpdateIndexerTime);
            // K2LOG_I(log::skvsvr, "PartialUpdate indexer/total {}", (double)perUpdateIndexerTime/(double)perUpdateTime);

            auto querynsecs = (double)k2::nsec(_querySum).count();
            auto queryInitnsecs = (double)k2::nsec(_initItSum).count();
            auto queryForwardnsecs = (double)k2::nsec(_forwardSum).count();
            auto queryEndnsecs = (double)k2::nsec(_handleEndSum).count();
            // In Loop
            auto queryDonensecs = (double)k2::nsec(_isDoneSum).count();
            auto queryAdnsecs = (double)k2::nsec(_scanAdSum).count();
            auto queryGetRecnsecs = (double)k2::nsec(_getRecSum).count();
            auto queryhandlensecs = (double)k2::nsec(_handleSum).count();
            auto queryCachensecs = (double)k2::nsec(_updateCacheSum).count();

            _totalQuery = _totalQuery ? _totalQuery : 1;
            K2LOG_W(log::skvsvr, "completed Query Op={}", _totalQuery);
            K2LOG_W(log::skvsvr, "Average Scan Number per Query={}", (double)_totalScanNum/_totalQuery);
            K2LOG_W(log::skvsvr, "Average Query Time={} ns", (double)querynsecs/_totalQuery);
            K2LOG_W(log::skvsvr, "Average Init scan Itor Time={} ns", (double)queryInitnsecs/_totalQuery);
            K2LOG_W(log::skvsvr, "Average scan forward Time={} ns", (double)queryForwardnsecs/_totalQuery);
            K2LOG_W(log::skvsvr, "Average handle scan end Time={} ns", (double)queryEndnsecs/_totalQuery);
            // Loop
            K2LOG_W(log::skvsvr, "Average Judge Done Time={} ns", (double)queryDonensecs/_totalQuery);
            K2LOG_W(log::skvsvr, "Per scan Judge Done Time={} ns", (double)queryDonensecs/_totalScanNum);
            K2LOG_W(log::skvsvr, "Average Scan advance Time={} ns", (double)queryAdnsecs/_totalQuery);
            K2LOG_W(log::skvsvr, "Per scan advance Time={} ns", (double)queryAdnsecs/_totalScanNum);
            K2LOG_W(log::skvsvr, "Average Get Datarecord Time={} ns", (double)queryGetRecnsecs/_totalQuery);
            K2LOG_W(log::skvsvr, "Per scan get record Time={} ns", (double)queryGetRecnsecs/_totalScanNum);
            K2LOG_W(log::skvsvr, "Average handle rec Time={} ns", (double)queryhandlensecs/_totalQuery);
            K2LOG_W(log::skvsvr, "Per scan handle rec Time={} ns", (double)queryhandlensecs/_totalScanNum);
            K2LOG_W(log::skvsvr, "Average update cache Time={} ns", (double)queryCachensecs/_totalQuery);
            K2LOG_W(log::skvsvr, "Per scan update cache Time={} ns", (double)queryCachensecs/_totalScanNum);

            K2LOG_W(log::skvsvr, "Total size of indexer={}MB", (double)_indexer.size()/(1024.0*1024.0));
        });
}

seastar::future<std::tuple<Status, dto::K23SIReadResponse>>
_makeReadOK(dto::DataRecord* rec) {
    if (rec == nullptr || rec->isTombstone) {
        K2LOG_D(log::skvsvr,"Read result not OK!");
        return RPCResponse(dto::K23SIStatus::KeyNotFound("read did not find key"), dto::K23SIReadResponse{});
    }

    auto response = dto::K23SIReadResponse();
    response.value = rec->value.share();
    K2LOG_D(log::skvsvr,"Read result is OK: {}",response);
    return RPCResponse(dto::K23SIStatus::OK("read succeeded"), std::move(response));
}

// Helper for iterating over the indexer, modifies it to end() if iterator would go past the target schema
// or if it would go past begin() for reverse scan. Starting iterator must not be end() and must
// point to a record with the target schema
void K23SIPartitionModule::_scanAdvance(IndexerIterator& it, bool reverseDirection, const String& schema) {
    // 2.2 Find next itor
    k2::TimePoint scanAdStartT = k2::Clock::now();

    if (!reverseDirection) {
        ++it;
        if (it != _indexer.end() && (_indexer.extractFromIter(it))->get_key().schemaName != schema) {
            it = _indexer.end();
        }
        K2LOG_D(log::indexer, "Scan Advance at key {}", it!=_indexer.end() ? (_indexer.extractFromIter(it))->get_key() : dto::Key());
        _scanAdSum += k2::Clock::now() - scanAdStartT;
        return;
    }

    if (it == _indexer.begin()) {
        it = _indexer.end();
    } else {
        --it;
        K2LOG_D(log::indexer, "Reverse scan Advance at key {}", it!=_indexer.end() ? (_indexer.extractFromIter(it))->get_key() : dto::Key());
        if ((_indexer.extractFromIter(it))->get_key().schemaName != schema) {
            it = _indexer.end();
        }
    }
    _scanAdSum += k2::Clock::now() - scanAdStartT;
}

// Helper for handleQuery. Returns an iterator to start the scan at, accounting for
// desired schema and (eventually) reverse direction scan
IndexerIterator K23SIPartitionModule::_initializeScan(const dto::Key& start, bool reverse, bool exclusiveKey) {
    K2LOG_D(log::skvsvr, "Initialize {} scanner with start key {}, exclusive key = {}", reverse, start, exclusiveKey);
    auto key_it = _indexer.lower_bound(start);
    // For reverse direction scan, key_it may not be in range because of how lower_bound works, so fix that here.
    // IF start key is empty, it means this reverse scan start from end of table OR
    //      if lower_bound returns a _indexer.end(), it also means reverse scan should start from end of table;
    // ELSE IF lower_bound returns a key equal to start AND exclusiveKey is true, reverse advance key_it once;
    // ELSE IF lower_bound returns a key bigger than start, find the first key not bigger than start;
    if (reverse) {
        if (start.partitionKey == "" || key_it == _indexer.end()) {
            K2LOG_D(log::skvsvr, "Empty partition key or end iterator");
            key_it = _indexer.last();
            K2LOG_D(log::skvsvr, "Empty partition key or end iterator end");
        } else if ((_indexer.extractFromIter(key_it))->get_key() == start && exclusiveKey) {
            _scanAdvance(key_it, reverse, start.schemaName);
        } else if ((_indexer.extractFromIter(key_it))->get_key() > start) {
            while ((_indexer.extractFromIter(key_it))->get_key() > start) {
                _scanAdvance(key_it, reverse, start.schemaName);
            }
        }
    }

    if (key_it != _indexer.end() && (_indexer.extractFromIter(key_it))->get_key().schemaName != start.schemaName) {
        key_it = _indexer.end();
    }
    K2LOG_D(log::skvsvr, "lower bound of start key {} is {}", start, key_it != _indexer.end() ? (_indexer.extractFromIter(key_it))->get_key() : dto::Key() );
    return key_it;
}

// Helper for handleQuery. Checks to see if the indexer scan should stop.
bool K23SIPartitionModule::_isScanDone(const IndexerIterator& it, const dto::K23SIQueryRequest& request,
                                       size_t response_size) {
    // if (it == _indexer.end()) {
    //     return true;
    // } else if ((_indexer.extractFromIter(it))->get_key() == request.key) {
    //     // Start key as inclusive overrides end key as exclusive
    //     return false;
    // } else if (!request.reverseDirection && (_indexer.extractFromIter(it))->get_key() >= request.endKey &&
    //            request.endKey.partitionKey != "") {
    //     return true;
    // } else if (request.reverseDirection && (_indexer.extractFromIter(it))->get_key() <= request.endKey) {
    //     return true;
    // } else if (request.recordLimit >= 0 && response_size == (uint32_t)request.recordLimit) {
    //     return true;
    // } else if (response_size == _config.paginationLimit()) {
    //     return true;
    // }
    // K2LOG_D(log::skvsvr, "Continue to scan");
    // return false;
    bool res = false;
    //2.1 Check if scan is done
    k2::TimePoint scanDoneStartT = k2::Clock::now();
    if (it == _indexer.end()) {
        res = true;
    } else if ((_indexer.extractFromIter(it))->get_key() == request.key) {
        // Start key as inclusive overrides end key as exclusive
        res = false;
    } else if (!request.reverseDirection && (_indexer.extractFromIter(it))->get_key() >= request.endKey &&
               request.endKey.partitionKey != "") {
        res = true;
    } else if (request.reverseDirection && (_indexer.extractFromIter(it))->get_key() <= request.endKey) {
        res = true;
    } else if (request.recordLimit >= 0 && response_size == (uint32_t)request.recordLimit) {
        res = true;
    } else if (response_size == _config.paginationLimit()) {
        res = true;
    }
    K2LOG_D(log::skvsvr, "Continue to scan");
    _isDoneSum += k2::Clock::now() - scanDoneStartT;
    return res;
}

// Helper for handleQuery. Returns continuation token (aka response.nextToScan)
dto::Key K23SIPartitionModule::_getContinuationToken(const IndexerIterator& it,
                    const dto::K23SIQueryRequest& request, dto::K23SIQueryResponse& response, size_t response_size) {
    // Three cases where scan is for sure done:
    // 1. Record limit is reached
    // 2. Iterator is not end() but is >= user endKey
    // 3. Iterator is at end() and partition bounds contains endKey
    if ((request.recordLimit >= 0 && response_size == (uint32_t)request.recordLimit) ||
        // Test for past user endKey:
        (it != _indexer.end() &&
            (request.reverseDirection ? (_indexer.extractFromIter(it))->get_key() <= request.endKey : (_indexer.extractFromIter(it))->get_key() >= request.endKey && request.endKey.partitionKey != "")) ||
        // Test for partition bounds contains endKey and we are at end()
        (it == _indexer.end() &&
            (request.reverseDirection ?
            _partition().keyRangeV.startKey <= request.endKey.partitionKey :
            request.endKey.partitionKey <= _partition().keyRangeV.endKey && request.endKey.partitionKey != ""))) {
        K2LOG_D(log::skvsvr, "This case");
        return dto::Key();
    }
    else if (it != _indexer.end()) {
        // This is the paginated case
        response.exclusiveToken = false;
        return (_indexer.extractFromIter(it))->get_key();
    }

    // This is the multi-partition case
    if (request.reverseDirection) {
        response.exclusiveToken = true;
        return dto::Key {
            request.key.schemaName,
            _partition().keyRangeV.startKey,
            ""
        };
    } else {
        response.exclusiveToken = false;
        return dto::Key {
            request.key.schemaName,
            _partition().keyRangeV.endKey,
            ""
        };
    }
}

// Makes the SKVRecord and applies the request's filter to it. If the returned Status is not OK,
// the caller should return the status in the query response. Otherwise bool in tuple is whether
// the filter passed
std::tuple<Status, bool> K23SIPartitionModule::_doQueryFilter(dto::K23SIQueryRequest& request,
                                                              dto::SKVRecord::Storage& storage) {
    // We know the schema name exists because it is validated at the beginning of handleQuery
    auto schemaIt = _schemas.find(request.key.schemaName);
    auto versionIt = schemaIt->second.find(storage.schemaVersion);
    if (versionIt == schemaIt->second.end()) {
        return std::make_tuple(dto::K23SIStatus::OperationNotAllowed(
            "Schema version of found record does not exist"), false);
    }

    dto::SKVRecord record(request.collectionName, versionIt->second, storage.share(), true);
    bool keep = false;
    Status status = dto::K23SIStatus::OK;

    try {
        keep = request.filterExpression.evaluate(record);
    }
    catch(dto::NoFieldFoundException&) {}
    catch(dto::TypeMismatchException&) {}
    catch (dto::DeserializationError&) {
        status = dto::K23SIStatus::OperationNotAllowed("DeserializationError in query filter");
    }
    catch (dto::InvalidExpressionException&) {
        status = dto::K23SIStatus::OperationNotAllowed("InvalidExpression in query filter");
    }

    return std::make_tuple(std::move(status), keep);
}

seastar::future<std::tuple<Status, dto::K23SIQueryResponse>>
K23SIPartitionModule::handleQuery(dto::K23SIQueryRequest&& request, dto::K23SIQueryResponse&& response, FastDeadline deadline) {
    K2LOG_D(log::skvsvr, "Partition: {}, received query {}", _partition, request);

    uint64_t numScans = 0;
    Status validateStatus = _validateReadRequest(request);
    if (!validateStatus.is2xxOK()) {
        return RPCResponse(std::move(validateStatus), dto::K23SIQueryResponse{});
    }
    if (_partition.getHashScheme() != dto::HashScheme::Range) {
            return RPCResponse(dto::K23SIStatus::OperationNotAllowed("Query not implemented for hash partitioned collection"), dto::K23SIQueryResponse{});
    }
    auto startT = k2::Clock::now();
    // 1. initialize scan iterator
    IndexerIterator key_it = _initializeScan(request.key, request.reverseDirection, request.exclusiveKey);
    _initItSum += k2::Clock::now() - startT;

    // 2. Scan Forward
    auto forwardT = k2::Clock::now();
    for (; !_isScanDone(key_it, request, response.results.size());
                        _scanAdvance(key_it, request.reverseDirection, request.key.schemaName)) {
        ++numScans;
        // 2.3 Get data record from KVNode
        auto getStartT = k2::Clock::now();
        auto& KVNode = *_indexer.extractFromIter(key_it);
        //dto::DataRecord* record = KVNode.get_datarecord(request.mtr.timestamp);
        int order;
        dto::DataRecord* record = KVNode.get_datarecord(request.mtr.timestamp, order, pbrb);
        // TODO: Use hot record.
        NodeVerMetadata verMD = KVNode.getNodeVerMetaData(order, pbrb);
        if (verMD.isHot)
            record = static_cast<dto::DataRecord *>(pbrb->getPlogAddrRow(record));

        // read the previouse datarecord
        auto read_pmem_status = _enginePtr->read(record->valuePmemPtr);
        if (!std::get<0>(read_pmem_status).is2xxOK()){
            K2LOG_E(log::skvsvr,"-------Partition {}  read pmem error :{}",
            _partition, std::get<0>(read_pmem_status).message);
        }
        std::get<1>(read_pmem_status).read(record->value);

        K2LOG_D(log::skvsvr, "Scan at key {}", KVNode.get_key());
        _getRecSum += k2::Clock::now() - getStartT;

        auto handleStartT = k2::Clock::now();
        bool needPush = record != nullptr && record->status == dto::DataRecord::WriteIntent && record->timestamp.compareCertain(request.mtr.timestamp) < 0;

        // 2.4 handle Rec
        if (!record) {
            // happy case: we either had no versions, or all versions were newer than the requested timestamp
            _handleSum += k2::Clock::now() - handleStartT;
            continue;
        }

        // happy case: either committed, or txn is reading its own write
        if (record && !needPush) {
            if (!record->isTombstone) {
                auto [status, keep] = _doQueryFilter(request, record->value);
                if (!status.is2xxOK()) {
                    return RPCResponse(std::move(status), dto::K23SIQueryResponse{});
                }
                if (!keep) {
                    continue;
                }

                // apply projection if the user call addProjection
                if (request.projection.size() == 0) {
                    // want all fields
                    response.results.push_back(record->value.share());
                } else {
                    // serialize partial SKVRecord according to projection
                    dto::SKVRecord::Storage storage;
                    bool success = _makeProjection(record->value, request, storage);
                    if (!success) {
                        K2LOG_W(log::skvsvr, "Error making projection!");
                        return RPCResponse(dto::K23SIStatus::InternalError("Error making projection"),
                                                dto::K23SIQueryResponse{});
                    }

                    response.results.push_back(std::move(storage));
                }
            }
            _handleSum += k2::Clock::now() - handleStartT;
            continue;
        }

        // If we get here it is a conflict, first decide to push or return early
        if (response.results.size() >= _config.queryPushLimit()) {
            _handleSum += k2::Clock::now() - handleStartT;
            break;
        }
        K2LOG_D(log::skvsvr, "query from txn {}, updates read cache for key range {} - {}",
                request.mtr, request.key, (_indexer.extractFromIter(key_it))->get_key());

        // Do a push but we need to save our place in the query
        // TODO we can test the filter condition against the WI and last committed version and possibly
        // avoid a push
        // Must update read cache before doing an async operation

        //2.6 Update ReadCache
        auto updateStartT = k2::Clock::now();
        /*request.reverseDirection ?
            _readCache->insertInterval((_indexer.extractFromIter(key_it))->get_key(), request.key, request.mtr.timestamp) :
            _readCache->insertInterval(request.key, (_indexer.extractFromIter(key_it))->get_key(), request.mtr.timestamp);
        */
        K2LOG_D(log::skvsvr, "About to PUSH in query request");
        request.key = (_indexer.extractFromIter(key_it))->get_key(); // if we retry, do so with the key we're currently iterating on
        // add metrics
        _queryPageScans.add(numScans);
        _queryPageReturns.add(response.results.size());
        _updateCacheSum += k2::Clock::now() - updateStartT;
        return _doPush(request.key, record->timestamp, request.mtr, deadline)
        .then([this, request=std::move(request),
                        resp=std::move(response), deadline](auto&& retryChallenger) mutable {
            if (!retryChallenger.is2xxOK()) {
                // sitting transaction won. Abort the incoming request
                return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in query push"), dto::K23SIQueryResponse{});
            }
            return handleQuery(std::move(request), std::move(resp), deadline);
        });
    }
    _forwardSum += k2::Clock::now() - forwardT;

    // 3. handle scan request end
    // Read cache update block
    k2::TimePoint endStartT = k2::Clock::now();
    dto::Key endInterval;
    if (key_it == _indexer.end()) {
        // For forward direction we need to lock the whole range of the schema, which we do
        // by appending a character, which may overshoot the range but is correct
        endInterval.schemaName = request.reverseDirection ? request.key.schemaName : request.key.schemaName + "a";
        endInterval.partitionKey = "";
        endInterval.rangeKey = "";
    } else {
        endInterval = (_indexer.extractFromIter(key_it))->get_key();
    }

    K2LOG_D(log::skvsvr, "query from txn {}, updates read cache for key range {} - {}",
                request.mtr, request.key, endInterval);
    /*request.reverseDirection ?
        _readCache->insertInterval(endInterval, request.key, request.mtr.timestamp) :
        _readCache->insertInterval(request.key, endInterval, request.mtr.timestamp);
    */

    response.nextToScan = _getContinuationToken(key_it, request, response, response.results.size());
    K2LOG_D(log::skvsvr, "nextToScan: {}, exclusiveToken: {}", response.nextToScan, response.exclusiveToken);

    _queryPageScans.add(numScans);
    _queryPageReturns.add(response.results.size());
    k2::TimePoint endT = k2::Clock::now();
    _handleEndSum += endT - endStartT;
    _querySum += endT - startT;
    ++_totalQuery;
    _totalScanNum += numScans;
    return RPCResponse(dto::K23SIStatus::OK("Query success"), std::move(response));
}

void K23SIPartitionModule::cacheKVRecordtoPBRB(uint32_t SMapIndex, dto::DataRecord* rec, KeyValueNode* nodePtr, int indexFlag) {
    bool inKVNode= nodePtr->check_version_inKVNode(rec->timestamp);
    if(!inKVNode) {
        //K2LOG_I(log::skvsvr, "rec->timestamp:{} not in KVNode, isDonePBRBGC:{}", rec->timestamp, isDonePBRBGC);
        return;
    }
    auto _findPositionStart = k2::now_nsec_count();
    std::pair<BufferPage *, RowOffset> retVal = pbrb->findCacheRowPosition(SMapIndex);
    BufferPage *pagePtr = retVal.first;
    RowOffset rowOffset = retVal.second;
    auto _findPositionEnd = k2::now_nsec_count();
    totalFindPositionns[indexFlag] += _findPositionEnd - _findPositionStart;
    
    if (pagePtr!=nullptr) { //find empty slot
        auto _headerStart = k2::now_nsec_count();
        auto rowAddr = pbrb->cacheRowHeaderFrom(SMapIndex, pagePtr, rowOffset, rec, nodePtr);
        K2LOG_D(log::skvsvr, "--------SMapIndex:{}, rowOffset:{}, rowAddr:{}, pagePtr empty:{}", SMapIndex, rowOffset, rowAddr, pagePtr==nullptr);
        auto _headerEnd = k2::now_nsec_count();
        totalHeaderns[indexFlag] += _headerEnd - _headerStart;

        #ifdef FIXEDFIELD_ROW
        // copy fields
        for(uint32_t j=0; j < schema.fields.size(); j++) {
            auto _copyFieldStart = k2::now_nsec_count();
            if (rec->value.excludedFields.size() && rec->value.excludedFields[j]) {
                // A value of NULL in the record is treated the same as if the field doesn't exist in the record
                K2LOG_I(log::pbrb, "######A value of NULL in the record");
                continue;
            }
            bool success = false;
            _cacheFieldValueToPBRB(SMapIndex, schema.fields[j], rec->value.fieldData, success, pagePtr, rowOffset, j);
            auto _copyFieldEnd = k2::now_nsec_count();
            totalCopyFeildns[indexFlag] += _copyFieldEnd - _copyFieldStart;
        }
        #endif

        #ifdef PAYLOAD_ROW
            auto _copyFieldStart = k2::now_nsec_count();
            pbrb->cacheRowPayloadFromDataRecord(SMapIndex, pagePtr, rowOffset, rec->value.fieldData);
            auto _copyFieldEnd = k2::now_nsec_count();
            totalCopyFeildns[indexFlag] += _copyFieldEnd - _copyFieldStart; 
        #endif

        //pbrb->setRowBitMapPage(pagePtr, rowOffset);
        // update KVNode of indexer.
        auto _updateKBNStart = k2::now_nsec_count();
        void *hotAddr = pbrb->getAddrByPageAndOffset(SMapIndex, pagePtr, rowOffset);

        //int returnValue= nodePtr->insert_hot_datarecord(rec->timestamp, static_cast<dto::DataRecord *>(hotAddr));
        int returnValue= nodePtr->insert_datarecord(static_cast<dto::DataRecord *>(hotAddr), pbrb, rec->timestamp, true, -1);
        K2LOG_D(log::skvsvr, "Cache request.key in KVNode and PBRB, schemaID:{}, returnValue:{}", SMapIndex, returnValue);
        K2LOG_D(log::skvsvr, "Stored hot address: {} in node", hotAddr);
        auto _updateKVNEnd = k2::now_nsec_count();
        totalUpdateKVNodens[indexFlag] += _updateKVNEnd - _updateKBNStart;
        if(returnValue==1) {
            K2LOG_I(log::skvsvr, "update KVNode failed!!!!");
            return;
        } else {
            pbrb->setRowBitMapPage(pagePtr, rowOffset);
        }
        //pbrb->checkPosition(pagePtr, SMapIndex, rowOffset);
        //nodePtr->printAll();
        //pbrb->printRowsBySchema(SMapIndex);
        rec->value.fieldData.seek(0);
    }
}

seastar::future<std::tuple<Status, dto::K23SIReadResponse>>
K23SIPartitionModule::handleRead(dto::K23SIReadRequest&& request, FastDeadline deadline) {
    K2LOG_D(log::skvsvr, "Partition: {}, received read {}", _partition, request);
    /*Status validateStatus = _validateReadRequest(request);
    if (!validateStatus.is2xxOK()) {
        return RPCResponse(std::move(validateStatus), dto::K23SIReadResponse{});
    }*/

#ifdef READ_BREAKDOWN
    auto _readStart = k2::now_nsec_count();
    // find the record we should return
    k2::TimePoint startT = k2::Clock::now();
    IndexerIterator it = _indexer.find(request.key);
    
    /////////////////// 1. Find record in indexer
    _readIndexerSum += k2::Clock::now() - startT;
    if(it == _indexer.end()) {
        return _makeReadOK(nullptr);
    }
    k2::TimePoint itorStartT = k2::Clock::now();
    KeyValueNode* nodePtr = _indexer.extractFromIter(it);
    _readItorSum += k2::Clock::now() - itorStartT;

    auto _indexEnd = k2::now_nsec_count();
    
    int indexFlag = getSchemaArrayIndex(request.key.schemaName);
    totalIndexns[indexFlag] += _indexEnd -_readStart;

    K2LOG_D(log::skvsvr, "read from txn {}, updates read cache for key {}",
                request.mtr, request.key);
    // update the read cache to lock out any future writers which may attempt to modify the key range
    // before this read's timestamp
    /*auto _updateCacheStart = k2::now_nsec_count();
    k2::TimePoint cacheStartT = k2::Clock::now();
    _readCache->insertInterval(request.key, request.key, request.mtr.timestamp, totalSerachTreens[indexFlag], totalUpdateTreens[indexFlag]);
    //////////////////// 2. update read cache
    _readCacheSum += k2::Clock::now() - cacheStartT;
    auto _updateCacheEnd = k2::now_nsec_count();
    totalUpdateCachens[indexFlag] += _updateCacheEnd - _updateCacheStart;
    */
    // If there is WI, if same TX return WI or set needPush true
    // else return right version or null&&needPush = false
    auto _recordStart = k2::now_nsec_count();
    int order;
    K2LOG_D(log::skvsvr, "Ready to read datarecord");

    /////////// 3. read Node, get record and update pbrb
    k2::TimePoint nodeStartT = k2::Clock::now();
    dto::DataRecord* rec = nodePtr->get_datarecord(request.mtr.timestamp, order, pbrb);
    auto _recordEnd = k2::now_nsec_count();
    totalGetAddrns[indexFlag] += _recordEnd - _recordStart;
    //nodePtr->printAll();

    if (rec != nullptr)
        K2LOG_D(log::skvsvr, "Node info ====== Timestamp: {}; Order: {}, SchemaVersion: {}", request.mtr.timestamp, order, rec->value.schemaVersion);

    dto::DataRecord* result = nullptr;
    if (rec != nullptr) {
        
        if (order == -1) {
            // case 1: cold version (not in kvnode)
            // return directly.
            K2LOG_I(log::skvsvr, "Case 1: Cold Version not in KVNode");
            auto _readNVMtart = k2::now_nsec_count();
            k2::OperationLatencyReporter reporter(_readPmemLogLatency);
            // read version from pmem engine
            auto read_pmem_status = _enginePtr->read(rec->valuePmemPtr);
            if (!std::get<0>(read_pmem_status).is2xxOK()){
                K2LOG_E(log::skvsvr,"-------Partition {}  read pmem error :{}",
                _partition, std::get<0>(read_pmem_status).message);
            }
            std::get<1>(read_pmem_status).read(rec->value);
            reporter.report();
            auto _readNVMEnd = k2::now_nsec_count();
            totalReadNVMns[indexFlag] += _readNVMEnd - _readNVMtart;
            result = rec;
            NvmReadNum[indexFlag]++;
        }
        else if (order >= 0) {
            // find schema
            auto schemaIt = _schemas.find(request.key.schemaName);
            K2ASSERT(log::skvsvr, schemaIt != _schemas.end(), "Found Schema: {}", schemaIt->first);
            uint32_t sVer;
            if (!nodePtr->is_inmem(order)){
                auto _readNVMtart = k2::now_nsec_count();
                k2::OperationLatencyReporter reporter(_readPmemLogLatency);
                // read version from pmem engine
                auto read_pmem_status = _enginePtr->read(rec->valuePmemPtr);
                if (!std::get<0>(read_pmem_status).is2xxOK()){
                    K2LOG_E(log::skvsvr,"-------Partition {}  read pmem error :{}",
                    _partition, std::get<0>(read_pmem_status).message);
                }
                std::get<1>(read_pmem_status).read(rec->value);
                reporter.report();
                sVer = rec->value.schemaVersion;
                //K2LOG_I(log::skvsvr,"Read datarecord  from pmemLog, schemaVersion:{}, excludedFields:{}",rec->value.schemaVersion, rec->value.excludedFields);
                auto _readNVMEnd = k2::now_nsec_count();
                totalReadNVMns[indexFlag] += _readNVMEnd - _readNVMtart;
            }
            // hot row
            else {
                //K2LOG_I(log::skvsvr,"Read datarecord value from pbrb {}",rec->value);
                sVer = pbrb->getSchemaVer(rec);
            }
            auto schemaVer = schemaIt->second.find(sVer);
            K2ASSERT(log::skvsvr, schemaVer != schemaIt->second.end(), "sVer: {}", sVer);
            dto::Schema& schema = *(schemaVer->second);

        #ifdef OUTPUT_ACCESS_PATTERN
            isRunBenchMark = 1;
            if(request.key.schemaName=="customer" && isRunBenchMark){
                map<dto::Key, int>::iterator result = writeRecordMap.find(request.key);
                if(result != writeRecordMap.end()){
                    ofile<<count++<<' '<<'R'<<' '<<result->second<<endl;
                }else{
                    map<dto::Key, int>::iterator resultR = readRecordMap.find(request.key);
                    if(resultR != readRecordMap.end()){
                        ofile<<count++<<' '<<'R'<<' '<<resultR->second<<endl;
                    }else{
                        readRecordMap.insert(pair<dto::Key, int>(request.key, keySequence));
                        ofile<<count++<<' '<<'R'<<' '<<keySequence<<endl;
                        keySequence++;
                    }
                }
                //K2LOG_I(log::skvsvr, "-request.key: {}, order:{}, schemaName: {}, schemaVer:{}", request.key, order, request.key.schemaName, schemaVer->first);
            }
        #endif

            uint32_t SMapIndex = pbrb->mapCachedSchema(request.key.schemaName, schemaVer->first);
            if(SMapIndex == 0){ //add a new schema to PBRB schema metadata
                SimpleSchema S;
                S.name = request.key.schemaName;
                S.version = schemaVer->first;
                for(uint32_t i = 0; i < schema.fields.size(); i++){
                    S.fields.push_back(schema.fields[i]);
                }
                auto sID = pbrb->addSchemaUMap(S);
                pbrb->createCacheForSchema(sID, S.version);
                SMapIndex = sID+1;
            }

            SMapIndex--;
            K2LOG_D(log::skvsvr, "SMAPIDX: {}", SMapIndex);
            if (!nodePtr->is_inmem(order)) {
                // case 2: cold version (in kvnode)
                K2LOG_D(log::skvsvr, "Case 2: Cold Version in KVNode");
                if (enablePBRB) {
                    //rec->value.fieldData.seek(0);
                    //insert the SKV record to the PBRB cache
                #ifdef SYNC
                    if(isDonePBRBGC){
                        cacheKVRecordtoPBRB(SMapIndex, rec, nodePtr, indexFlag);
                    }
                #endif

                #ifdef ASYNC
                //if(hitRatio[indexFlag] > 0.2 && sampleCounter%2==0) {
                if(hitRatio[indexFlag] > 0.2) {
                    sampleCounter = 1;
                    if(requestQueue.size()< 200 && requestQueue1.size()< 200){
                        //requestEntry *rEntry = new struct requestEntry;
                        requestEntry *rEntry = queuePool.front();
                        if(rEntry == nullptr) {
                            rEntry = new struct requestEntry;
                        } else {
                            queuePool.pop();
                        }
                        rEntry->SMapIndex = SMapIndex;
                        //rEntry->value = rec->value.copy();
                        rEntry->rec = rec;                 
                        rEntry->nodePtr = nodePtr;
                        rEntry->indexFlag = indexFlag;                       
                        if(queueIndex==0){
                            requestQueue.push(rEntry);
                        } else {
                            //K2LOG_I(log::skvsvr, "--queueIndex:{}, queue size:{}", queueIndex, requestQueue1.size());
                            requestQueue1.push(rEntry);
                        }    
                    } 
                /*} else {
                    sampleCounter++;
                }*/     
                }                                   
                #endif
                }

                NvmReadNum[indexFlag]++;
                result = rec;
                //K2LOG_I(log::skvsvr, "Case 2: Read from NVM, sleep:{} us, NvmReadNum:{}", (double)(_End-_Start), NvmReadNum);
            }
            else {
                // case 3: hot version
                pbrbHitNum[indexFlag]++;
                //K2LOG_I(log::skvsvr, "Case 3: Hot Version in KVNode, pbrbHitNum:{}", pbrbHitNum[indexFlag]);
                auto _readPBRBtart = k2::now_nsec_count();
                //void *hotAddr = static_cast<void *>(rec);
                #ifdef PAYLOAD_ROW                  
                    auto _genRecordStart = k2::now_nsec_count();
                    result = pbrb->generateDataRecord(rec);
                    auto _genRecordEnd = k2::now_nsec_count();
                    totalGenRecordns[indexFlag] += _genRecordEnd - _genRecordStart;

                    /*auto _genPayloadStart = k2::now_nsec_count();
                    result->value.fieldData = Payload(Payload::DefaultAllocator(1024)); 
                    auto _genPayloadEnd = k2::now_nsec_count();
                    allocatePayloadns[indexFlag] += _genPayloadEnd - _genPayloadStart;
                    //result->value= storage.copy();
                    pbrb->generateSKVRecordByPayloadRow(SMapIndex, rec, true, result->value.fieldData, totalReadCopyFeildns[indexFlag]);
                    */
                    storagePayload.seek(0);
                    pbrb->generateSKVRecordByPayloadRow(SMapIndex, rec, true, storagePayload, totalReadCopyFeildns[indexFlag]);
                    //result->value.fieldData = storage.fieldData.copy();
                #endif
                
                #ifdef FIXEDFIELD_ROW
                    dto::SKVRecord *sRec = pbrb->generateSKVRecordByRow(SMapIndex, rec, request.collectionName, schemaVer->second, false, totalReadCopyFeildns[indexFlag]);
                    auto _genRecordStart = k2::now_nsec_count();
                    result = pbrb->generateDataRecord(sRec, rec);
                    auto _genRecordEnd = k2::now_nsec_count();
                    totalGenRecordns[indexFlag] += _genRecordEnd - _genRecordStart;
                #endif
                //result = static_cast<dto::DataRecord *>(pbrb->getPlogAddrRow(rec));
                //pbrb->setTimestampRow(hotAddr, request.mtr.timestamp);
                auto _readPBRBEnd = k2::now_nsec_count();
                totalReadPBRBns[indexFlag] += _readPBRBEnd - _readPBRBtart;
            }
        }
        totalReadSize[indexFlag] += result->value.fieldData.getSize();     
    }
    ////////////////////////////////////
    _readNodeSum += k2::Clock::now() - nodeStartT;
    K2LOG_D(log::skvsvr, "Result of Getrecord with status{} key{}", result ? result->status : -1, nodePtr->get_key());
    
    ++_totalRead;
    _readSum += k2::Clock::now() - startT;
    
    // case need push: WI && WI.ts < tx.timestamp
    bool needPush = result != nullptr && result->status == dto::DataRecord::WriteIntent && result->timestamp.compareCertain(request.mtr.timestamp) < 0;
    
    auto _readEnd = k2::now_nsec_count();
    totalReadns[indexFlag] += _readEnd - _readStart; 

    if (!needPush) {
        return _makeReadOK(result);
    }
    K2LOG_D(log::skvsvr, "need push with state{}, record ts={}, mtr={}",
                result->status, result->timestamp, request.mtr.timestamp);
    K2LOG_D(log::skvsvr, "need push node states:size={} begin is empty {} second is empty {} third is empty {}",
                nodePtr->size(), nodePtr->_getpointer(0)==nullptr,nodePtr->_getpointer(1)==nullptr,nodePtr->_getpointer(2)==nullptr);

    // record is still pending and isn't from same transaction.
    return _doPush(request.key,rec->timestamp, request.mtr, deadline)
        .then([this, request=std::move(request), deadline](auto&& retryChallenger) mutable {
            if (!retryChallenger.is2xxOK()) {
                return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in read push"), dto::K23SIReadResponse{});
            }
            return handleRead(std::move(request), deadline);
        });
#endif

#ifdef NO_READ_BREAKDOWN
    K2LOG_D(log::skvsvr, "read from txn {}, updates read cache for key {}",
                request.mtr, request.key);
    // update the read cache to lock out any future writers which may attempt to modify the key range
    // before this read's timestamp
    //_readCache->insertInterval(request.key, request.key, request.mtr.timestamp);
    // find the record we should return
    auto idxStartT = k2::Clock::now();
    IndexerIterator it = _indexer.find(request.key);
    // 2. Find record in indexer
    _readIndexerSum += k2::Clock::now() - idxStartT;
    if(it == _indexer.end()) {
        return _makeReadOK(nullptr);
    }
    k2::TimePoint itorStartT = k2::Clock::now();
    KeyValueNode* nodePtr = _indexer.extractFromIter(it);
    _readItorSum += k2::Clock::now() - itorStartT;
    // If there is WI, if same TX return WI or set needPush true
    // else return right version or null&&needPush = false
    // DataRecord* rec = _getDataRecordForRead(versions, request.mtr.timestamp);
    // bool needPush = !rec ? _checkPushForRead(versions, request.mtr.timestamp) : false;
    int order;
    K2LOG_D(log::skvsvr, "Ready to read datarecord");
    dto::DataRecord* rec = nodePtr->get_datarecord(request.mtr.timestamp, order, pbrb);

    if (rec != nullptr)
        K2LOG_D(log::skvsvr, "Node info ====== Timestamp: {}; Order: {}, SchemaVersion: {}", request.mtr.timestamp, order, rec->value.schemaVersion);
    //DataRecord* result = rec;
    DataRecord* result = nullptr;
    if (rec != nullptr) {
        
        if (order == -1) {
            // case 1: cold version (not in kvnode)
            // return directly.
            K2LOG_D(log::skvsvr, "Case 1: Cold Version not in KVNode");
            result = rec;
        }
        
        else if (order >= 0) {
            // find schema
            auto schemaIt = _schemas.find(request.key.schemaName);
            K2ASSERT(log::skvsvr, schemaIt != _schemas.end(), "Found Schema: {}", schemaIt->first);
            uint32_t sVer;

            // datarecord
            if (!nodePtr->is_inmem(order))
                sVer = rec->value.schemaVersion;
            // hot row
            else {
                sVer = pbrb->getSchemaVer(rec);
            }
            auto schemaVer = schemaIt->second.find(sVer);
            K2ASSERT(log::skvsvr, schemaVer != schemaIt->second.end(), "sVer: {}", sVer);
            dto::Schema& schema = *(schemaVer->second);
            K2LOG_D(log::skvsvr, "----request.key: {}, schemaName: {}, schemaVer:{}", request.key, request.key.schemaName, schemaVer->first);

            uint32_t SMapIndex = pbrb->mapCachedSchema(request.key.schemaName, schemaVer->first);
            if(SMapIndex == 0){ //add a new schema to PBRB schema metadata
                SimpleSchema S;
                S.name = request.key.schemaName;
                S.version = schemaVer->first;
                for(uint32_t i = 0; i < schema.fields.size(); i++){
                    S.fields.push_back(schema.fields[i]);
                }
                auto sID = pbrb->addSchemaUMap(S);
                pbrb->createCacheForSchema(sID, S.version);
                SMapIndex = sID+1;
            }

            // insert the SKV record to the PBRB cache, return the RAM address of the cached slot
            SMapIndex--;
            K2LOG_D(log::skvsvr, "SMAPIDX: {}", SMapIndex);
            // pbrb->printRowsBySchema(SMapIndex);
            if (!nodePtr->is_inmem(order)) {
                // case 2: cold version (in kvnode)
                K2LOG_D(log::skvsvr, "Case 2: Cold Version in KVNode");
                std::pair<BufferPage *, RowOffset> retVal = pbrb->findCacheRowPosition(SMapIndex); 
                //std::pair<BufferPage *, RowOffset> retVal = pbrb->findCacheRowPosition(SMapIndex, request.key); 
                BufferPage *pagePtr = retVal.first;
                RowOffset rowOffset = retVal.second;
        if (pagePtr!=nullptr) { //find empty slot    
                auto rowAddr = pbrb->cacheRowHeaderFrom(SMapIndex, pagePtr, rowOffset, rec, nodePtr);
                K2LOG_D(log::skvsvr, "----SMapIndex:{}, rowOffset:{}, rowAddr:{}, pagePtr empty:{}", SMapIndex, rowOffset, rowAddr, pagePtr==nullptr);
                ////////////////////////
                #ifdef FIXEDFIELD_ROW
                // copy fields
                for(uint32_t j=0; j < schema.fields.size(); j++){
                    if (rec->value.excludedFields.size() && rec->value.excludedFields[j]) {
                        // A value of NULL in the record is treated the same as if the field doesn't exist in the record
                        continue;
                    }
                    bool success = false;
                    _cacheFieldValueToPBRB(SMapIndex, schema.fields[j], rec->value.fieldData, success, pagePtr, rowOffset, j);
                }
                #endif

                #ifdef PAYLOAD_ROW
                    pbrb->cacheRowPayloadFromDataRecord(SMapIndex, pagePtr, rowOffset, rec->value.fieldData);//////
                #endif

                pbrb->setRowBitMapPage(pagePtr, rowOffset);
                // update indexer.
                void *hotAddr = pbrb->getAddrByPageAndOffset(SMapIndex, pagePtr, rowOffset);
                nodePtr->insert_hot_datarecord(rec->timestamp, static_cast<dto::DataRecord *>(hotAddr));
                K2LOG_D(log::skvsvr, "Stored hot address: {} in node", hotAddr);
                //#ifdef FIXEDFIELD_ROW
                //    pbrb->printFieldsRow(pagePtr, rowOffset);
                //#endif
                rec->value.fieldData.seek(0);
        }
                result = rec;
            }
            else {
                // case 3: hot version
                //K2LOG_I(log::skvsvr, "Case 3: Hot Version in KVNode, pbrbHitNum:{}", pbrbHitNum);
                void *hotAddr = static_cast<void *>(rec);
                #ifdef PAYLOAD_ROW
                    dto::SKVRecord *sRec = pbrb->generateSKVRecordByRow(SMapIndex, hotAddr, request.collectionName, schemaVer->second, true);
                    result = pbrb->generateDataRecord(sRec, hotAddr);
                    result->value.fieldData.seek(0);
                #endif
                
                #ifdef FIXEDFIELD_ROW
                    dto::SKVRecord *sRec = pbrb->generateSKVRecordByRow(SMapIndex, hotAddr, request.collectionName, schemaVer->second, false);
                    result = pbrb->generateDataRecord(sRec, hotAddr);
                #endif
                //result = static_cast<dto::DataRecord *>(pbrb->getPlogAddrRow(rec));
                // TODO: Evict expired versions.
            }
        }
    }
    ////////////////////////////////////
    // case need push: WI && WI.ts < tx.timestamp
    // auto result = rec;
    K2LOG_D(log::skvsvr, "Result of Getrecord with status{} key{}", result ? result->status : -1, nodePtr->get_key());
    bool needPush = result != nullptr && result->status == dto::DataRecord::WriteIntent && result->timestamp.compareCertain(request.mtr.timestamp) < 0;

    // happy case: either committed, or txn is reading its own write, or there is no matching version
    if (!needPush) {
        return _makeReadOK(result);
    }
    K2LOG_D(log::skvsvr, "need push with state{}, record ts={}, mtr={}",
                rec->status, rec->timestamp, request.mtr.timestamp);
    K2LOG_D(log::skvsvr, "need push node states:size={} begin is empty {} second is empty {} third is empty {}",
                nodePtr->size(), nodePtr->_getpointer(0)==nullptr,nodePtr->_getpointer(1)==nullptr,nodePtr->_getpointer(2)==nullptr);

    // record is still pending and isn't from same transaction.
    return _doPush(request.key,result->timestamp, request.mtr, deadline)
        .then([this, request=std::move(request), deadline](auto&& retryChallenger) mutable {
            if (!retryChallenger.is2xxOK()) {
                return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in read push"), dto::K23SIReadResponse{});
            }
            return handleRead(std::move(request), deadline);
        });
#endif

}

std::size_t K23SIPartitionModule::_findField(const dto::Schema schema, k2::String fieldName ,dto::FieldType fieldtype) {
    std::size_t fieldNumber = -1;
    for (std::size_t i = 0; i < schema.fields.size(); ++i) {
        if (schema.fields[i].name == fieldName && schema.fields[i].type == fieldtype) {
            return i;
        }
    }
    return fieldNumber;
}

template <typename T>
void _advancePayloadPosition(const dto::SchemaField& field, Payload& payload, bool& success) {
    (void) field;
    payload.skip<T>();
    success = true;
}

template <typename T>
void _copyPayloadBaseToUpdate(const dto::SchemaField& field, Payload& base, Payload& update, bool& success) {
    (void) field;
    success = base.copyToPayload<T>(update);
}

template <typename T>
void _getNextPayloadOffset(const dto::SchemaField& field, Payload& base, uint32_t baseCursor,
                           std::vector<uint32_t>& fieldsOffset, bool& success) {
    (void) field;
    (void) base;
    uint32_t tmpOffset = fieldsOffset[baseCursor] + sizeof(T);
    fieldsOffset.push_back(tmpOffset);
    success = true;
}

template <>
void _getNextPayloadOffset<String>(const dto::SchemaField& field, Payload& base, uint32_t baseCursor,
                           std::vector<uint32_t>& fieldsOffset, bool& success) {
    (void) field;
    uint32_t strLen;
    base.seek(fieldsOffset[baseCursor]);
    success = base.read(strLen);
    if (!success) return;
    uint32_t tmpOffset = fieldsOffset[baseCursor] + sizeof(uint32_t) + strLen; // uint32_t for length; '\0' doesn't count
    fieldsOffset.push_back(tmpOffset);
}

void K23SIPartitionModule::_cacheFieldValueToPBRB(uint32_t schemaId, const dto::SchemaField& field, Payload& payload, bool& success, BufferPage *pagePtr, RowOffset rowOffset, uint32_t fieldID){
    // K2_DTO_CAST_APPLY_FIELD_VALUE(_getFieldData, field, payload, success, fieldID);
    // K2LOG_I(log::skvsvr, "Ready to cache field: [field.type: {}, field.name: {}, value: {}, strSize: {}]", field.type, field.name, value, strSize);
    switch (field.type) {
        case dto::FieldType::STRING: {
            k2::String value{};
            success = payload.read(value);
            size_t strSize = value.size();
            K2LOG_D(log::skvsvr, "field.type: {}, field.name: {}, value:{}, strSize:{}", field.type, field.name, value, strSize);
            pbrb->cacheRowFieldFromDataRecord(schemaId, pagePtr, rowOffset, &value, strSize, fieldID, true);
        } break;
        case dto::FieldType::INT16T: {
            int16_t value{};
            success = payload.read(value);
            //K2LOG_I(log::skvsvr, "field.type: {}, field.name: {}, value:{}", field.type, field.name, value);
            pbrb->cacheRowFieldFromDataRecord(schemaId, pagePtr, rowOffset, &value, 0, fieldID, false);
        } break;
        case dto::FieldType::INT32T: {
            int32_t value{};
            success = payload.read(value);
            //K2LOG_I(log::skvsvr, "field.type: {}, field.name: {}, value:{}", field.type, field.name, value);
            pbrb->cacheRowFieldFromDataRecord(schemaId, pagePtr, rowOffset, &value, 0, fieldID, false);
        } break;
        case dto::FieldType::INT64T: {
            int64_t value{};
            success = payload.read(value);
            //K2LOG_I(log::skvsvr, "field.type: {}, field.name: {}, value:{}", field.type, field.name, value);
            pbrb->cacheRowFieldFromDataRecord(schemaId, pagePtr, rowOffset, &value, 0, fieldID, false);
        } break;
        case dto::FieldType::FLOAT: {
            float value{};
            success = payload.read(value);
            //K2LOG_I(log::skvsvr, "field.type: {}, field.name: {}, value:{}", field.type, field.name, value);
            pbrb->cacheRowFieldFromDataRecord(schemaId, pagePtr, rowOffset, &value, 0, fieldID, false);
        } break;
        case dto::FieldType::DOUBLE: {
            double value{};
            success = payload.read(value);
            //K2LOG_I(log::skvsvr, "field.type: {}, field.name: {}, value:{}", field.type, field.name, value);
            pbrb->cacheRowFieldFromDataRecord(schemaId, pagePtr, rowOffset, &value, 0, fieldID, false);
        } break;
        case dto::FieldType::BOOL: {
            bool value{};
            success = payload.read(value);
            //K2LOG_I(log::skvsvr, "field.type: {}, field.name: {}, value:{}", field.type, field.name, value);
            pbrb->cacheRowFieldFromDataRecord(schemaId, pagePtr, rowOffset, &value, 0, fieldID, false);
        } break;
        case dto::FieldType::DECIMAL64: {
            std::decimal::decimal64 value{};
            success = payload.read(value);
            //K2LOG_I(log::skvsvr, "field.type: {}, field.name: {}, value:{}", field.type, field.name, value);
            pbrb->cacheRowFieldFromDataRecord(schemaId, pagePtr, rowOffset, &value, 0, fieldID, false);
        } break;
        case dto::FieldType::DECIMAL128: {
            std::decimal::decimal128 value{};
            success = payload.read(value);
            //K2LOG_I(log::skvsvr, "field.type: {}, field.name: {}, value:{}", field.type, field.name, value);
            pbrb->cacheRowFieldFromDataRecord(schemaId, pagePtr, rowOffset, &value, 0, fieldID, false);
        } break;
        case dto::FieldType::FIELD_TYPE: {
            dto::FieldType value{};
            success = payload.read(value);
            //K2LOG_I(log::skvsvr, "field.type: {}, field.name: {}, value:{}", field.type, field.name, value);
            pbrb->cacheRowFieldFromDataRecord(schemaId, pagePtr, rowOffset, &value, 0, fieldID, false);
        } break;
        default:
            auto msg = fmt::format(
                "cannot apply field of type {}", field.type);
            throw dto::TypeMismatchException(msg);
    }
    K2LOG_D(log::skvsvr, "Stored Field Data.");
}

bool K23SIPartitionModule::_isUpdatedField(uint32_t fieldIdx, std::vector<uint32_t> fieldsForPartialUpdate) {
    for(std::size_t i = 0; i < fieldsForPartialUpdate.size(); ++i) {
        if (fieldIdx == fieldsForPartialUpdate[i]) return true;
    }
    return false;
}

bool K23SIPartitionModule::_makeFieldsForSameVersion(dto::Schema& schema, dto::K23SIWriteRequest& request, dto::DataRecord& version) {
    K2LOG_D(log::skvsvr, "Make Fields For Same Version");
    Payload basePayload = version.value.fieldData.shareAll();   // base payload
    Payload payload(Payload::DefaultAllocator());                     // payload for new record

    for (std::size_t i = 0; i < schema.fields.size(); ++i) {
        if (_isUpdatedField(i, request.fieldsForPartialUpdate)) {
            // this field is updated
            if (request.value.excludedFields[i] == 0 &&
                    (version.value.excludedFields.empty() || version.value.excludedFields[i] == 0)) {
                // Request's payload has new value, AND
                // base payload also has this field (empty()==true indicate that base payload contains every fields).
                // Then use 'req' payload, at the mean time _advancePosition of base payload.
                bool success = false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], request.value.fieldData, payload, success);
                if (!success) return false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], basePayload, success);
                if (!success) return false;
            } else if (request.value.excludedFields[i] == 0 &&
                    (!version.value.excludedFields.empty() && version.value.excludedFields[i] == 1)) {
                // Request's payload has new value, AND
                // base payload skipped this field.
                // Then use 'req' value, do not _advancePosition of base payload.
                bool success = false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], request.value.fieldData, payload, success);
                if (!success) return false;
            } else if (request.value.excludedFields[i] == 1 &&
                    (version.value.excludedFields.empty() || version.value.excludedFields[i] == 0)) {
                // Request's payload skipped this value(means the field is updated to NULL), AND
                // base payload has this field.
                // Then exclude this field, at the mean time _advancePosition of base payload.
                request.value.excludedFields[i] = true;
                bool success = false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], basePayload, success);
                if (!success) return false;
            } else {
                // Request's payload skipped this value, AND base payload also skipped this field.
                // set excludedFields[i]
                request.value.excludedFields[i] = true;
            }
        } else {
            // this field is NOT updated
            if (request.value.excludedFields[i] == 0 &&
                    (version.value.excludedFields.empty() || version.value.excludedFields[i] == 0)) {
                // Request's payload contains this field, AND
                // base SKVRecord also has value of this field.
                // copy 'base skvRecord' value, at the mean time _advancePosition of 'req' payload.
                bool success = false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], basePayload, payload, success);
                if (!success) return false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], request.value.fieldData, success);
                if (!success) return false;
            } else if (request.value.excludedFields[i] == 0 &&
                    (!version.value.excludedFields.empty() && version.value.excludedFields[i] == 1)) {
                // Request's payload contains this field, AND
                // base SKVRecord do NOT has this field.
                // skip this field, at the mean time _advancePosition of 'req' payload.
                request.value.excludedFields[i] = true;
                bool success;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], request.value.fieldData, success);
                if (!success) return false;
            } else if (request.value.excludedFields[i] == 1 &&
                    (version.value.excludedFields.empty() || version.value.excludedFields[i] == 0)) {
                // Request's payload do NOT contain this field, AND
                // base SKVRecord has value of this field.
                // copy 'base skvRecord' value.
                bool success = false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], basePayload, payload, success);
                if (!success) return false;
                request.value.excludedFields[i] = false;
            } else {
                // else, request payload skipped this field, AND base SKVRecord also skipped this field,
                // set excludedFields[i]
                request.value.excludedFields[i] = true;
            }
        }
    }

    request.value.fieldData = std::move(payload);
    request.value.fieldData.truncateToCurrent();
    return true;
}

bool K23SIPartitionModule::_makeFieldsForDiffVersion(dto::Schema& schema, dto::Schema& baseSchema, dto::K23SIWriteRequest& request, dto::DataRecord& version) {
    std::size_t findField; // find field index of base SKVRecord
    std::vector<uint32_t> fieldsOffset(1); // every fields offset of base SKVRecord
    std::size_t baseCursor = 0; // indicate fieldsOffset cursor

    Payload basePayload = version.value.fieldData.shareAll();   // base payload
    Payload payload(Payload::DefaultAllocator());                     // payload for new record

    // make every fields in schema for new full-record-WI
    for (std::size_t i = 0; i < schema.fields.size(); ++i) {
        findField = -1;
        if (!_isUpdatedField(i, request.fieldsForPartialUpdate)) {
            // if this field is NOT updated, payload value comes from base SKVRecord.
            findField = _findField(baseSchema, schema.fields[i].name, schema.fields[i].type);
            if (findField == (std::size_t)-1) {
                return false; // if do not find any field, Error return
            }

            // Each field's offset whose index is lower than baseCursor is save in the fieldsOffset
            if (findField < baseCursor) {
                if (request.value.excludedFields[i] == false) {
                    bool success;
                    K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], request.value.fieldData, success);
                    if (!success) return false;
                }
                if (version.value.excludedFields.empty() || version.value.excludedFields[findField] == false) {
                    // copy value from base
                    basePayload.seek(fieldsOffset[findField]);
                    bool success = false;
                    K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], basePayload, payload, success);
                    if (!success) return false;
                    request.value.excludedFields[i] = false;
                } else {
                    // set excludedFields==true
                    request.value.excludedFields[i] = true;
                }
            } else {
                // 1. save offsets in 'fieldsOffset' from baseCursor to findField according to base SKVRecord;
                // note: add offset only if excludedField[i]==false.
                // 2. write 'findField' value from base SKVRecord to payload to make full-record-WI;
                // 3. baseCursor = findField + 1;
                for (; baseCursor <= findField; ++baseCursor) {
                    if (version.value.excludedFields.empty() || version.value.excludedFields[baseCursor] == false) {
                        bool success = false;
                        K2_DTO_CAST_APPLY_FIELD_VALUE(_getNextPayloadOffset, baseSchema.fields[baseCursor],
                                                      basePayload, baseCursor, fieldsOffset, success);
                        if (!success) return false;
                    } else {
                        fieldsOffset.push_back(fieldsOffset[baseCursor]);
                    }
                }

                if (request.value.excludedFields[i] == false) {
                    bool success;
                    K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], request.value.fieldData, success);
                    if (!success) return false;
                }
                if (version.value.excludedFields.empty() || version.value.excludedFields[findField] == false) {
                    // copy value from base
                    basePayload.seek(fieldsOffset[findField]);
                    bool success = false;
                    K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], basePayload, payload, success);
                    if (!success) return false;
                    request.value.excludedFields[i] = false;
                } else {
                    // set excludedFields[i]=true
                    request.value.excludedFields[i] = true;
                }

                baseCursor = findField + 1;
            }
        } else {
            // this field is to be updated.
            if (request.value.excludedFields[i] == false) {
                // request's payload has a value.
                // 1. write() value from req's SKVRecorqd to payload
                bool success = false;
                K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], request.value.fieldData, payload, success);
                if (!success) return false;
            } else {
                // request's payload skips this field
                // 1. set excludedField
                request.value.excludedFields[i] = true;
            }
        }
    }

    request.value.fieldData = std::move(payload);
    request.value.fieldData.truncateToCurrent();
    return true;
}

bool K23SIPartitionModule::_parsePartialRecord(dto::K23SIWriteRequest& request, dto::DataRecord& previous) {
    // We already know the schema version exists because it is validated at the begin of handleWrite
    auto schemaIt = _schemas.find(request.key.schemaName);
    auto schemaVer = schemaIt->second.find(request.value.schemaVersion);
    dto::Schema& schema = *(schemaVer->second);

    if (!request.value.excludedFields.size()) {
        request.value.excludedFields = std::vector<bool>(schema.fields.size(), false);
    }

    // read the previouse datarecord
    auto read_pmem_status = _enginePtr->read(previous.valuePmemPtr);
    if (!std::get<0>(read_pmem_status).is2xxOK()){
        K2LOG_E(log::skvsvr,"-------Partition {}  read pmem error :{}",
        _partition, std::get<0>(read_pmem_status).message);
    }
    std::get<1>(read_pmem_status).read(previous.value);

    // based on the latest version to construct the new SKVRecord
    if (request.value.schemaVersion == previous.value.schemaVersion) {
        // quick path --same schema version.
        // make every fields in schema for new SKVRecord
        if(!_makeFieldsForSameVersion(schema, request, previous)) {
            return false;
        }
    } else {
        // slow path --different schema version.
        auto latestSchemaVer = schemaIt->second.find(previous.value.schemaVersion);
        if (latestSchemaVer == schemaIt->second.end()) {
            return false;
        }
        dto::Schema& baseSchema = *(latestSchemaVer->second);

        if (!_makeFieldsForDiffVersion(schema, baseSchema, request, previous)) {
            return false;
        }
    }

    return true;
}

bool K23SIPartitionModule::_makeProjection(dto::SKVRecord::Storage& fullRec, dto::K23SIQueryRequest& request,
        dto::SKVRecord::Storage& projectionRec) {
    auto schemaIt = _schemas.find(request.key.schemaName);
    auto schemaVer = schemaIt->second.find(fullRec.schemaVersion);
    dto::Schema& schema = *(schemaVer->second);
    std::vector<bool> excludedFields(schema.fields.size(), true);   // excludedFields for projection
    Payload projectedPayload(Payload::DefaultAllocator());            // payload for projection

    for (uint32_t i = 0; i < schema.fields.size(); ++i) {
        if (fullRec.excludedFields.size() && fullRec.excludedFields[i]) {
            // A value of NULL in the record is treated the same as if the field doesn't exist in the record
            continue;
        }

        std::vector<k2::String>::iterator fieldIt;
        fieldIt = std::find(request.projection.begin(), request.projection.end(), schema.fields[i].name);
        if (fieldIt == request.projection.end()) {
            // advance base payload
            bool success = false;
            K2_DTO_CAST_APPLY_FIELD_VALUE(_advancePayloadPosition, schema.fields[i], fullRec.fieldData,
                                          success);
            if (!success) {
                fullRec.fieldData.seek(0);
                return false;
            }
            excludedFields[i] = true;
        } else {
            // write field value into payload
            bool success = false;
            K2_DTO_CAST_APPLY_FIELD_VALUE(_copyPayloadBaseToUpdate, schema.fields[i], fullRec.fieldData,
                                          projectedPayload, success);
            if (!success) {
                fullRec.fieldData.seek(0);
                return false;
            }
            excludedFields[i] = false;
        }
    }

    // set cursor(0) of base payload
    fullRec.fieldData.seek(0);

    projectionRec.excludedFields = std::move(excludedFields);
    projectionRec.fieldData = std::move(projectedPayload);
    projectionRec.fieldData.truncateToCurrent();
    projectionRec.schemaVersion = fullRec.schemaVersion;
    return true;
}

template<typename ResponseT>
seastar::future<std::tuple<Status, ResponseT>>
K23SIPartitionModule::_respondAfterFlush(std::tuple<Status, ResponseT>&& resp) {
    K2LOG_D(log::skvsvr, "Awaiting persistence flush before responding");
    return _persistence->flush()
        .then([resp=std::move(resp)] (auto&& flushStatus) mutable {
            if (!flushStatus.is2xxOK()) {
                K2LOG_E(log::skvsvr, "Persistence failed with status {}", flushStatus);
                // TODO gracefully fail to aid in faster recovery.
                seastar::engine().exit(1);
            }

            K2LOG_D(log::skvsvr, "persistence flush succeeded. Sending response to client");
            return seastar::make_ready_future<std::tuple<Status, ResponseT>>(std::move(resp));
        });
}

seastar::future<Status>
K23SIPartitionModule::_designateTRH(dto::K23SI_MTR mtr, dto::Key trhKey) {
    K2LOG_D(log::skvsvr, "designating trh for {}", mtr);
    if (!_validateRetentionWindow(mtr.timestamp) || _startTs.compareCertain(mtr.timestamp) == dto::Timestamp::GT) {
        return seastar::make_ready_future<Status>(dto::K23SIStatus::AbortRequestTooOld("TRH create request is too old"));
    }

    return _txnMgr.createTxn(std::move(mtr), std::move(trhKey));
}

seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
K23SIPartitionModule::handleWrite(dto::K23SIWriteRequest&& request, FastDeadline deadline) {
    // NB: failures in processing a write do not require that we set the TR state to aborted at the TRH. We rely on
    //     the client to do the correct thing and issue an abort on a failure.
    K2LOG_D(log::skvsvr, "Partition: {}, handle write: {}", _partition, request);
    if (request.designateTRH) {
        /*if (!_validateRequestPartition(request)) {
            // tell client their collection partition is gone
            return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in write"), dto::K23SIWriteResponse());
        }*/
        return _designateTRH(request.mtr, request.key)
            .then([this, request=std::move(request), deadline] (auto&& status) mutable {
                if (!status.is2xxOK()) {
                    K2LOG_D(log::skvsvr, "failed creating TR for {}", request.mtr);
                    return RPCResponse(std::move(status), dto::K23SIWriteResponse{});
                }

                K2LOG_D(log::skvsvr, "succeeded creating TR. Processing write for {}", request.mtr);
                return _processWrite(std::move(request), deadline);
            });
    }

    return _processWrite(std::move(request), deadline);
}

seastar::future<std::tuple<Status, dto::K23SIWriteResponse>>
K23SIPartitionModule::_processWrite(dto::K23SIWriteRequest&& request, FastDeadline deadline) {
    K2LOG_D(log::skvsvr, "key {} ,processing write: {}", request.key, request);

#ifdef OUTPUT_ACCESS_PATTERN
    if(request.key.schemaName=="customer" && isRunBenchMark){
        map<dto::Key, int>::iterator result = writeRecordMap.find(request.key);
        if(result != writeRecordMap.end()){
            ofile<<count++<<' '<<'W'<<' '<<result->second<<endl;
        }else{
            map<dto::Key, int>::iterator resultR = readRecordMap.find(request.key);
            if(resultR != readRecordMap.end()){
                ofile<<count++<<' '<<'W'<<' '<<resultR->second<<endl;
            }else{
                writeRecordMap.insert(pair<dto::Key, int>(request.key, keySequence));
                ofile<<count++<<' '<<'W'<<' '<<keySequence<<endl;
                keySequence++;
            }
        }
        //K2LOG_I(log::skvsvr, "######write key: {}", request.key);
    }
#endif

    //int indexFlag = getSchemaArrayIndex(request.key.schemaName);
    //request.value.fieldData.seek(0);
    //totalReadSize[indexFlag] += request.value.fieldData.getSize(); 
    auto startT = k2::Clock::now();
    // 1. Lookup in indexer
    IndexerIterator it = _indexer.find(request.key);
    KeyValueNode* nodePtr = nullptr;
    bool isInsert = it==_indexer.end();
    _writeFindSum += k2::Clock::now() - startT;

    // 2. Find a KVNode or insert one
    if(it == _indexer.end()) {
        K2LOG_D(log::skvsvr, "Insert new KeyValueNode for key:{}", request.key);
        auto idxStartT = k2::Clock::now();
        nodePtr = _indexer.insert(request.key);   
        _insertIndexerSum += k2::Clock::now() - idxStartT;
        ++_totalInsert;     
        K2ASSERT(log::skvsvr, nodePtr!=nullptr, "Insert failed and return nullptr");
    }
    else {
        //K2LOG_D(log::skvsvr, "PartialUpdate and get node from indexer");
        nodePtr = _indexer.extractFromIter(it);
        _partialUpdateIndexerSum += k2::Clock::now()-startT;
        ++_totalPartialUpdate;
        K2ASSERT(log::skvsvr, nodePtr!=nullptr, "Result of extractFrom iter is nullptr");
    }

    // 3. validate write request, check push and duplicated write
    k2::TimePoint validateStartT = k2::Clock::now();
    /*Status validateStatus = _validateWriteRequest(request, *nodePtr);
    K2LOG_D(log::skvsvr, "write for {} validated with status {}", request, validateStatus);
    if (!validateStatus.is2xxOK()) {
        if (nodePtr->begin() == nullptr) {
            // remove the key from indexer if there are no versions in node
            k2::TimePoint deleteStartT = k2::Clock::now();
            _indexer.erase(request.key);
            _deleteIndexerSum += k2::Clock::now() - deleteStartT;
            ++_totalDelete;
        }
        K2LOG_D(log::skvsvr, "rejecting write {} due to {}", request, validateStatus);
        // we may come here after a TRH create. Make sure to flush that
        return RPCResponse(std::move(validateStatus), dto::K23SIWriteResponse{});
    }*/

    // check to see if we should push or is this a write from same txn
    KeyValueNode& KVNode = *nodePtr;
    K2LOG_D(log::skvsvr, "KeyValueNode @{}", (void*)(&KVNode));
    dto::DataRecord* rec = nodePtr->begin();
    // nodePtr->printAll();
    bool isHot = nodePtr->is_inmem(0);
    /*if(isHot){
        rec = static_cast<dto::DataRecord *>(pbrb->getPlogAddrRow(rec));
        isHot = false;
    }*/
    //NodeVerMetadata verMD = nodePtr->getNodeVerMetaData(0, pbrb);
    if (isHot){
        rec = static_cast<dto::DataRecord *>(pbrb->getPlogAddrRow(rec));
    }

    K2LOG_D(log::skvsvr, "status of begin is {}", rec!=nullptr &&rec->status==dto::DataRecord::Committed);
    if (rec != nullptr && rec->status  == dto::DataRecord::WriteIntent && rec->timestamp != request.mtr.timestamp) {
        // this is a write request finding a WI from a different transaction. Do a push with the remaining
        // deadline time.
        K2LOG_D(log::skvsvr, "different WI found for key {}, ol", request.key);
        return _doPush(request.key, rec->timestamp, request.mtr, deadline)
            .then([this, request = std::move(request), deadline](auto&& retryChallenger) mutable {
                if (!retryChallenger.is2xxOK()) {
                    // challenger must fail. Flush in case a TR was created during this call to handle write
                    K2LOG_D(log::skvsvr, "write push challenger lost for key {}", request.key);
                    return RPCResponse(dto::K23SIStatus::AbortConflict("incumbent txn won in write push"), dto::K23SIWriteResponse{});
                }

                K2LOG_D(log::skvsvr, "write push retry for key {}", request.key);
                return _processWrite(std::move(request), deadline);
            });
    }

    // Handle idempotency here. If request ids match, then this was a retry message from the client
    // and we should return OK
    if (rec != nullptr && rec->status == dto::DataRecord::WriteIntent &&
        request.mtr.timestamp == rec->timestamp &&
        request.request_id == rec->request_id) {
        K2LOG_D(log::skvsvr, "duplicate write encountered in request {}", request);
        return RPCResponse(dto::K23SIStatus::Created("wi was already created"), dto::K23SIWriteResponse{});
    }

    // Note that if we are here and a WI exists, it must be from the txn of the current request
    // Exists precondition can be set by the user (e.g. with a delete to know if a record was actually
    // delete) and it is set for partial updates
    if (request.precondition == dto::ExistencePrecondition::Exists && (!rec || rec->isTombstone)) {
        K2LOG_D(log::skvsvr, "Request {} not accepted since Exists precondition failed", request);
        //_readCache->insertInterval(request.key, request.key, request.mtr.timestamp);
        return RPCResponse(dto::K23SIStatus::ConditionFailed("Exists precondition failed"), dto::K23SIWriteResponse{});
    }

    if (request.precondition == dto::ExistencePrecondition::NotExists && rec && !rec->isTombstone) {
        // Need to add to read cache to prevent an erase coming in before this requests timestamp
        // If the condition passes (ie, there was no previous version and the insert succeeds) then
        // we do not need to insert into the read cache because the write intent will handle conflicts
        // and if the transaction aborts then any state it implicitly observes does not matter
        K2LOG_D(log::skvsvr, "write from txn {}, updates read cache for key {}", request.mtr, request.key);
        //_readCache->insertInterval(request.key, request.key, request.mtr.timestamp);

        // The ConditionFailed status does not mean that the transaction must abort. It is up to the user
        // to decide to abort or not, similar to a KeyNotFound status on read.
        return RPCResponse(dto::K23SIStatus::ConditionFailed("Previous record exists"), dto::K23SIWriteResponse{});
    }

    if (request.fieldsForPartialUpdate.size() > 0) {
        // parse the partial record to full record
        if (!_parsePartialRecord(request, *rec)) {
            K2LOG_I(log::skvsvr, "can not parse partial record for key {}", request.key);
            rec->value.fieldData.seek(0);
            //_readCache->insertInterval(request.key, request.key, request.mtr.timestamp);
            return RPCResponse(dto::K23SIStatus::ConditionFailed("missing fields or can not interpret partialUpdate"), dto::K23SIWriteResponse{});
        }
    }
     _writeValidateSum += k2::Clock::now() - validateStartT;

    // all checks passed - we're ready to place this WI as the latest version
    // 4. Create WI
    k2::TimePoint WIStartT = k2::Clock::now();
    auto status = _createWI(std::move(request), KVNode);
    _createWISum += k2::Clock::now() - WIStartT;
    K2LOG_D(log::skvsvr, "KVNode @{} after create WI", (void*)(&KVNode));
    if(isInsert) {
        _insertSum += k2::Clock::now() - startT;
    }
    else {
        _partialUpdateSum += k2::Clock::now() - startT;
    }
    return RPCResponse(std::move(status), dto::K23SIWriteResponse{});
}

Status
K23SIPartitionModule::_createWI(dto::K23SIWriteRequest&& request, KeyValueNode& KVNode) {
    K2LOG_D(log::skvsvr, "Write Request creating WI: {}", request);
    // we need to copy this data into a new memory block so that we don't hold onto and fragment the transport memory
    
    k2::OperationLatencyReporter reporter(_writePmemLogLatency);
    Payload payload(Payload::DefaultAllocator());
    payload.write(request.value);
    payload.seek(0);
    auto pmem_status = _enginePtr->append(payload);
    if( !std::get<0>(pmem_status).is2xxOK()){
        return std::get<0>(pmem_status);
    }
    reporter.report();
    PmemAddress pmemAddr =  std::get<1>(pmem_status);
    //K2LOG_I(log::skvsvr,"Write datarecord to pmemLog, pmemAddr: {}, request.value:{}",pmemAddr, request.value);

    dto::DataRecord *rec = new dto::DataRecord{.value = dto::SKVRecord::Storage{}, .valuePmemPtr = pmemAddr, .isTombstone=request.isDelete, .timestamp=request.mtr.timestamp,
                        .prevVersion=nullptr, .status=dto::DataRecord::WriteIntent, .request_id=request.request_id};
    //KVNode.printAll();
    KVNode.insert_datarecord(rec, pbrb, rec->timestamp, false, -1);
    // TODO: evict old hot version in pbrb!
    KVNode.set_writeintent();
    K2LOG_D(log::skvsvr, "After _createWI:");
    // KVNode.printAll();
    auto status = _twimMgr.addWrite(std::move(request.mtr), std::move(request.key), std::move(request.trh), std::move(request.trhCollection));

    if (!status.is2xxOK()) {
        return status;
    }
    // the TWIM accepted the write. Add it as a WI now  
    _persistence->append(*rec);
    _totalWI++;
    return Statuses::S201_Created("WI created");
}

seastar::future<std::tuple<Status, dto::K23SITxnPushResponse>>
K23SIPartitionModule::handleTxnPush(dto::K23SITxnPushRequest&& request) {
    K2LOG_D(log::skvsvr, "Partition: {}, push request: {}", _partition, request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in push"), dto::K23SITxnPushResponse());
    }
    if (!_validateRetentionWindow(request.challengerMTR.timestamp)) {
        // the request is outside the retention window
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld("request(challenger MTR) too old in push"), dto::K23SITxnPushResponse());
    }

    return _txnMgr.push(std::move(request.incumbentMTR), std::move(request.challengerMTR), std::move(request.key));
}

seastar::future<std::tuple<Status, dto::K23SITxnEndResponse>>
K23SIPartitionModule::handleTxnEnd(dto::K23SITxnEndRequest&& request) {
    K2LOG_D(log::skvsvr, "Partition: {}, transaction end: {}", _partition, request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2LOG_D(log::skvsvr, "transaction end too old for txn={}", request.mtr);
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in end"), dto::K23SITxnEndResponse{});
    }

    return _txnMgr.endTxn(std::move(request));
}

seastar::future<std::tuple<Status, dto::K23SITxnHeartbeatResponse>>
K23SIPartitionModule::handleTxnHeartbeat(dto::K23SITxnHeartbeatRequest&& request) {
    K2LOG_D(log::skvsvr, "Partition: {}, transaction hb: {}", _partition, request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        K2LOG_D(log::skvsvr, "txn hb too old txn={}", request.mtr);
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in hb"), dto::K23SITxnHeartbeatResponse{});
    }
    if (!_validateRetentionWindow(request.mtr.timestamp)) {
        // the request is outside the retention window
        K2LOG_D(log::skvsvr, "txn hb too old txn={}", request.mtr);
        return RPCResponse(dto::K23SIStatus::AbortRequestTooOld("txn too old in hb"), dto::K23SITxnHeartbeatResponse{});
    }
    return _txnMgr.heartbeat(std::move(request.mtr), std::move(request.key))
        .then([](auto&& status) {
            return RPCResponse(std::move(status), dto::K23SITxnHeartbeatResponse{});
        });
}

seastar::future<Status>
K23SIPartitionModule::_doPush(dto::Key key, dto::Timestamp incumbentId, dto::K23SI_MTR challengerMTR, FastDeadline deadline) {
    auto* incumbent = _twimMgr.getTxnWIMeta(incumbentId);
    K2ASSERT(log::skvsvr, incumbent != nullptr, "TWIM does not exists for {} in push for key {}", incumbentId, key)
    K2LOG_D(log::skvsvr, "executing push against txn={}, for mtr={}", *incumbent, challengerMTR);

    dto::K23SITxnPushRequest request{};
    request.collectionName = incumbent->trhCollection;
    request.incumbentMTR = incumbent->mtr;
    request.key = incumbent->trh; // this is the routing key - should be the TRH key
    request.challengerMTR = std::move(challengerMTR);
    return seastar::do_with(std::move(request), std::move(key), [this, deadline, &incumbent] (auto& request, auto& key) {
        auto fut = seastar::make_ready_future<std::tuple<Status, dto::K23SITxnPushResponse>>();
        if (incumbent->isAborted()) {
            fut = fut.then([] (auto&&) {
                return RPCResponse(dto::K23SIStatus::OK("challenger won in push since incumbent was already aborted"),
                              dto::K23SITxnPushResponse{ .incumbentFinalization = dto::EndAction::Abort,
                                                         .allowChallengerRetry = true});
            });
        }
        else if (incumbent->isCommitted()) {
            // Challenger should retry if they are newer than the committed value
            fut = fut.then([] (auto&&) {
                return RPCResponse(dto::K23SIStatus::OK("incumbent won in push since incumbent was already committed"),
                              dto::K23SITxnPushResponse{.incumbentFinalization = dto::EndAction::Commit,
                                                        .allowChallengerRetry = true});
            });
        }
        else {
            // we don't know locally what's going on with this txn. Make a remote call to find out
            fut = fut.then([this, &request, deadline] (auto&&) {
                return _cpo.partitionRequest<dto::K23SITxnPushRequest, dto::K23SITxnPushResponse, dto::Verbs::K23SI_TXN_PUSH>(deadline, request);
            });
        }
        return fut.then([this, &key, &request](auto&& responsePair) {
            auto& [status, response] = responsePair;
            K2LOG_D(log::skvsvr, "Push request completed with status={} and response={}", status, response);
            if (!status.is2xxOK()) {
                K2LOG_E(log::skvsvr, "txn push failed: {}", status);
                return seastar::make_ready_future<Status>(std::move(status));
            }

            // update the write intent if necessary
            IndexerIterator IndexerIt = _indexer.find(key);
            if (IndexerIt == _indexer.end()) {
                return seastar::make_ready_future<Status>(response.allowChallengerRetry ? dto::K23SIStatus::OK : dto::K23SIStatus::AbortConflict);
            }

            KeyValueNode& node = *_indexer.extractFromIter(IndexerIt);
            //dto::DataRecord* rec = node.get_datarecord(request.incumbentMTR.timestamp);
            // updated with pbrb.
            int order;
            dto::DataRecord* rec = node.get_datarecord(request.incumbentMTR.timestamp, order, pbrb);
            //NodeVerMetadata verMD = node.getNodeVerMetaData(order, pbrb);
            if(node.is_inmem(0)){
                rec = static_cast<dto::DataRecord *>(pbrb->getPlogAddrRow(rec));
            }

            if (rec != nullptr && rec->status == dto::DataRecord::WriteIntent &&
                rec->timestamp == request.incumbentMTR.timestamp) {
                switch (response.incumbentFinalization) {
                    case dto::EndAction::None: {
                        break;
                    }
                    case dto::EndAction::Abort: {
                        if (auto status = _twimMgr.abortWrite(request.incumbentMTR.timestamp, key); !status.is2xxOK()) {
                            K2LOG_W(log::skvsvr, "Unable to abort write in {} with local txn metadata due to {}", request.incumbentMTR, status);
                            return seastar::make_ready_future<Status>(std::move(status));
                        }
                        _removeWI(node);
                        node.set_zero_writeintent();
                        _finalizedWI++;
                        break;
                    }
                    case dto::EndAction::Commit: {
                        if (auto status = _twimMgr.commitWrite(request.incumbentMTR.timestamp, key); !status.is2xxOK()) {
                            K2LOG_W(log::skvsvr, "Unable to commit write in {} with local txn metadata due to {}", request.incumbentMTR, status);
                            return seastar::make_ready_future<Status>(std::move(status));
                        }                        
                        _totalCommittedPayload += rec->value.fieldData.getSize();
                        rec->status = dto::DataRecord::Committed;
                        _recordVersions++;
                        _finalizedWI++;
                        node.set_zero_writeintent();
                        break;
                    }
                    default:
                        K2LOG_E(log::skvsvr, "Unable to convert WI state based on txn state: {}, in txn: {}", response.incumbentFinalization, rec->timestamp);
                }
            }

            // signal the caller what to do with the challenger
            return seastar::make_ready_future<Status>(response.allowChallengerRetry ? dto::K23SIStatus::OK : dto::K23SIStatus::AbortConflict);
        });
    });
}

seastar::future<std::tuple<Status, dto::K23SITxnFinalizeResponse>>
K23SIPartitionModule::handleTxnFinalize(dto::K23SITxnFinalizeRequest&& request) {
    // find the version deque for the key
    K2LOG_D(log::skvsvr, "Partition: {}, txn finalize: {}", _partition, request);
    if (!_validateRequestPartition(request)) {
        // tell client their collection partition is gone
        return RPCResponse(dto::K23SIStatus::RefreshCollection("collection refresh needed in finalize"), dto::K23SITxnFinalizeResponse{});
    }

    if (auto status = _twimMgr.endTxn(request.txnTimestamp, request.action); !status.is2xxOK()) {
        K2LOG_W(log::skvsvr, "Unable to end transaction {} with local txn metadata due to {}", request.txnTimestamp, status);
        return RPCResponse(std::move(status), dto::K23SITxnFinalizeResponse{});
    }

    // Put the twim in Finalizing state
    if (auto status=_twimMgr.finalizingWIs(request.txnTimestamp); !status.is2xxOK()) {
        K2LOG_W(log::skvsvr, "Unable to start finalizing in transaction {} with local txn metadata due to {}", request.txnTimestamp, status);
        return RPCResponse(std::move(status), dto::K23SITxnFinalizeResponse{});
    };

    if (auto status = _finalizeTxnWIs(request.txnTimestamp, request.action); !status.is2xxOK()) {
        K2LOG_W(log::skvsvr, "Unable to finalize WIs in transaction {} due to {}", request.txnTimestamp, status);
        return RPCResponse(std::move(status), dto::K23SITxnFinalizeResponse{});
    }

    // Finalize and discard the twim
    if (auto status=_twimMgr.finalizedTxn(request.txnTimestamp); !status.is2xxOK()) {
        K2LOG_W(log::skvsvr, "Unable to complete finalization in transaction {} with local txn metadata due to {}", request.txnTimestamp, status);
        return RPCResponse(std::move(status), dto::K23SITxnFinalizeResponse{});
    };

    return RPCResponse(dto::K23SIStatus::OK("Finalization success"), dto::K23SITxnFinalizeResponse{});
}

Status K23SIPartitionModule::_finalizeTxnWIs(dto::Timestamp txnts, dto::EndAction action) {
    auto* twim = _twimMgr.getTxnWIMeta(txnts);
    if (twim == nullptr) {
        return dto::K23SIStatus::KeyNotFound(fmt::format("Twim not found for txn {}", txnts));
    }
    K2ASSERT(log::skvsvr, twim->isCommitted() || twim->isAborted(), "Twim {} has not ended yet", *twim);
    for (auto& key: twim->writeKeys) {
        IndexerIterator idxIt = _indexer.find(const_cast<dto::Key &>(key));
        K2ASSERT(log::skvsvr, idxIt != _indexer.end(),
                 "TWIM {} has registered WI for key {} but key is not in indexer", *twim, key);

        KeyValueNode& KVNode = *_indexer.extractFromIter(idxIt);
        //dto::DataRecord* WIRec = KVNode.begin();
        //NodeVerMetadata verMD = KVNode.getNodeVerMetaData(0, pbrb);
        dto::DataRecord* WIRec = KVNode.begin();
        if(KVNode.is_inmem(0)){
            WIRec = static_cast<dto::DataRecord *>(pbrb->getPlogAddrRow(WIRec));
        }
        // KVNode.printAll();

        K2ASSERT(log::skvsvr, WIRec!=nullptr&&WIRec->status==dto::DataRecord::WriteIntent,
                 "TWIM {} has registered WI for key{}, but key does not have a WI", *twim, key);
        K2ASSERT(log::skvsvr, WIRec->timestamp == txnts,
                 "TWIM {} has registered WI for key{}, but WI is from different transaction {}",
                 *twim, key, WIRec->timestamp);
        // Map version
        // MapIterator idxItMap = _mapIndexer.find(const_cast<dto::Key &>(key));
        // K2ASSERT(log::skvsvr, idxItMap != _mapIndexer.end(),
        //          "TWIM {} has registered WI for key {} but key is not in indexer", *twim, key);
        // KeyValueNode& KVNodeMap = *_mapIndexer.extractFromIter(idxItMap);
        // dto::DataRecord* WIRecMap = KVNodeMap.begin();        
        // K2ASSERT(log::skvsvr, WIRecMap!=nullptr&&WIRecMap->status==dto::DataRecord::WriteIntent,
        //          "TWIM {} has registered Map WI for key{}, but key does not have a WI", *twim, key);
        // K2ASSERT(log::skvsvr, WIRecMap->timestamp == txnts,
        //          "TWIM {} has registered Map WI for key{}, but WI is from different transaction {}",
        //          *twim, key, WIRecMap->timestamp);

        switch (action) {
            case dto::EndAction::Abort: {
                K2LOG_D(log::skvsvr, "aborting {}, in txn {}", key, *twim);
                _removeWI(KVNode);
                // _removeWI(KVNodeMap);
                KVNode.set_zero_writeintent();
                break;
            }
            case dto::EndAction::Commit: {
                K2LOG_D(log::skvsvr, "committing {}, in txn {}", key, *twim);
                
                _totalCommittedPayload += WIRec->value.fieldData.getSize();
                WIRec->status = dto::DataRecord::Committed;
                // _totalCommittedPayload += WIRecMap->value.fieldData.getSize();
                // WIRecMap->status = dto::DataRecord::Committed;
                _recordVersions++;
                KVNode.set_zero_writeintent();
                break;
            }
            default:
                K2LOG_W(log::skvsvr,
                        "failing finalize due to action mismatch key={}, action={}, twim={}",
                        key, action, *twim);
                return dto::K23SIStatus::OperationNotAllowed("request was not an abort or commit, likely memory corruption");
        }
    }

    _finalizedWI++;
    return dto::K23SIStatus::OK;
}

seastar::future<std::tuple<Status, dto::K23SIPushSchemaResponse>>
K23SIPartitionModule::handlePushSchema(dto::K23SIPushSchemaRequest&& request) {
    K2LOG_D(log::skvsvr, "handlePushSchema for schema: {}", request.schema.name);
    if (_cmeta.name != request.collectionName) {
        return RPCResponse(Statuses::S403_Forbidden("Collection names in partition and request do not match"), dto::K23SIPushSchemaResponse{});
    }

    _schemas[request.schema.name][request.schema.version] = std::make_shared<dto::Schema>(request.schema);

    return RPCResponse(Statuses::S200_OK("push schema success"), dto::K23SIPushSchemaResponse{});
}

// For test and debug purposes, not normal transaction processsing
// Returns all versions+WIs for a particular key
seastar::future<std::tuple<Status, dto::K23SIInspectRecordsResponse>>
K23SIPartitionModule::handleInspectRecords(dto::K23SIInspectRecordsRequest&& request) {
    K2LOG_D(log::skvsvr, "handleInspectRecords for: {}", request.key);

    IndexerIterator it = _indexer.find(request.key);
    if (it == _indexer.end()) {
        return RPCResponse(dto::K23SIStatus::KeyNotFound("Key not found in indexer"), dto::K23SIInspectRecordsResponse{});
    }
    auto& KVNode = *_indexer.extractFromIter(it);
    int size = KVNode.size();

    std::vector<dto::DataRecord> records;
    records.reserve(size + 1);

    dto::DataRecord * rec = nullptr;
    for(int i = 0; i<size; ++i) {
        // use cold record. (TODO: change to hot row later)
        if (KVNode.is_inmem(i))
            rec = static_cast<dto::DataRecord *>(pbrb->getPlogAddrRow(rec));

        if(i < 3) {
            rec = KVNode._getpointer(i);
            K2LOG_D(log::skvsvr, "inspect record {}", rec == nullptr);
        }
        else {
            //rec = rec->prevVersion;
            rec = KVNode._getpointer(2)->prevVersion;
        }
        dto::DataRecord copy {
            .value=rec->value.share(),
            .valuePmemPtr =rec->valuePmemPtr,
            .isTombstone=rec->isTombstone,
            .timestamp=rec->timestamp,
            .prevVersion=rec->prevVersion,
            .status=rec->status,
            .request_id=rec->request_id
        };
        K2LOG_D(log::skvsvr, "copy success with status : {}", copy.status);
        records.push_back(std::move(copy));
    }

    dto::K23SIInspectRecordsResponse response {
        std::move(records)
    };
    return RPCResponse(dto::K23SIStatus::OK("Inspect records success"), std::move(response));
}

// For test and debug purposes, not normal transaction processsing
// Returns the specified TRH
seastar::future<std::tuple<Status, dto::K23SIInspectTxnResponse>>
K23SIPartitionModule::handleInspectTxn(dto::K23SIInspectTxnRequest&& request) {
    K2LOG_D(log::skvsvr, "handleInspectTxn {}", request);
    return _txnMgr.inspectTxn(request.timestamp);
}

// For test and debug purposes, not normal transaction processsing
// Returns all WIs on this node for all keys
seastar::future<std::tuple<Status, dto::K23SIInspectWIsResponse>>
K23SIPartitionModule::handleInspectWIs(dto::K23SIInspectWIsRequest&&) {
    K2LOG_D(log::skvsvr, "handleInspectWIs");
    std::vector<dto::DataRecord> records;

    for (auto it = _indexer.begin(); it != _indexer.end(); ++it) {
        if (_indexer.extractFromIter(it)->begin() == nullptr || _indexer.extractFromIter(it)->begin()->status == dto::DataRecord::Committed) {
            continue;
        }

        //dto::DataRecord* rec = _indexer.extractFromIter(it)->begin();
        KeyValueNode &KVNode = *_indexer.extractFromIter(it);
        dto::DataRecord* rec = KVNode.begin();
        if (KVNode.is_inmem(0))
            rec = static_cast<dto::DataRecord *>(pbrb->getPlogAddrRow(rec));
        // Check whether statuses are same.
        K2ASSERT(log::skvsvr, 
                    (rec->status == dto::DataRecord::WriteIntent) == KVNode.is_writeintent(), 
                    "Rec isWI: {} != KVNode isWI: {}", 
                    (rec->status == dto::DataRecord::WriteIntent),
                    KVNode.is_writeintent());
        // Not WI. Skip
        if (!KVNode.is_writeintent())
            continue;

        dto::DataRecord copy {
                .value=rec->value.share(),
                .valuePmemPtr=rec->valuePmemPtr,
                .isTombstone=rec->isTombstone,
                .timestamp=rec->timestamp,
                .prevVersion=rec->prevVersion,
                .status=rec->status,
                .request_id=rec->request_id
        };
        records.push_back(std::move(copy));
    }

    dto::K23SIInspectWIsResponse response { std::move(records) };
    return RPCResponse(dto::K23SIStatus::OK("Inspect WIs success"), std::move(response));
}

seastar::future<std::tuple<Status, dto::K23SIInspectAllTxnsResponse>>
K23SIPartitionModule::handleInspectAllTxns(dto::K23SIInspectAllTxnsRequest&&) {
    K2LOG_D(log::skvsvr, "handleInspectAllTxns");
    return _txnMgr.inspectTxns();
}

// For test and debug purposes, not normal transaction processsing
// Returns all keys on this node
seastar::future<std::tuple<Status, dto::K23SIInspectAllKeysResponse>>
K23SIPartitionModule::handleInspectAllKeys(dto::K23SIInspectAllKeysRequest&& request) {
    (void) request;
    K2LOG_D(log::skvsvr, "handleInspectAllKeys");
    std::vector<dto::Key> keys;
    keys.reserve(_indexer.size());

    for (auto it = _indexer.begin(); it != _indexer.end(); ++it) {
        keys.push_back(_indexer.extractFromIter(it)->get_key());
    }

    dto::K23SIInspectAllKeysResponse response { std::move(keys) };
    return RPCResponse(dto::K23SIStatus::OK("Inspect AllKeys success"), std::move(response));
}

// For a given challenger timestamp and key, check if a push is needed against a WI
/*bool K23SIPartitionModule::_checkPushForRead(const VersionSet& versions, const dto::Timestamp& timestamp) {
    if (!versions.WI.has_value()) {
        return false;
    }

    // timestamps are unique, so if it is an exact match we know it is the same txn
    // If our timestamp is lower than the WI, we also don't need to push for read
    if (versions.WI->data.timestamp.compareCertain(timestamp) >= 0) {
        return false;
    }

    return true;
}*/

// get the data record with the given key which is not newer than the given timestsamp, or if it
// is an exact match for a write intent (for read your own writes, etc)
/*dto::DataRecord*
K23SIPartitionModule::_getDataRecordForRead(VersionSet& versions, dto::Timestamp& timestamp) {
    if (versions.WI.has_value() && versions.WI->data.timestamp.compareCertain(timestamp) == 0) {
        return &(versions.WI->data);
    } else if (versions.WI.has_value() &&
                timestamp.compareCertain(versions.WI->data.timestamp) > 0) {
        return nullptr;
    }

    auto viter = versions.committed.begin();
    // position the version iterator at the version we are after
    while (viter != versions.committed.end() && timestamp.compareCertain(viter->timestamp) < 0) {
         // skip newer records
        ++viter;
    }

    if (viter == versions.committed.end()) {
        return nullptr;
    }

    return &(*viter);
}*/

// Helper to remove a WI and delete the key from the indexer of there are no committed records
void K23SIPartitionModule::_removeWI(KeyValueNode& node) {
    node.remove_datarecord(0, pbrb); //////
    //TODO check available
    if (node.begin() == nullptr) {
        _indexer.erase(node.get_key());
        K2LOG_D(log::skvsvr, "remove key: {} from indexer", node.get_key());
        return;
    }
}

seastar::future<> K23SIPartitionModule::_recovery() {
    //TODO perform recovery
    K2LOG_D(log::skvsvr, "Partition: {}, recovery", _partition);
    return seastar::make_ready_future();
}

void K23SIPartitionModule::stringFeildUtilization() {
    IndexerIterator itor = _indexer.begin();
    if(itor ==_indexer.end()) return;
    long totalFieldsSize[12] = {0};
    long totalStoreSize[12] = {0};
    long heapFieldNum[12] = {0};
    for (; itor!=_indexer.end(); ++itor) {
        KeyValueNode* nodePtr = _indexer.extractFromIter(itor);
        int indexFlag = getSchemaArrayIndex(nodePtr->get_key().schemaName);
        for (int i = 0; i < 3; i++) {
            if (!nodePtr->is_inmem(i)) continue;
            void* HotRowAddr = static_cast<void *> (nodePtr->_getpointer(i));
            auto pair = pbrb->findRowByAddr(HotRowAddr);
            BufferPage *pagePtr = pair.first;
            RowOffset rowOff = pair.second;
        #ifdef FIXEDFIELD_ROW
            pbrb->countStringFeildUtilization(pagePtr, rowOff, totalFieldsSize[indexFlag], totalStoreSize[indexFlag], heapFieldNum[indexFlag], false);
        #endif

        #ifdef PAYLOAD_ROW
            pbrb->countStringFeildUtilization(pagePtr, rowOff, totalFieldsSize[indexFlag], totalStoreSize[indexFlag], heapFieldNum[indexFlag], true);
        #endif
            //K2LOG_D(log::skvsvr, "##indexFlag:{}, pair:{}", indexFlag, pair.second);
        }
    }

    for (int i=0; i<5; i++) {
        K2LOG_I(log::skvsvr, "##i:{}, totalFieldsSize:{}, totalStoreSize:{}, heapFieldNum:{}", i, totalFieldsSize[i], totalStoreSize[i], heapFieldNum[i]);
    }
}

int K23SIPartitionModule::getSchemaArrayIndex(String SName) {
    int indexFlag = 0;
    if(SName == "item"){
        indexFlag=0;
    }else if(SName == "warehouse"){
        indexFlag=1;
    }else if(SName == "stock"){
        indexFlag=2;
    }else if(SName == "district"){
        indexFlag=3;
    }else if(SName == "customer"){
        indexFlag=4;
    }else if(SName == "history"){
        indexFlag=5;
    }else if(SName == "orderline"){
        indexFlag=6;
    }else if(SName == "neworder"){
        indexFlag=7;
    }else if(SName == "order"){
        indexFlag=8;
    }else if(SName == "idx_customer_name"){
        indexFlag=9;
    }else if(SName == "idx_order_customer"){
        indexFlag=10;
    }else{
        indexFlag=11;
    }
    return indexFlag;
}

void K23SIPartitionModule::doBackgroundPBRBGC(PBRB *pbrb, IndexerT& _indexer, dto::Timestamp& newWaterMark, Duration& retentionPeriod) {
    K2LOG_I(log::pbrb, "####in doBackgroundPBRBGC, _freePageList.size:{}", pbrb->getFreePageList().size());

    if(updateHitRatioT < 50){
        for(int i=0; i< 12; i++) {
            //if(NvmReadNum[i] > 0) hitRatio[i] = (float)pbrbHitNum[i]/(float)(NvmReadNum[i]+pbrbHitNum[i]);
            if(NvmReadNum[i] > 0) hitRatio[i] = (float)(pbrbHitNum[i]-lastHitNum[i])/(float)(NvmReadNum[i]-lastNvmReadNum[i]+pbrbHitNum[i]-lastHitNum[i]);
            K2LOG_I(log::pbrb, "i:{}, hitRatio:{}", i, hitRatio[i]);
            lastHitNum[i] = pbrbHitNum[i];
            lastNvmReadNum[i] = NvmReadNum[i];
        }
        updateHitRatioT++;
    } else {
        for(int i=0; i < 12; i++){
            hitRatio[i] = 1.0;
        }
        updateHitRatioT = 0;
    }
    
    if ((float)pbrb->getFreePageList().size()/(float)pbrb->getMaxPageNumber() >= 0.3) { //0.4, 0.5
        return;
    }
    if(newWaterMark.compareCertain(pbrb->watermark) > 0) pbrb->watermark = newWaterMark;
   
    float maxPageListUsage = 0.0, pageListUsageArray[100];
    uint32_t schemaIDArray[100];
    k2::String schemaNameArray[100];
    int schemaCount = 0;
    float avgPageListUsage = pbrb->getAveragePageListUsage(maxPageListUsage, schemaIDArray, schemaNameArray, pageListUsageArray, schemaCount);

    if(schemaCount==0) return;
    std::map<uint32_t, k2::dto::Timestamp> watermarkMap;
    //std::map<String, k2::dto::Timestamp> watermarkMap;
    //need to do GC when the usage of a page list > 0.8
    for(int i=0; i < schemaCount; i++) {
        int ratio = 100*(pageListUsageArray[i]-0.7)/(1-0.7);
        k2::dto::Timestamp waterMark = newWaterMark + retentionPeriod*ratio/100;
        watermarkMap.insert(pair<uint32_t, k2::dto::Timestamp>(schemaIDArray[i], waterMark));
        //watermarkMap.insert(pair<String, k2::dto::Timestamp>(schemaNameArray[i], waterMark));
        K2LOG_I(log::pbrb, "newWaterMark:{}, schemaID:{}, waterMark:{}, avgPageListUsage:{}", newWaterMark, schemaIDArray[i], waterMark, avgPageListUsage);
    }

    isDonePBRBGC = false;
    //_indexer.begin();
    IndexerIterator itor = _indexer.begin();
    for (; itor!=_indexer.end(); ++itor) {
        KeyValueNode* nodePtr = _indexer.extractFromIter(itor);
        //K2LOG_I(log::pbrb, "######in doBackgroundPBRBGC, schemaName:{}, key:{}", nodePtr->get_key().schemaName, nodePtr->get_key());
        for (int i = 0; i < 3; i++) {
            if (!nodePtr->is_inmem(i)) continue;
            //K2LOG_I(log::pbrb, "######watermark:{}", watermark);
            //void* HotRowAddr = static_cast<void *> (nodePtr->_getpointer(i));
            //dto::Timestamp rowTimestamp = pbrb->getTimestampRow(HotRowAddr);
            //if (pbrb->watermark.compareCertain(rowTimestamp) > 0) {
            uint32_t curSID = pbrb->getSchemaID(nodePtr->get_key().schemaName);
            if(curSID == 0) {
                break;
                K2LOG_I(log::pbrb, "######Do not find schema:{}", nodePtr->get_key().schemaName);
            }
            curSID--;
            std::map<uint32_t, k2::dto::Timestamp>::iterator l_it;
            //std::map<String, k2::dto::Timestamp>::iterator l_it;
            l_it = watermarkMap.find(curSID);
            if(l_it == watermarkMap.end()) {
                break;
                //K2LOG_I(log::pbrb, "######watermark:{}", watermark);
            }
            if (nodePtr->compareTimestamp(i, l_it->second) < 0) {
            //if (nodePtr->compareTimestamp(i, pbrb->watermark) < 0) {
                //K2LOG_I(log::pbrb, "evict row:{}, order:{}", nodePtr->get_key(), i);
                void* HotRowAddr = static_cast<void *> (nodePtr->_getpointer(i));
                auto pair = pbrb->findRowByAddr(HotRowAddr);
                BufferPage *pagePtr = pair.first;
                RowOffset rowOff = pair.second;
                dto::DataRecord *coldAddr = static_cast<dto::DataRecord *> (pbrb->getPlogAddrRow(HotRowAddr));
                //nodePtr->setColdAddr(i, coldAddr);
                nodePtr->insert_datarecord(coldAddr, pbrb, pbrb->watermark, true, i);
                nodePtr->set_inmem(i, 0);
                //nodePtr->printAll();
                //outputHeader(pagePtr);
                //K2LOG_I(log::pbrb, "before removeHotRow, schemaName:{}, getHotRowsNumPage(pagePtr):{}", nodePtr->get_key().schemaName, getHotRowsNumPage(pagePtr));
                pbrb->removeHotRow(pagePtr, rowOff);
                //pbrb->clearRowBitMap(pagePtr, rowOff);
                //K2LOG_I(log::pbrb, "after removeHotRow getHotRowsNumPage(pagePtr):{}", getHotRowsNumPage(pagePtr));
                //TODO: release heap space
            /*#ifdef FIXEDFIELD_ROW
                pbrb->releaseHeapSpace(pagePtr, HotRowAddr);
            #endif

            #ifdef PAYLOAD_ROW
                pbrb->releasePayloadHeapSpace(pagePtr, HotRowAddr);
            #endif
            */
            }
        }
    }
    
   /*
   std::map<SchemaId, BufferPage*> headerPageMap = pbrb->getAllPageHeader();
   K2LOG_I(log::pbrb, "headerPageMap size:{}", headerPageMap.size());
   std::map<SchemaId, BufferPage*>::iterator iter;
   for ( iter = headerPageMap.begin(); iter != headerPageMap.end(); iter++){
            SchemaId sid = iter->first;
            BufferPage *pagePtr = iter->second;
            while (pagePtr) {
                uint32_t maxRowCnt = pbrb->getMaxRowCnt(sid);
                for (uint32_t i = 0; i < maxRowCnt; i++) {
                    void* HotRowAddr = pbrb->getAddrByPageAndOffset(sid, pagePtr, i);
                    dto::Timestamp rowTimestamp = pbrb->getTimestampRow(HotRowAddr);
                    if (pbrb->watermark.compareCertain(rowTimestamp) > 0) {
                        dto::DataRecord *coldAddr = static_cast<dto::DataRecord *> (pbrb->getPlogAddrRow(HotRowAddr));
                        KeyValueNode* nodePtr = static_cast<KeyValueNode *> (pbrb->getKVNodeAddrRow(HotRowAddr));
                        int offsetKVNode = nodePtr->getOffsetKVNode(HotRowAddr);
                        if(offsetKVNode < 0) {
                            K2LOG_I(log::pbrb, "Not match valuepointer in KVNode!");
                        } else {
                            nodePtr->setColdAddr(offsetKVNode, coldAddr);
                            nodePtr->set_inmem(offsetKVNode, 0);
                        }
                        //nodePtr->printAll();
                        //outputHeader(pagePtr);
                        //K2LOG_I(log::pbrb, "before removeHotRow, schemaName:{}, getHotRowsNumPage(pagePtr):{}", nodePtr->get_key().schemaName, getHotRowsNumPage(pagePtr));
                        pbrb->removeHotRow(pagePtr, i);
                        //pbrb->clearRowBitMap(pagePtr, rowOff);
                        //K2LOG_I(log::pbrb, "after removeHotRow getHotRowsNumPage(pagePtr):{}", getHotRowsNumPage(pagePtr));
                        //TODO: release heap space
                    #ifdef FIXEDFIELD_ROW
                        pbrb->releaseHeapSpace(pagePtr, HotRowAddr);
                    #endif

                    #ifdef PAYLOAD_ROW
                        pbrb->releasePayloadHeapSpace(pagePtr, HotRowAddr);
                    #endif

                    }
                }
                //K2LOG_I(log::pbrb, "after setHotRowsNumPage:{}", hotRowNum);
                pagePtr = pbrb->getNextPage(pagePtr);
            }
        }
    */
    //pbrb->updateHotRowNumofPBRB();
    
    float maxPageListUsage1 = 0.0;
    schemaCount=0;
    float avgPageListUsage1 = pbrb->getAveragePageListUsage(maxPageListUsage1, schemaIDArray, schemaNameArray, pageListUsageArray, schemaCount);
    //K2LOG_I(log::pbrb, "####before GC avgPageListUsage:{}, after GC avgPageListUsage:{}, _freePageList:{}", avgPageListUsage, avgPageListUsage1, pbrb->getFreePageList().size());
    K2LOG_I(log::pbrb, "####before GC maxPageListUsage:{}, after GC maxPageListUsage:{}, avgPageListUsage1:{}, _freePageList:{}", maxPageListUsage, maxPageListUsage1, avgPageListUsage1, pbrb->getFreePageList().size());
    //TODO: merge pages that with low usage
#ifdef SPACE_UTILIZATION
    stringFeildUtilization();
#endif
    isDonePBRBGC = true;
}

}  // ns k2
