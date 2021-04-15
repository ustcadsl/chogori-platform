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

#include <k2/transport/PayloadSerialization.h>
#include <seastar/core/sharded.hh>
#include <k2/transport/Payload.h>
#include <k2/transport/Status.h>
#include <k2/common/Common.h>
#include <k2/config/Config.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/transport/BaseTypes.h>
#include <k2/transport/TXEndpoint.h>
#include <k2/persistence/plog_client/PlogClient.h>


namespace k2 {

namespace log {
inline thread_local k2::logging::Logger lgbase("k2::logstream_base");
}

enum LogStreamType : Verb {
    LogStreamTypeHead = 100,
    WAL,
    IndexerSnapshot,
    Aux,
    LogStreamTypeEnd
};


class LogStreamBase;
class LogStream;
class MetadataMgr;

// a base class that provide operations to handle the plogs. it will be used by both log stream and metadata manager
class LogStreamBase{
private:
    // to store the information of each plog id used by the log stream
    struct PlogInfo {
        uint32_t currentOffset;
        bool sealed;
        uint32_t index;
    };

    struct MetadataElement{
        String plogId;
        uint32_t start_offset;
        uint32_t size;
    };
public:
    LogStreamBase();
    ~LogStreamBase();

    // write data to the the current plog, return the latest Plog ID and latest offset
    seastar::future<std::pair<String, uint32_t> > append(Payload payload);

    // write data to the the current plog with the start offset, return the latest Plog ID and latest offset
    seastar::future<std::pair<String, uint32_t> > append(Payload payload, String plogId, uint32_t offset);

    // read the data from the log stream
    seastar::future<Payload > read(String start_plogId, uint32_t start_offset, uint32_t size);

    // reload the _usedPlogIdVector and _usedPlogInfo for replay purpose
    seastar::future<Status> reload(std::vector<dto::MetadataRecord> plogsOfTheStream);

    // obtain the target plog status
    seastar::future<std::tuple<Status, std::tuple<uint32_t, bool>>> get_plog_status(String plogId);
private:
    PlogClient _client;
    // the maximum size of each plog
    constexpr static uint32_t PLOG_MAX_SIZE = 16 * 1024 * 1024;
    // how many redundant plogs it will create in advance 
    constexpr static uint32_t PLOG_POOL_SIZE = 1;
    // the maximun bytes a read command could read
    constexpr statis uint32_t PLOG_MAX_READ_SIZE = 2*1024*1024;

    // whether this log stream base has been created 
    bool _create = false; 

    // The vector to store the redundant plog Id
    std::vector<String> _preallocatedPlogPool;
    // The vector to store the used plog Id
    std::vector<String> _usedPlogIdVector;
    // The map to store the used plog information
    std::unordered_map<String, PlogInfo> _usedPlogInfo;
    // whether the logstream is switching the plog 
    bool _switched;
    std::vector<seastar::promise<>> _switchRequestWaiters;

    // a virtual API that used to persist the Plog Id and sealed offset of each used plog
    virtual seastar::future<Status> _addNewPlog(uint32_t sealed_offset, String new_plogId)=0;

    // when exceed the size limit of current plog, we need to seal the current plog, write the sealed offset to metadata, and write the contents to the new plog
    seastar::future<std::pair<String, uint32_t> > _switchPlogAndAppend(Payload payload);

protected:
     // create a log stream base
    seastar::future<> _preallocatePlogs();

    // init the plog client client
    seastar::future<> _init_plog_client(String cpo_url, String persistenceClusterName);

    // retrive a plog from _preallocatedPlogPool to be ready to serve requests and persist its plogId
    seastar::future<> _activeAndPersistTheFirstPlog();
    
};

// TODO: Test the performance of the Inheritance
class LogStream:public LogStreamBase{
public:
    LogStream();
    ~LogStream();

    // set the name of this log stream and the meta data manager pointer
    seastar::future<> init(Verb name, MetadataMgr* metadataMgr, String cpo_url, String persistenceClusterName, bool reload);
private:
    // the name of this log stream, such as "WAL", "IndexerSnapshot", "Aux", etc
    Verb _name;
    // the pointer to the metadata manager
    MetadataMgr* _metadataMgr;
    virtual seastar::future<Status> _addNewPlog(uint32_t sealed_offset, String new_plogId);
};


class MetadataMgr:public LogStreamBase{

public:
    MetadataMgr();
    ~MetadataMgr();
    
    // set the partition name, init all the log streams this metadata mgr used
    seastar::future<> init(String cpo_url, String partitionName, String persistenceClusterName, bool reload);
    // handle the persistence requests from all the logstreams
    seastar::future<Status> addNewPLogIntoLogStream(Verb name, uint32_t sealed_offset, String new_plogId);
    // return the request logstream to client
    LogStream* obtainLogStream(Verb log_stream_name);
    // replay the entire Metadata Manager
    seastar::future<Status> replay(String cpo_url, String partitionName, String persistenceClusterName);
private:
    // a map to store all the log streams managed by this metadata manager
    std::unordered_map<Verb, LogStream*> _logStreamMap;
    CPOClient _cpo;
    String _partitionName;
    virtual seastar::future<Status> _addNewPlog(uint32_t sealed_offset, String new_plogId);
    ConfigDuration _cpo_timeout {"cpo_timeout", 1s};
};

} // k2