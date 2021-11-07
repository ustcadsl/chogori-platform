#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <seastar/core/sleep.hh>
// #include <seastar/core/thread.hh>
using namespace k2;
#include "Log.h"


#include "MetaService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <queue>
#include <future>
#include <tuple>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;




bool finish;


struct MyCollectionCreateRequest {
    k2::dto::CollectionCreateRequest req;
    // seastar::promise<k2::Status> prom;  
    // std::promise<k2::Status> prom;
    std::promise<k2::Status>* prom = new std::promise<k2::Status>();
    // K2_DEF_FMT(MyCollectionCreateRequest, req);
};
inline std::queue<MyCollectionCreateRequest> collectionCreateQ;

class Client {
public:
    Client() {
        K2LOG_I(log::k23si, "Ctor");
        _client = new K23SIClient(K23SIClientConfig());
    }

    ~Client() {
        delete _client;
        // delete _txns;
    }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::k23si, "Stop");
        finish = true;

        _stop = true;

        return std::move(_poller)
            .then([this] {
                return _client->gracefulStop();
            })
            .then([this] {
                // drain all queue items and fail them due to shutdown
                return _pollForWork();
            });
    }

    seastar::future<> start() {
        K2LOG_I(log::k23si, "Start");
        // start polling the request queues only on core 0
        if (seastar::this_shard_id() == 0) {
            K2LOG_I(log::k23si, "Poller starting");
            _poller = _poller.then([this] {
                return seastar::do_until(
                    [this] {
                        return _stop;
                    },
                    [this] {
                        return _pollForWork();
                    }
                );
            });
        }
        return _client->start();
    }


    template <typename Q, typename Func>
    seastar::future<> pollQ(Q& q, Func&& visitor) {
        // lock the mutex before manipulating the queue
        // std::unique_lock lock(requestQMutex);

        std::vector<seastar::future<>> futs;
        futs.reserve(q.size());

        while (!q.empty()) {
            K2LOG_I(log::k23si, "Found Req");
            futs.push_back(
                seastar::do_with(std::move(q.front()), std::forward<Func>(visitor),
                [] (auto& req, auto& visitor) {
                    try {
                        return visitor(req)
                            .handle_exception([&req](auto exc) {
                                K2LOG_W_EXC(log::k23si, exc, "caught exception");
                                req.prom->set_exception(exc);
                                //返回异常
                            });
                    }
                    catch (const std::exception& exc) {
                        // K2LOG_W(log::k2ss, "Caught exception during poll of {}: {}", typeid(Q).name(), exc.what());
                        req.prom->set_exception(std::current_exception());
                        //返回异常
                        return seastar::make_ready_future();
                    }
                    catch (...) {
                        // K2LOG_W(log::k2ss, "Caught unknown exception during poll of {}", typeid(Q).name());
                        req.prom->set_exception(std::current_exception());
                        //返回异常
                        return seastar::make_ready_future();
                    }
                }));
            q.pop();
        }
        return seastar::when_all_succeed(futs.begin(), futs.end());
    }


    


private:
    K23SIClient* _client;
    // std::unordered_map<k2::dto::K23SI_MTR, k2::K2TxnHandle>* _txns;

    seastar::future<> _poller = seastar::make_ready_future();
    seastar::future<> _pollForWork() {
        return seastar::when_all_succeed(
            _pollCreateCollectionQ()
        )
            .discard_result();  // TODO: collection creation is rare, maybe consider some optimization later on to pull on demand only.
    }

    // seastar::future<> _pollBeginQ();
    // seastar::future<> _pollEndQ();
    // seastar::future<> _pollSchemaGetQ();
    // seastar::future<> _pollSchemaCreateQ();
    // seastar::future<> _pollReadQ();
    // seastar::future<> _pollCreateScanReadQ();
    // seastar::future<> _pollScanReadQ();
    // seastar::future<> _pollWriteQ();
    // seastar::future<> _pollUpdateQ();
    seastar::future<> _pollCreateCollectionQ() {
        return pollQ(collectionCreateQ, [this](auto& req) {
            if (_stop) {
                return seastar::make_exception_future(std::runtime_error("seastar app has been shutdown"));
            }
            return _client->makeCollection(std::move(req.req.metadata), {req.req.clusterEndpoints[0]},
                                        std::move(req.req.rangeEnds))
                .then([this, &req](auto&& result) {
                    K2LOG_D(log::k23si, "Collection create received {}", result);
                    req.prom->set_value(std::move(result));
                });
        });
    }
    // seastar::future<> _pollDropCollectionQ();

    bool _stop = false;
};


void start(int argc,char** argv){
    App app("main");
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<String>>()->multitoken()->default_value(std::vector<String>()), "A list(space-delimited) of endpoints to assign in the test collection")
        ("tso_endpoint", bpo::value<String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo", bpo::value<String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
    app.addApplet<TSO_ClientLib>();
    app.addApplet<Client>();    //和k2服务器交互的客户端
    app.start(argc,argv);
}


template <typename Q, typename Request>
void pushQ(Q& queue, Request&& r) {
    // std::lock_guard lock{requestQMutex};
    if (finish) {
        r.prom->set_exception(std::make_exception_ptr(std::runtime_error("queue processing has been shutdown")));
        //返回错误信息
    }
    else {
        queue.push(r);
    }
    // queue.push(std::forward<Request>(r));
    // queue.push(r);
}





class MetaServiceHandler : virtual public MetaServiceIf {
public:
    MetaServiceHandler(){}

    int32_t createSpace(const CreateSpaceReq& req) {
        //在这里将req包装成一个新的struct类，其中包含CreateSpaceReq，和一个promise，去用来接收返回的结果，来判断是否创建是否正常结束。

        //TO DO: 修改eps和rangeEnds
        // auto eps = ConfigVar<std::vector<String>>("tcp_remotes");
        std::vector<k2::String> endpoints;
        endpoints.push_back("tcp+k2rpc://0.0.0.0:10000");
        //rangeEnds
        std::vector<k2::String> rangeEnds;
        rangeEnds.push_back("");

        MyCollectionCreateRequest request{
            .req = k2::dto::CollectionCreateRequest{
                .metadata{
                    .name = req.properties.space_name,
                    .hashScheme = dto::HashScheme::HashCRC32C,
                    .storageDriver = k2::dto::StorageDriver::K23SI,
                    .capacity{ },
                    .retentionPeriod = 24h
                },
                .clusterEndpoints = std::move(endpoints),
                .rangeEnds = std::move(rangeEnds)
            },
            .prom = {}
        };
        auto future = request.prom->get_future();
        pushQ(collectionCreateQ, std::move(request));
        try {   //future.get()时可能抛出异常
            auto res = future.get();    //返回一个tuple
            // auto status = std::get<0>(res);
            if(!res.is2xxOK()){
                K2LOG_I(log::k23si, "fail to create a collection");
                return -2;
            }
            else 
                return 0;
        }
        catch(...) {
            return -1;
        }
        
    }

    int32_t add(const int32_t num1, const int32_t num2) {
        // Your implementation goes here
        return num1+num2;
        printf("add\n");
    }
};


int main(int argc, char **argv) {
    int port = 9090;
    // std::thread t(start,argc,argv);
    // t.join();
    seastar::thread th([argc, argv]{
        start(argc, argv);
    });
    ::std::shared_ptr<MetaServiceHandler> handler(new MetaServiceHandler());
    ::std::shared_ptr<TProcessor> processor(new MetaServiceProcessor(handler));
    ::std::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    ::std::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    ::std::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();


    return 0;
}