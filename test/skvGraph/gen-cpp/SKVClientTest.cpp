

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <seastar/core/sleep.hh>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace k2;
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
#include "Log.h"

#include "MetaService.h"

#define UNUSED(x) (void)x

class CreateSpaceHandler
{
public: // application lifespan
    CreateSpaceHandler(CreateSpaceReq req) : _client(K23SIClientConfig())
    {
        K2LOG_I(log::k23si, "ctor");
        _req = req;
    }

    //   CreateSpaceHandler(CreateSpaceReq req) : _client(K23SIClientConfig()){  _req = req;}

    ~CreateSpaceHandler() { K2LOG_I(log::k23si, "dtor"); }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop()
    {
        K2LOG_I(log::k23si, "stop");
        return std::move(_testFuture);
    }

    void start()
    {
        // seastar::future<> start(){

        printf("createSpace\n");

        _testTimer.set_callback([this]
                                {
                                    _testFuture = seastar::make_ready_future()
                                                      .then([this]()
                                                            { return _client.start(); })
                                                      .then([this]
                                                            {
                                                                // auto eps = ConfigVar<std::vector<String>>("tcp_remotes");
                                                                std::vector<String> eps;

                                                                K2LOG_I(log::k23si, "Creating test collection...");
                                                                eps.push_back("tcp+k2rpc://0.0.0.0:10000");
                                                                //rangeEnds
                                                                std::vector<k2::String> rangeEnds;
                                                                rangeEnds.push_back("");

                                                                dto::CollectionMetadata md1{
                                                                    .name = "collname1", //之后改成从请求中读出的名字
                                                                    .hashScheme = dto::HashScheme::HashCRC32C,
                                                                    .storageDriver = dto::StorageDriver::K23SI,
                                                                    .capacity = {},
                                                                    .retentionPeriod = 2h};

                                                                // printf("\n%ld\n", eps().size());
                                                                return _client.makeCollection(std::move(md1), {eps[0]}, std::move(rangeEnds));
                                                            })
                                                      .then([](auto &&status)
                                                            { K2EXPECT(log::k23si, status.is2xxOK(), true); });
                                });
        _testTimer.arm(0ms);
        // return seastar::make_ready_future();
        seastar::make_ready_future();
    }

private:
    seastar::timer<> _testTimer;
    seastar::future<> _testFuture = seastar::make_ready_future();

    K23SIClient _client;
    CreateSpaceReq _req;
};

class MetaServiceHandler : virtual public MetaServiceIf
{
public:
    void createSpace(ExecResp &_return, const CreateSpaceReq &req)
    {
        // Your implementation goes here
        UNUSED(_return);

        App app("CreateSpaceHandler");
        app.addOptions()("tcp_remotes", bpo::value<std::vector<String>>()->multitoken()->default_value(std::vector<String>()), "A list(space-delimited) of endpoints to assign in the test collection")("tso_endpoint", bpo::value<String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")("cpo", bpo::value<String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
        app.addApplet<TSO_ClientLib>();
        app.addApplet<CreateSpaceHandler>(req);

      //  return app.start(argc, **argv);
    }
};

int main(int argc, char argv)
{
  /*
    App app("SKVClientTest");
    app.addOptions()("tcp_remotes", bpo::value<std::vector<String>>()->multitoken()->default_value(std::vector<String>()), "A list(space-delimited) of endpoints to assign in the test collection")("tso_endpoint", bpo::value<String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")("cpo", bpo::value<String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
    app.addApplet<TSO_ClientLib>();
    app.addApplet<CreateSpaceHandler>();
    return app.start(argc, argv);
    */

  int port = 9090;
  ::std::shared_ptr<MetaServiceHandler> handler(new MetaServiceHandler());
  ::std::shared_ptr<TProcessor> processor(new MetaServiceProcessor(handler));
  ::std::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  ::std::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  ::std::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);

  std::cout<<"begin server"<<std::endl;
  server.serve();
  std::cout<<"end server"<<std::endl;

  return 0;
}
