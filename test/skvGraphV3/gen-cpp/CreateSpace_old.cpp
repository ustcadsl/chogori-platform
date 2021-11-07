

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/module/k23si/client/k23si_client.h>

#include <k2/tso/client/tso_clientlib.h>

#include <seastar/core/sleep.hh>
using namespace k2;
#include "Log.h"

#include "MetaService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;


int32_t exitcode;
int count = 0;
CreateSpaceReq __req;

class CreateSpaceHandler {
public:  // application lifespan
    CreateSpaceHandler()noexcept :  _client(K23SIClient(K23SIClientConfig())) { K2LOG_I(log::k23si, "ctor");}
    ~CreateSpaceHandler(){ K2LOG_I(log::k23si, "dtor");}

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::k23si, "\n\n\n\n\n\n\n\n\n\n\n\n\nstop");
        return std::move(_testFuture);
    }
    
    //void start(){
      seastar::future<> start(){
        std::cout <<"createSpace" << std::endl;
        
        _testTimer.set_callback([this] {
            _testFuture = seastar::make_ready_future()
            .then([this] () {
                std::cout << "first"<< std::endl;
                return _client.start();
            })
            .then([this] {
                std::cout << "second"<< std::endl;
                auto eps = ConfigVar<std::vector<String>>("tcp_remotes");
                
                K2LOG_I(log::k23si, "Creating test collection...");

                //rangeEnds
                std::vector<k2::String> rangeEnds;
                rangeEnds.push_back("");
                
                
                dto::CollectionMetadata md{
                    .name = _req.properties.space_name,    //之后改成从请求中读出的名字
                    .hashScheme = dto::HashScheme::HashCRC32C,
                    .storageDriver = dto::StorageDriver::K23SI,
                    .capacity = {},
                    .retentionPeriod = 24h
                };
                
                // printf("\n%ld\n", eps().size());
                return seastar::when_all_succeed(
                    _client.makeCollection(std::move(md), {eps()[0]}, std::move(rangeEnds)),
                    seastar::sleep(10ms)
                );
            })
            .then([](auto&& statuses) {
                auto& [status] = statuses;
                std::cout << "third"<< std::endl;
                if(status.is2xxOK())  exitcode = 0;
                else exitcode = -1;
                std::cout << "forth"<< std::endl;
                std::cout << exitcode<< std::endl;
                // K2EXPECT(log::k23si, status.is2xxOK(), true);
               seastar::engine().exit(exitcode);
            });
        });
        _testTimer.arm(0ms);
      //   return seastar::make_ready_future<>();
       
      
       return seastar::make_ready_future();
       std::cout<<"after return future"<<std::endl;
    }
    private:

    seastar::timer<> _testTimer;
    seastar::future<> _testFuture = seastar::make_ready_future();

    K23SIClient _client;
    CreateSpaceReq _req;
    
};

class MetaServiceHandler : virtual public MetaServiceIf {
 public:
  MetaServiceHandler(int argc, char **argv){
        std::cout<<"here"<<std::endl;
        
  }

  int32_t createSpace(const CreateSpaceReq& req) {
    exitcode = -1;  //默认出问题，因为可能程序无法到达最后
    std::cout << "\n\n" <<_argc <<std::endl;
    __req = req;
    std::cout << __req.properties.space_name << std::endl;
    if(count == 0){
        std::cout << "\n\n__app" << std::endl;
        // __app->start(_argc, _argv);
        _app = new App("main");
        _app->addOptions()
            ("tcp_remotes", bpo::value<std::vector<String>>()->multitoken()->default_value(std::vector<String>()), "A list(space-delimited) of endpoints to assign in the test collection")
            ("tso_endpoint", bpo::value<String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
            ("cpo", bpo::value<String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
        _app->addApplet<TSO_ClientLib>();
        _app->addApplet<CreateSpaceHandler>();
        _app->start(_argc, _argv);
        delete _app;
    }
    else{
        std::cout << "\n\n1_app" << std::endl;
        // __app->start(_argc, _argv);
        __app = new App("main");
        __app->addOptions()
            ("tcp_remotes", bpo::value<std::vector<String>>()->multitoken()->default_value(std::vector<String>()), "A list(space-delimited) of endpoints to assign in the test collection")
            ("tso_endpoint", bpo::value<String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
            ("cpo", bpo::value<String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
        __app->addApplet<TSO_ClientLib>();
        __app->addApplet<CreateSpaceHandler>();
        __app->start(_argc, _argv);
        delete __app;
    }
    count++;
    return exitcode;
  }

/*
  void createTag(ExecResp& _return, const CreateTagReq& req) {
    // Your implementation goes here
    printf("createTag\n");
    
  }
  */

   int32_t add(const int32_t num1, const int32_t num2) {
    // Your implementation goes here

    return num1+num2;
    printf("add\n");
  }
  
  private:
    App* _app;
    App* __app;
    int _argc;
    char **_argv;
   // K23SIClient _client;
};

int main(int argc, char **argv) {
  
  int port = 9090;

    
    App app("main");
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<String>>()->multitoken()->default_value(std::vector<String>()), "A list(space-delimited) of endpoints to assign in the test collection")
        ("tso_endpoint", bpo::value<String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo", bpo::value<String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
    app.addApplet<TSO_ClientLib>();
    app.addApplet<CreateSpaceHandler>();
    App _app("main123");
    _app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<String>>()->multitoken()->default_value(std::vector<String>()), "A list(space-delimited) of endpoints to assign in the test collection")
        ("tso_endpoint", bpo::value<String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo", bpo::value<String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
    _app.addApplet<TSO_ClientLib>();
    _app.addApplet<CreateSpaceHandler>();

  ::std::shared_ptr<MetaServiceHandler> handler(new MetaServiceHandler(argc,argv));
  ::std::shared_ptr<TProcessor> processor(new MetaServiceProcessor(handler));
  ::std::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  ::std::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  ::std::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();


  return 0;
}
