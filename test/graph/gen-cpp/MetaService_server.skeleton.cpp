// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "MetaService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

class MetaServiceHandler : virtual public MetaServiceIf {
 public:
  MetaServiceHandler() {
    // Your initialization goes here
  }

  void createSpace(ExecResp& _return, const CreateSpaceReq& req) {
    // Your implementation goes here
    printf("createSpace\n");
  }

  int32_t add(const int32_t num1, const int32_t num2) {
    // Your implementation goes here
    printf("add\n");
  }

  void createTag(ExecResp& _return, const CreateTagReq& req) {
    // Your implementation goes here
    printf("createTag\n");
  }

  void addVertices(ExecResponse& _return, const AddVerticesRequest& req) {
    // Your implementation goes here
    printf("addVertices\n");
  }

};

int main(int argc, char **argv) {
  int port = 9090;
  ::std::shared_ptr<MetaServiceHandler> handler(new MetaServiceHandler());
  ::std::shared_ptr<TProcessor> processor(new MetaServiceProcessor(handler));
  ::std::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  ::std::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  ::std::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}

