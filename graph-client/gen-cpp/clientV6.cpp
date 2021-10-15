#include<iostream>
#include <unistd.h>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/protocol/TBinaryProtocol.h>


#include "MetaService.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

int main(int argc, char **argv) {

    // ::apache::thrift::stdcxx::shared_ptr<TTransport> socket(new TSocket("localhost", 9090));
    // ::apache::thrift::stdcxx::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    // ::apache::thrift::stdcxx::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

   std::shared_ptr<TTransport> socket(new TSocket("localhost", 9703));
   std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
   std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  
   
    MetaServiceClient client(protocol);




    try{

        transport->open();

       

        std::cout<<"transport open"<<std::endl;
       // client.ping();
       // std::cout << "ping()" << std::endl;

        SpaceDesc testDesc;
        testDesc.space_name="test1";
        CreateSpaceReq testReq;
        testReq.properties = testDesc;
        ExecResp _return;
       
       client.createSpace(_return,testReq);

       std::cout<<"SpaceID: "<<_return.id.space_id<<std::endl;
       std::cout<<"code： "<<_return.code<<std::endl;


//tag
   //  ColumnTypeDef tag_type;
    // tag_type.type = PropertyType::STRING;
     

     //  ColumnDef   tagcol;
    //   tagcol.name = "palyer";
     //  tagcol.type = tag_type ;

     //  SKVGraphSchema tagSchema;

     //  tagSchema.columns.push_back(tagcol);

       CreateTagReq tagReq;
       tagReq.tag_name = "tag1";

     //  tagReq.space_id = _return.id.space_id;
        tagReq.space_id = 1;
     //  tagReq.skv_graph_schema = tagSchema;


    //   ExecResp tag_resp;

    //   client.createTag(tag_resp,tagReq);

    //   std::cout<<"tagID:  "<<tag_resp.id.tag_id<<std::endl;
    //    std::cout<<"code： "<<tag_resp.code<<std::endl;


  
    //    std::cout<<client.add(1,2)<<std::endl;



//space 
        SpaceDesc testDesc2;
       testDesc2.space_name="graphTest";
      
        CreateSpaceReq testReq2;
       testReq2.properties = testDesc2;

        ExecResp _return2;
       
       client.createSpace(_return2,testReq2);

       std::cout<<"SpaceID: "<<_return2.id.space_id<<std::endl;
       std::cout<<"code： "<<_return2.code<<std::endl;
       


     
        
        transport->close();

    } catch (TException& tx) {
        std::cout << "ERROR: " << tx.what() << std::endl;
    }
    return 0;
}
