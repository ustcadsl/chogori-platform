//create tag
//tag is a schema;

//test git


//When the user creates a schema, it sends it as a request to the CPO. After validation, 
//the CPO pushes the schema to all k2 storage nodes that own a partition of the collection
//

/*  type in skv
enum class FieldType : uint8_t {
    NULL_T = 0,
    STRING, // NULL characters in string is OK
    INT16T,
    INT32T,
    INT64T,
    FLOAT, // Not supported as key field for now
    DOUBLE,  // Not supported as key field for now
    BOOL,
    DECIMAL64, // Provides 16 decimal digits of precision
    DECIMAL128, // Provides 34 decimal digits of precision
    FIELD_TYPE, // The value refers to one of these types. Used in query filters.
    NOT_KNOWN = 254,
    NULL_LAST = 255
};
*/

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
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
#include <list>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

static std::unordered_map<std::string, int32_t> spaceTable;

static std::unordered_map<std::string, int32_t> tagTable;
static std::unordered_map<std::string, std::vector<std::string>> name2Eps = {
    {"test1", {"tcp+k2rpc://0.0.0.0:10000"}},{"test2", {"tcp+k2rpc://0.0.0.0:10001"}},{"test3", {"tcp+k2rpc://0.0.0.0:10002"}}
};
bool finish = false;

struct MyCollectionCreateRequest
{
    k2::dto::CollectionCreateRequest req;
    // seastar::promise<k2::Status>* prom;
    std::promise<k2::Status> *prom;
    // std::promise<k2::Status>* prom = new std::promise<k2::Status>();
    // K2_DEF_FMT(MyCollectionCreateRequest, req);
};
inline std::queue<MyCollectionCreateRequest> collectionCreateQ;

struct MySchemaCreateRequest
{

    k2::dto::CreateSchemaRequest req;
    std::promise<k2::CreateSchemaResult> *prom; //返回的future 不同
};
inline std::queue<MySchemaCreateRequest> SchemaCreateQ;

class Client
{
public:
    Client()
    {
        K2LOG_I(log::k23si, "Ctor");
        _client = new K23SIClient(K23SIClientConfig());
    }

    ~Client()
    {
        delete _client;
        // delete _txns;
    }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop()
    {
        K2LOG_I(log::k23si, "Stop");
        finish = true;

        _stop = true;

        return std::move(_poller)
            .then([this]
                  { return _client->gracefulStop(); })
            .then([this]
                  {
                      // drain all queue items and fail them due to shutdown
                      return _pollForWork();
                  });
    }

    seastar::future<> start()
    {
        K2LOG_I(log::k23si, "Start");
        // start polling the request queues only on core 0

        std::cout << "\n\n\n  seastar app start \n\n";
        if (seastar::this_shard_id() == 0)
        {
            K2LOG_I(log::k23si, "Poller starting");
            _poller = _poller.then([this]
                                   {
                                       return seastar::do_until(
                                           [this]
                                           {
                                               return _stop;
                                           },
                                           [this]
                                           {
                                               // std::cout << "\n\n\n\nabc\n\n\n\n";
                                               return _pollForWork();
                                           });
                                   });
        }
        return _client->start();
    }

    template <typename Q, typename Func>
    seastar::future<> pollQ(Q &q, Func &&visitor)
    {
        // lock the mutex before manipulating the queue
        // std::unique_lock lock(requestQMutex);

        std::vector<seastar::future<>> futs;
        futs.reserve(q.size());

        while (!q.empty())
        {
            K2LOG_I(log::k23si, "Found Req");
            futs.push_back(
                seastar::do_with(std::move(q.front()), std::forward<Func>(visitor),
                                 // seastar::do_with(MyCollectionCreateRequest(), Func,
                                 [](auto &req, auto &visitor)
                                 {
                                     try
                                     {
                                         // std::cout << "\n\n\n\nline104\n\n\n\n";
                                         return visitor(req)
                                             .handle_exception([&req](auto exc)
                                                               {
                                                                   K2LOG_W_EXC(log::k23si, exc, "caught exception");
                                                                   req.prom->set_exception(exc);
                                                                   //返回异常
                                                               });
                                     }
                                     catch (const std::exception &exc)
                                     {
                                         // K2LOG_W(log::k2ss, "Caught exception during poll of {}: {}", typeid(Q).name(), exc.what());
                                         req.prom->set_exception(std::current_exception());
                                         //返回异常
                                         return seastar::make_ready_future();
                                     }
                                     catch (...)
                                     {
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
    K23SIClient *_client;
    // std::unordered_map<k2::dto::K23SI_MTR, k2::K2TxnHandle>* _txns;

    seastar::future<> _poller = seastar::make_ready_future();
    seastar::future<> _pollForWork()
    {
        return seastar::when_all_succeed(
                   _pollCreateCollectionQ(), _pollSchemaCreateQ())
            .discard_result();
    }

    // seastar::future<> _pollBeginQ();
    // seastar::future<> _pollEndQ();
    // seastar::future<> _pollSchemaGetQ();
    seastar::future<> _pollSchemaCreateQ()
    {
        return pollQ(SchemaCreateQ, [this](auto &req)
                     {
                         std::cout<<"\n\n\n _pollSchemaCreateQ";
                         // std::cout << "\n\n\n\n"<<req.req.clusterEndpoints[0]<<"\n\n\n\n"<< req.req.rangeEnds<<"abc\n\n\n";
                         if (_stop)
                         {
                             return seastar::make_exception_future(std::runtime_error("seastar app has been shutdown"));
                         }
                         std::cout<<"\n\n_client->createSchema \n";
                         std::cout<<req.req.collectionName<<std::endl;

                         return _client->createSchema(std::move(req.req.collectionName), std::move(req.req.schema))
                             .then([this, &req](auto &&result)
                                   {
                                       std::cout<<"\n\nafter _clientcreateSchema";
                                       K2LOG_D(log::k23si, "Schema create received {}", result);
                                       req.prom->set_value(std::move(result));
                                   });
                     });
    }
    // seastar::future<> _pollReadQ();
    // seastar::future<> _pollCreateScanReadQ();
    // seastar::future<> _pollScanReadQ();
    // seastar::future<> _pollWriteQ();
    // seastar::future<> _pollUpdateQ();
    seastar::future<> _pollCreateCollectionQ()
    {
        return pollQ(collectionCreateQ, [this](auto &req)
                     {
                         // std::cout << "\n\n\n\n"<<req.req.clusterEndpoints[0]<<"\n\n\n\n"<< req.req.rangeEnds<<"abc\n\n\n";
                         if (_stop)
                         {
                             return seastar::make_exception_future(std::runtime_error("seastar app has been shutdown"));
                         }
                         return _client->makeCollection(std::move(req.req.metadata), {req.req.clusterEndpoints[0]},
                                                        std::move(req.req.rangeEnds))
                             .then([this, &req](auto &&result)
                                   {
                                       K2LOG_D(log::k23si, "Collection create received {}", result);
                                       // std::cout << "\n\n\n\nline164\n\n\n\n";
                                       std::cout << "\n\n cliet_makecollection \n\n"
                                                 << std::endl;
                                       req.prom->set_value(std::move(result));
                                   });
                     });
    }
    // seastar::future<> _pollDropCollectionQ();

    bool _stop = false;
};

void start(int argc, char **argv)
{
    App app("main");
    app.addOptions()("tcp_remotes", bpo::value<std::vector<String>>()->multitoken()->default_value(std::vector<String>()), "A list(space-delimited) of endpoints to assign in the test collection")("tso_endpoint", bpo::value<String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")("cpo", bpo::value<String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
    app.addApplet<TSO_ClientLib>();
    app.addApplet<Client>(); //和k2服务器交互的客户端
    app.start(argc, argv);
}

template <typename Q, typename Request>
void pushQ(Q &queue, Request &&r)
{
    // std::lock_guard lock{requestQMutex};
    if (finish)
    {
        r.prom->set_exception(std::make_exception_ptr(std::runtime_error("queue processing has been shutdown")));
        //返回错误信息
    }
    else
    {
        queue.push(r);
    }
    // queue.push(std::forward<Request>(r));
    // queue.push(r);
}

class MetaServiceHandler : virtual public MetaServiceIf
{
public:
    MetaServiceHandler() {}

    void createSpace(ExecResp &_return, const CreateSpaceReq &req)
    {
        //在这里将req包装成一个新的struct类，其中包含CreateSpaceReq，和一个promise，去用来接收返回的结果，来判断是否创建是否正常结束。

        //TO DO: 修改eps和rangeEnds
        // auto eps = ConfigVar<std::vector<String>>("tcp_remotes");
        // std::cout << "\n\n\n\nline212\n\n\n\n";

        bool isRepeated = false;
        int32_t spaceID;
        std::vector<k2::String> endpoints;
        std::vector<std::string> stdEndpoints = name2Eps[req.properties.space_name];
        for (const std::string& ep : stdEndpoints) {
            endpoints.emplace_back(ep);
        }
        
        //rangeEnds
        std::vector<k2::String> rangeEnds;
        rangeEnds.push_back("");

        //判断name是否出现过
        if(spaceTable.find(req.properties.space_name) != spaceTable.end())
            { //出现过
                std::cout << "space alerady exist!";
                isRepeated = true;
                _return.code = ErrorCode::E_EXISTED;
                _return.id.space_id = spaceTable[req.properties.space_name];
              
                _return.id.__set_space_id( _return.id.space_id);
                return;
            }

        if (!isRepeated)
        {
            spaceID = spaceTable.size();
            std::cout << "\n\n " << "spaceTable.size() "<< spaceTable.size()<< std::endl;
            spaceID++;
            std::cout << "\n\n\n"<< "spaceID" << spaceID << std::endl;
            spaceTable[req.properties.space_name] = spaceID;
        }

        MyCollectionCreateRequest request{
            .req = k2::dto::CollectionCreateRequest{
                .metadata{
                    .name = std::to_string(spaceID),
                    .hashScheme = dto::HashScheme::HashCRC32C,
                    .storageDriver = k2::dto::StorageDriver::K23SI,
                    .capacity{},
                    .retentionPeriod = 24h},
                .clusterEndpoints = std::move(endpoints),
                .rangeEnds = std::move(rangeEnds)},
            .prom = new std::promise<k2::Status>()};

        // pushQ(collectionCreateQ, std::move(request));

        pushQ(collectionCreateQ, request);

        std::cout << "\n\n\n"<< "pushQ" << std::endl;

        try
        { //future.get()时可能抛出异常
            // std::cout << "\n\n\n\nline240\n\n\n\n";
            auto result = request.prom->get_future();
            auto status = result.get();

            if (!status.is2xxOK())
            {
                K2LOG_I(log::k23si, "fail to create a collection");
                // return -2;
                std::cout << status << std::endl;
                _return.code = ErrorCode::E_RPC_FAILURE;
                _return.id.space_id = -1;
               
                _return.id.__set_space_id( _return.id.space_id);
                return;
            }
            else
            {
                _return.code = ErrorCode::SUCCEEDED;
                _return.id.space_id = spaceTable[req.properties.space_name];
                std::cout << "\n\n _return.id.space_id  " << _return.id.space_id << std::endl;
             
              _return.id.__set_space_id( _return.id.space_id);



                return;
            }
            std::cout << "\n\n\n\nline242\n\n\n\n";
        }
        catch (...)
        {
            _return.code = ErrorCode::E_UNKNOWN;
            return;
        }
        return;
    }
    void addVertices(ExecResponse& _return, const AddVerticesRequest& req){

    }
    int32_t add(const int32_t num1, const int32_t num2)
    {
        // Your implementation goes here
        return num1 + num2;
        printf("add\n");
    }

    //tag
    void createTag(ExecResp &_return, const CreateTagReq &req)
    {

        std::cout << "start create tag\n";

        bool isRepeated = false;

        int32_t spaceID = req.space_id;
        int32_t tagID;

        if (tagTable.find(req.tag_name) != tagTable.end())
        { //出现过
            std::cout << "tag alerady exist!";
            isRepeated = true;
            _return.code = ErrorCode::E_EXISTED;
            _return.id.tag_id = tagTable[req.tag_name];
            _return.id.__set_tag_id (_return.id.tag_id);
            //返回错误信息。
            return;
        }

        if (!isRepeated)
        {
            tagID = tagTable.size();
            tagID++;
            tagTable[req.tag_name] = tagID;
            std::cout<<"\n\n req.tag_name:  "<<req.tag_name<<std::endl;
            std::cout<<"\n\n tagID:  "<<tagID<<std::endl;
        }

        k2::dto::Schema TagSchema;
        // std::string collname=_req.space_name;
        // schema.name = _req.tag_name;
        TagSchema.name = std::to_string(tagID);
        TagSchema.version = 1;


        std::cout<<"\n\n\n TagSchema";

        TagSchema.fields = std::vector<dto::SchemaField>{
            {dto::FieldType::INT16T, "Type", false, false},
            {dto::FieldType::INT16T, "PartID", false, false},
            {dto::FieldType::INT64T, "VertexID", false, false},
            {dto::FieldType::INT32T, "TagID", false, false}};

        std::cout<<"TagSchema.fields.size"<<TagSchema.fields.size() <<std::endl;

        std::cout<<"\n\n\n before TagKey";

       

        TagSchema.setPartitionKeyFieldsByName(std::vector<String>{"Type", "PartID"});
        TagSchema.setRangeKeyFieldsByName(std::vector<String>{"VertexID", "TagID"});

        std::cout<<"\n\n\n TagKey";
        std::cout<<"\n\n\n TagKey";

        //columns is a vector of struct define the schema and type: eg age int; name string;
        if (!req.skv_graph_schema.columns.empty())
        {
            std::vector<k2::dto::SchemaField> GraphSchema;
            std::cout<<"\n\n\n GraphSchema";
            for (long unsigned int i = 0; i < req.skv_graph_schema.columns.size(); i++)
            {
                std::cout<<"\n\n i：  "<< i;
                GraphSchema[i].type = static_cast<k2::dto::FieldType>(req.skv_graph_schema.columns[i].type.type);
                GraphSchema[i].name = req.skv_graph_schema.columns[i].name;
                GraphSchema[i].descending = false;
                GraphSchema[i].nullLast = false;
            }
            TagSchema.fields.insert(TagSchema.fields.end(), GraphSchema.begin(), GraphSchema.end());
        }

         std::cout<<"\n\n\n Schemafield";

        MySchemaCreateRequest request{
            .req = k2::dto::CreateSchemaRequest{
                .collectionName = std::to_string(spaceID),
                .schema = std::move(TagSchema)},
            .prom = new std::promise<k2::CreateSchemaResult>()};

         std::cout<<"\n\n\n Request";

        /*        MySchemaCreateRequest request = {
            .req = k2::dto::CreateSchemaRequest{
                .collectionName = std::to_string(spaceID), //spaceID                       
                .schema.name = std::to_string(TagID),
                .schema.version = 1,
                .schema.fields = std::vector<dto::SchemaField>{
                    {dto::FieldType::INT16T, "Type", false, false},
                    {dto::FieldType::INT16T, "PartID", false, false},
                    {dto::FieldType::INT64T, "VertexID", false, false},
                    {dto::FieldType::INT32T, "TagID", false, false}
                    };

          //client 自定义的schema 字段，value 段；
            .schema.fields.insert(schema.fields.end(),GraphSchema.begin(),GraphSchema.end()) ,

            .schema.setPartitionKeyFieldsByName(std::vector<String>{"Type", "PartID"});
            .schema.setRangeKeyFieldsByName(std::vector<String>{"VertexID", "TagID"});
          },
        .prom = new std::promise<k2::Status>() };
*/
        pushQ(SchemaCreateQ, request);

        std::cout <<"\n\n pushQ create Tag"<<std::endl;

        try
        {
            auto result = request.prom->get_future();
            //auto status = result.get();
            auto CreateSchemaResult = result.get();
            auto status = CreateSchemaResult.status;
            if (!status.is2xxOK())
            {
                K2LOG_I(log::k23si, "fail to create a tag");
                // return -2;
                _return.code = ErrorCode::E_RPC_FAILURE;
               
                _return.id.__set_tag_id (-1);
                return;
            }
            else
            {
                _return.code = ErrorCode::SUCCEEDED;
                _return.id.tag_id = tagTable[req.tag_name];
                _return.id.__set_tag_id( _return.id.tag_id);
                return;
            }
        }
        catch (...)
        {
            _return.code = ErrorCode::E_UNKNOWN;
            return;
        }
        return;
    } //end of create Tag

}; // end of MetaServiceHandler

int main(int argc, char **argv)
{
    //int port = 9090;
     int port = 9703;
    std::thread t(start, argc, argv);
    // t.join();
    // seastar::thread th([argc, argv]{
    //     std::cout << "\n\n\n\nhere\n\n\n\n";
    //     start(argc, argv);
    //     std::cout << "\n\n\n\n1234\n\n\n\n";
    // });
    ::std::shared_ptr<MetaServiceHandler> handler(new MetaServiceHandler());
    ::std::shared_ptr<TProcessor> processor(new MetaServiceProcessor(handler));
    ::std::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    ::std::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    ::std::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();

    return 0;
}