
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



#include "MetaService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;



#include <future>
#include <tuple>
#include <list>
#include "k2_includes.h"
#include "k2_client.h"
#include "queue_defs.h"
using namespace k2;
#include "Log.h"

static std::unordered_map<std::string, int32_t> spaceTable;

static std::unordered_map<std::string, int32_t> tagTable;
static std::unordered_map<std::string, std::vector<std::string>> name2Eps = {
    {"test1", {"tcp+k2rpc://0.0.0.0:10000"}},{"test2", {"tcp+k2rpc://0.0.0.0:10001"}},{"test3", {"tcp+k2rpc://0.0.0.0:10002"}}
};


void start(int argc, char **argv) {
    App app("main");
    app.addOptions()("tcp_remotes", bpo::value<std::vector<String>>()->multitoken()->default_value(std::vector<String>()), "A list(space-delimited) of endpoints to assign in the test collection")("tso_endpoint", bpo::value<String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")("cpo", bpo::value<String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");
    app.addApplet<TSO_ClientLib>();
    app.addApplet<k2pg::gate::PGK2Client>(); //和k2服务器交互的客户端
    app.start(argc, argv);
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
                    .retentionPeriod = 24h
                },
                .clusterEndpoints = std::move(endpoints),
                .rangeEnds = std::move(rangeEnds)
            },
            .prom = new std::promise<k2::Status>()
        };

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
    // void addVertices(ExecResponse& _return, const AddVerticesRequest& req){
    //     //检查传入的数据是否合法，并构造相应的skvrecord
    //     //因为需要获得对应schema的信息
    //     std::vector<k2::GetSchemaResult> results;
    //     for(auto iter = req.prop_names.begin(); iter != req.prop_names.end(); iter++){
    //         MySchemaGetRequest request{
    //             .collectionName = std::to_string(req.space_id),
    //             .schemaName = std::to_string(iter -> first),
    //         };
    //     }
        
    //     pushQ(SchemaGetQ, request);
    //     int index = 0;  //记录处理到Schema中SchemaField的下标
        
    // }

    void addVertices(ExecResponse& _return, const AddVerticesRequest& req){
        //因为只支持结点的必要属性，可忽略请求中 prop_names字段
        //假设NewVertex中值均按照schema的顺序排列，只需按顺序序列化字段即可，无需获得schema比对字段
        //只需遍历parts中所有的NewVertex即可
        //在内部做一个从tagid到schema的映射，以防多个相同tag的结点都从数据库中请求相同的schema
        std::unordered_map<int32_t, std::shared_ptr<k2::dto::Schema>> table;
        std::vector<MyWriteRequest> request_list;


        //开始一个事务
        k2::K2TxnOptions options{};
        options.syncFinalize = true;
        MyBeginTxnRequest qr{.opts = options, .prom = new std::promise<k2::dto::K23SI_MTR>(), .startTime = k2::Clock::now()};
        pushQ(BeginTxnQ, std::move(qr));
        k2::dto::K23SI_MTR mtr;
        try {
            auto result = qr.prom->get_future();
            mtr = result.get();
        }
        catch (...) {
            _return.code = ErrorCode::E_UNKNOWN;
            return;
        }
        for(auto iter = req.parts.begin(); iter != req.parts.end(); iter++){
            for(auto item = iter -> second.begin(); item != iter -> second.end(); item++){
                //item是单独的一个NewVertex
                for(auto tag_iter = item -> tags.begin(); tag_iter != item -> tags.end(); tag_iter++){
                    //tag_iter对应一个NewTag
                    std::shared_ptr<k2::dto::Schema> s;
                    if(table.find(tag_iter -> tag_id) != table.end()){
                        s = table[tag_iter -> tag_id];
                    }
                    else{
                        MySchemaGetRequest request{
                            .collectionName = std::to_string(req.space_id),
                            .schemaName = std::to_string(tag_iter -> tag_id),
                            .schemaVersion = 1, //目前所有schema的version均为1，之后可能需要进一步修改
                            .prom = new std::promise<k2::GetSchemaResult>()
                        };
                        pushQ(SchemaGetQ, request);
                        try{
                            auto result = request.prom->get_future();
                            auto schemaResult = result.get();
                            auto status = schemaResult.status;
                            if (!status.is2xxOK()){
                                //获取schema时出错
                                _return.code = ErrorCode::E_NOT_FOUND;
                                return;
                            }
                            s = schemaResult.schema;
                            table[tag_iter -> tag_id] = s;
                        }
                        catch (...){
                            _return.code = ErrorCode::E_UNKNOWN;
                            return;
                        }
                    }
                    k2::dto::SKVRecord skvRecord(std::to_string(req.space_id), s);
                    /*
                    {dto::FieldType::INT16T, "Type", false, false},
                    {dto::FieldType::INT16T, "PartID", false, false},
                    {dto::FieldType::INT64T, "VertexID", false, false},
                    {dto::FieldType::INT32T, "TagID", false, false}
                    必要属性如上所示，只需依次序列化即可
                    */
                    try{
                        skvRecord.serializeNext<int16_t>(tag_iter -> props[0].iVal);
                        skvRecord.serializeNext<int16_t>(tag_iter -> props[1].iVal);
                        skvRecord.serializeNext<int64_t>(tag_iter -> props[2].iVal);
                        skvRecord.serializeNext<int32_t>(tag_iter -> props[3].iVal);
                    }
                    catch (...){
                        _return.code = ErrorCode::E_UNKNOWN;
                        return;
                    }
                    MyWriteRequest write_request {
                        .mtr = mtr,
                        .record = std::move(skvRecord),
                        .prom = new std::promise<k2::WriteResult>()
                    };
                    request_list.push_back(std::move(write_request));
                    // pushQ(WriteRequestQ, write_request);     //为防止由于出错，只有部分结点被加入，在遍历完结点后pushQ
                }
            }
        }
        //已经构建了所有结点的增加请求
        for(auto iter = request_list.begin(); iter != request_list.end(); iter++){
            pushQ(WriteRequestQ, std::move(*(iter))); 
        }
        bool isSucceed = true;
        for(auto iter = request_list.begin(); iter != request_list.end(); iter++){
            try{
                auto result = iter -> prom->get_future();
                auto WriteResult = result.get();
                auto status = WriteResult.status;
                if (!status.is2xxOK()){
                    isSucceed = false;
                    break;
                }
            }
            catch (...){
                _return.code = ErrorCode::E_UNKNOWN;
                return;
            }
        }
        if(isSucceed){
            _return.code = ErrorCode::SUCCEEDED;
            //正常结束事务
            // struct MyEndTxnRequest {
            //     k2::dto::K23SI_MTR mtr;
            //     bool shouldCommit;
            //     std::promise<k2::EndResult> *prom;
            // };
            MyEndTxnRequest end_txn_req {
                .mtr = mtr,
                .shouldCommit = true,
                .prom = new std::promise<k2::EndResult>()
            };
            pushQ(EndTxnQ, end_txn_req);
            try{
                auto result = end_txn_req.prom->get_future();
                auto EndResult = result.get();
                auto status = EndResult.status;
                if (!status.is2xxOK()){
                    _return.code = ErrorCode::E_UNKNOWN;
                }
                return;
            }
            catch (...){
                _return.code = ErrorCode::E_UNKNOWN;
                return;
            }
        }
        else{
            //结束事务，不提交,需细化errorcode
            _return.code = ErrorCode::E_UNKNOWN;
            MyEndTxnRequest end_txn_req {
                .mtr = mtr,
                .shouldCommit = false,
                .prom = new std::promise<k2::EndResult>()
            };
            pushQ(EndTxnQ, end_txn_req);
            try{
                auto result = end_txn_req.prom->get_future();
                auto EndResult = result.get();
                auto status = EndResult.status;
                if (!status.is2xxOK()){
                    _return.code = ErrorCode::E_UNKNOWN;
                }
                return;
            }
            catch (...){
                _return.code = ErrorCode::E_UNKNOWN;
                return;
            }
        }
    }

    int32_t add(const int32_t num1, const int32_t num2)
    {
        // Your implementation goes here
        return num1 + num2;
        printf("add\n");
    }
    void beginTx(ExecResponse& _return, const int32_t TxnOptions){
        if(TxnOptions)
            _return.code = ErrorCode::E_UNKNOWN;
        return;
    }
    void endTx(ExecResponse& _return, const int32_t shouldCommit){
        if(shouldCommit)
            _return.code = ErrorCode::E_UNKNOWN;
        return;
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
                .schema = std::move(TagSchema)
            },
            .prom = new std::promise<k2::CreateSchemaResult>()
        };

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