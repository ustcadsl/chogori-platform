#define CATCH_CONFIG_MAIN

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

namespace k2{
const char* collname = "skv_collection";
const char* scname = "skv_schema";
const char* scWoRKey = "skv_schema_wo_rkey";  // schema name without range key
const char* scETC = "skv_error_case";         // schema name: error test cases



class schemaCreation {
public: // application lifespan
    schemaCreation() { K2LOG_I(log::k23si, "ctor"); }
    ~schemaCreation() {K2LOG_I(log::k23si, "dtor"); }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::k23si, "stop");
        return std::move(_testFuture);
    }

    seastar::future<> start() {

    K2LOG_I(log::k23si, "+++++++ start schema creation test +++++++");
    ConfigVar<String> configEp("cpo_endpoint");
    _cpoEndpoint = RPC().getTXEndpoint(configEp());

    // let start() finish and then run the tests
    _testTimer.set_callback([this] {
        _testFuture = runScenario00()
           .then([this] {
            K2LOG_I(log::k23si, "======= All tests passed ========");
            exitcode = 0;
        })
    .handle_exception([this](auto exc) {
            try {
                std::rethrow_exception(exc);
            } catch (RPCDispatcher::RequestTimeoutException& exc) {
                K2LOG_E(log::k23si, "======= Test failed due to timeout ========");
                exitcode = -1;
            } catch (std::exception& e) {
                K2LOG_E(log::k23si, "======= Test failed with exception [{}] ========", e.what());
                exitcode = -1;
            }
        })
        .finally([this] {
            K2LOG_I(log::k23si, "======= Test ended ========");
            seastar::engine().exit(exitcode);
        });
    });
    _testTimer.arm(0ms);
        return seastar::make_ready_future<>();
}

private:
    std::unique_ptr<k2::TXEndpoint> _cpoEndpoint;
    k2::ConfigVar<std::vector<k2::String>> _k2ConfigEps{"k2_endpoints"};
    seastar::future<> _testFuture = seastar::make_ready_future();
    seastar::timer<> _testTimer;
    int exitcode = -1;

    seastar::future<> createCollection(){
        K2LOG_I(log::k23si, "create a collection with assignments");

        auto request = dto::CollectionCreateRequest{
            .metadata{
                .name = collname,
                .hashScheme=dto::HashScheme::HashCRC32C,
                .storageDriver=dto::StorageDriver::K23SI,
                .capacity{
                    .dataCapacityMegaBytes = 100,
                    .readIOPs = 100000,
                    .writeIOPs = 100000
                },
                .retentionPeriod = 5h
            },
            .clusterEndpoints = _k2ConfigEps(),
            .rangeEnds{}
        };
        return RPC()
            .callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, request, *_cpoEndpoint, 1s)
            .then([](auto&& response) {
                // create the collection
                auto& [status, resp] = response;
                K2EXPECT(log::k23si, status, Statuses::S201_Created);
            })
            .then([] {
                // wait for collection to get assigned
                return seastar::sleep(200ms);
            })
            .then([this] {
                // check to make sure the collection is assigned
                auto request = dto::CollectionGetRequest{.name = collname};
                return RPC()
                    .callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, request, *_cpoEndpoint, 100ms);
            })

    }


public: // tests

seastar::future<> runScenario00() {
    K2LOG_I(log::k23si, "+++++++ Schema Creation Test 00: initiate a schema with partition&range key +++++++");
    K2LOG_I(log::k23si, "STEP1: assign a collection named {}", collname);

    // step 1
    return createCollection()
    // step 2
    .then([this] {
        K2LOG_I(log::k23si, "------- create collection success. -------");
        K2LOG_I(log::k23si, "STEP2: create a shema named \"skv_schema\" with 3 fields {{FirstName | LastName | Balance}}");
        dto::Schema schema;
        schema.name = scname;
        schema.version = 1;
        schema.fields = std::vector<dto::SchemaField> {
                {dto::FieldType::STRING, "FirstName", false, false},
                {dto::FieldType::STRING, "LastName", false, false},
                {dto::FieldType::INT32T, "Balance", false, false}
        };

        schema.setPartitionKeyFieldsByName(std::vector<String>{"LastName"});
        schema.setRangeKeyFieldsByName(std::vector<String> {"FirstName"});

        dto::CreateSchemaRequest request{ collname, std::move(schema) };
        return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
        .then([this] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);

            dto::GetSchemasRequest request { collname };
            return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s);
        })
        .then([] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);
            K2EXPECT(log::k23si, resp.schemas.size(), 1);
            K2EXPECT(log::k23si, resp.schemas[0].name, scname);

            K2LOG_I(log::k23si, "------- create schema success. -------");
            return seastar::make_ready_future<>();
        });
    });
}

int main(int argc, char** argv) {
    k2::App app("schemaCreationTest");
    app.addOptions()("k2_endpoints", bpo::value<std::vector<k2::String>>()->multitoken(), "The endpoints of the k2 cluster");
    app.addOptions()("cpo_endpoint", bpo::value<k2::String>(), "The endpoint of the CPO");
    app.addApplet<k2::schemaCreation>();
    return app.start(argc, argv);
}







//create Tag
class CreateTagHandler {
public:  // application lifespan
    CreateTagHandler(const CreateTagReq& req) { K2LOG_I(log::k23si, "ctor"); ;_req = req;}
    ~CreateTagHandler(){ K2LOG_I(log::k23si, "dtor");}

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::k23si, "stop");
        return std::move(_testFuture);
    }
    
    
    

    void start(){
         
        tag_exitcode=-1;

        dto::schema schema;
        std::string collname=_req.space_name;
        schema.name = _req.tag_name;
        schema.version = 1;
        
        schema.fields = std::vector<dto::SchemaField> {
                {dto::FieldType::INT16T, "Type", false, false},
                {dto::FieldType::INT16T, "PartID", false, false},
                {dto::FieldType::INT64T, "VertexID", false, false}
                {dto::FieldType::INT32T, "TagID", false, false}
        };
        

        schema.setPartitionKeyFieldsByName(std::vector<String>{"Type","PartID"});
        schema.setRangeKeyFieldsByName(std::vector<String> {"VertexID","TagID"});

        dto::CreateSchemaRequest request{ collname, std::move(schema) };

        return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *_cpoEndpoint, 1s)
        .then([this] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);

            dto::GetSchemasRequest request { collname };
            return RPC().callRPC<dto::GetSchemasRequest, dto::GetSchemasResponse>(dto::Verbs::CPO_SCHEMAS_GET, request, *_cpoEndpoint, 1s);
        })
        .then([] (auto&& response) {
            auto& [status, resp] = response;
            K2EXPECT(log::k23si, status, Statuses::S200_OK);
            K2EXPECT(log::k23si, resp.schemas.size(), 1);
            K2EXPECT(log::k23si, resp.schemas[0].name, scname);

            K2LOG_I(log::k23si, "------- create schema success. -------");
            tag_exitcode=0;
            return seastar::make_ready_future<>();
        });



    private:

    seastar::timer<> _testTimer;
    seastar::future<> _testFuture = seastar::make_ready_future();

    // K23SIClient _client;
    
    
    CreateTagReqTest _req;


};
