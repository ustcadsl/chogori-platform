#thrift的编译命令： thrift -r --gen cpp skvGraphV6.thrift
#skv-graph v6;
#creat space, space like a database in DB;
#create tag(vertex ), tag defined a vertex's shcema 


# struct schema  


typedef i32  GraphSpaceID
typedef i32  PartitionID
typedef i32  TagID
typedef i32  Port


// These are all data types supported in the graph properties
enum PropertyType {
    UNKNOWN = 0,

    // Simple types
    BOOL = 1,
    INT64 = 2,          // This is the same as INT in v1
    VID = 3,            // Deprecated, only supported by v1  V2弃用，仅在V1 版本中使用
    FLOAT = 4,
    DOUBLE = 5,
    STRING = 6,
    // String with fixed length. If the string content is shorteri
    // than the given length, '\0' will be padded to the end
    FIXED_STRING = 7,   // New in v2
    INT8 = 8,           // New in v2
    INT16 = 9,          // New in v2
    INT32 = 10,         // New in v2

    // Date time
    TIMESTAMP = 21,
    DATE = 24,
    DATETIME = 25,
    TIME = 26,
} (cpp.enum_strict)



enum ErrorCode {
    SUCCEEDED          = 0,

    // RPC Failure
    E_DISCONNECTED     = -1,
    E_FAIL_TO_CONNECT  = -2,
    E_RPC_FAILURE      = -3,

    E_LEADER_CHANGED   = -11,

    // Operation Failure
    E_NO_HOSTS         = -21,
    E_EXISTED          = -22,
    E_NOT_FOUND        = -23,
    E_INVALID_HOST     = -24,
    E_UNSUPPORTED      = -25,
    E_NOT_DROP         = -26,
    E_BALANCER_RUNNING = -27,
    E_CONFIG_IMMUTABLE = -28,
    E_CONFLICT         = -29,
    E_INVALID_PARM     = -30,
    E_WRONGCLUSTER     = -31,

    E_STORE_FAILURE             = -32,
    E_STORE_SEGMENT_ILLEGAL     = -33,
    E_BAD_BALANCE_PLAN          = -34,
    E_BALANCED                  = -35,
    E_NO_RUNNING_BALANCE_PLAN   = -36,
    E_NO_VALID_HOST             = -37,
    E_CORRUPTTED_BALANCE_PLAN   = -38,
    E_NO_INVALID_BALANCE_PLAN   = -39,

    // Authentication Failure
    E_INVALID_PASSWORD          = -41,
    E_IMPROPER_ROLE             = -42,
    E_INVALID_PARTITION_NUM     = -43,
    E_INVALID_REPLICA_FACTOR    = -44,
    E_INVALID_CHARSET           = -45,
    E_INVALID_COLLATE           = -46,
    E_CHARSET_COLLATE_NOT_MATCH = -47,

    // Admin Failure
    E_SNAPSHOT_FAILURE       = -51,
    E_BLOCK_WRITE_FAILURE    = -52,
    E_REBUILD_INDEX_FAILURE  = -53,
    E_INDEX_WITH_TTL         = -54,
    E_ADD_JOB_FAILURE        = -55,
    E_STOP_JOB_FAILURE       = -56,
    E_SAVE_JOB_FAILURE       = -57,
    E_BALANCER_FAILURE       = -58,
    E_JOB_NOT_FINISHED       = -59,
    E_TASK_REPORT_OUT_DATE   = -60,
    E_INVALID_JOB            = -61,

    // Backup Failure
    E_BACKUP_FAILURE = -70,
    E_BACKUP_BUILDING_INDEX = -71,
    E_BACKUP_SPACE_NOT_FOUND = -72,

    // RESTORE Failure
    E_RESTORE_FAILURE = -80,

    E_UNKNOWN        = -99,
} (cpp.enum_strict)


union ID {
    1: GraphSpaceID  space_id,
    2: TagID         tag_id,
#    3: EdgeType      edge_type,
#   4: IndexID       index_id,
#   5: ID            cluster_id,
}

struct HostAddr {
    // Host could be a valid IPv4 or IPv6 address, or a valid domain name
    1: string   host,
    2: Port     port,
} (cpp.type = "nebula::HostAddr")


// ExecResp defined the return value
struct ExecResp {
    1: ErrorCode        code,
    // For custom kv operations, it is useless.
    2: ID               id,
    // Valid if ret equals E_LEADER_CHANGED.
   // 3: HostAddr  leader,
}

struct ColumnTypeDef {   //定义每一列的类型
    1: required PropertyType    type,
    // type_length is valid for fixed_string type
    2: optional i16             type_length = 0,
}


struct SpaceDesc {
    1: binary                   space_name,
#    2: i32                      partition_num = 0,
#    3: i32                      replica_factor = 0,
#    4: binary                   charset_name,
#    5: binary                   collate_name,
#    6: ColumnTypeDef            vid_type = {"type": PropertyType.FIXED_STRING, "type_length": 8},
#   7: optional binary          group_name,
#   8: optional IsolationLevel  isolation_level,
#   9: optional binary          comment,
}



struct SpaceItem {
    1: GraphSpaceID         space_id,
    2: SpaceDesc            properties,
}


struct CreateSpaceReq {
    1: SpaceDesc        properties,
    2: bool             if_not_exists,
}

// ttl time to live, 清洗过期数据
struct SchemaProp {
    1: optional i64      ttl_duration,
    2: optional binary   ttl_col,
    3: optional binary   comment,
}


struct ColumnDef {
    1: required binary          name,
    2: required ColumnTypeDef   type,
    3: optional binary          default_value,
    4: optional bool            nullable = false,
    5: optional binary          comment,
}


struct SKVGraphSchema {
    1: list<ColumnDef> columns,  //定义具体的schema
    2: optional SchemaProp schema_prop, //定义schema的ttl的信息，不使用
}



// Tags related operations
struct CreateTagReq {
    1: GraphSpaceID         space_id,
    2: binary               tag_name,
    3: SKVGraphSchema       skv_graph_schema,
    4: bool                 if_not_exists,
}




#storage service

struct PartitionResult {
    1: required ErrorCode           code,
    2: required PartitionID  part_id,
    // Only valid when code is E_LEADER_CHANAGED.
 //   3: optional common.HostAddr     leader,
}

struct ResponseCommon {
    // Only contains the partition that returns error
    1: required list<PartitionResult>   failed_parts,
    // Query latency from storage service
    2: required i32                     latency_in_us,
}

struct ExecResponse {
  //  1: required ResponseCommon result,
    1: required ErrorCode code;   //返回Errorcode作为测试
}


enum NullType {
    __NULL__ = 0,
    NaN      = 1,
    BAD_DATA = 2,
    BAD_TYPE = 3,
    ERR_OVERFLOW = 4,
    UNKNOWN_PROP = 5,
    DIV_BY_ZERO = 6,
    OUT_OF_RANGE = 7,
} 


//只支持基本类型
union Value {
    1: NullType                                 nVal;
    2: bool                                     bVal;
    3: i64                                      iVal;
    4: double                                   fVal;
    5: binary                                   sVal;
    6: i32                                      wVal;
    7: i16                                      hwVal;
 /* 6: Date                                     dVal;
    7: Time                                     tVal;
    8: DateTime                                 dtVal;
    9: Vertex (cpp.type = "nebula::Vertex")     vVal (cpp.ref_type = "unique");
    10: Edge (cpp.type = "nebula::Edge")        eVal (cpp.ref_type = "unique");
    11: Path (cpp.type = "nebula::Path")        pVal (cpp.ref_type = "unique");
    12: NList (cpp.type = "nebula::List")       lVal (cpp.ref_type = "unique");
    13: NMap (cpp.type = "nebula::Map")         mVal (cpp.ref_type = "unique");
    14: NSet (cpp.type = "nebula::Set")         uVal (cpp.ref_type = "unique");
    15: DataSet (cpp.type = "nebula::DataSet")  gVal (cpp.ref_type = "unique");
*/
}



//add Vertex
struct NewTag {
    1: TagID         tag_id,
    2: list<Value>   props,
}

struct NewVertex {
    //1: Value id,      //VID, 使用i64代替 Nebula的 Value定义的数据结构
    1: i64  id,
    2: list<NewTag> tags,
}

struct AddVerticesRequest {
    1: GraphSpaceID                      space_id,
    // partId => vertices
    2: map<PartitionID, list<NewVertex>>   
     (cpp.template = "std::unordered_map")  parts,  
    // A map from TagID -> list of prop_names
    // The order of the property names should match the data order specified
    //   in the NewVertex.NewTag.props
    3: map<TagID, list<binary>>         
     (cpp.template = "std::unordered_map")    prop_names,
    // if ture, when (vertexID,tagID) already exists, do nothing
    4: bool                                     if_not_exists,
}






service  MetaService { 
//meta service
    # space 
    ExecResp createSpace(1: CreateSpaceReq req),

    
    //test  for connection
    i32 add(1:i32 num1, 2:i32 num2)

    # tag
    ExecResp createTag(1: CreateTagReq req);



    
//storage service
    # 2.1 CURD of vertex 

    ExecResponse addVertices(1: AddVerticesRequest req);


// transaction service  参数仅作为测试
    ExecResponse beginTx(1: i32 TxnOptions);
    ExecResponse endTx(1: i32 shouldCommit);
    

    
}