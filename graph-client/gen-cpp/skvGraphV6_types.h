/**
 * Autogenerated by Thrift Compiler (0.16.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#ifndef skvGraphV6_TYPES_H
#define skvGraphV6_TYPES_H

#include <iosfwd>

#include <thrift/Thrift.h>
#include <thrift/TApplicationException.h>
#include <thrift/TBase.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/transport/TTransport.h>

#include <functional>
#include <memory>




struct PropertyType {
  enum type {
    UNKNOWN = 0,
    BOOL = 1,
    INT64 = 2,
    VID = 3,
    FLOAT = 4,
    DOUBLE = 5,
    STRING = 6,
    FIXED_STRING = 7,
    INT8 = 8,
    INT16 = 9,
    INT32 = 10,
    TIMESTAMP = 21,
    DATE = 24,
    DATETIME = 25,
    TIME = 26
  };
};

extern const std::map<int, const char*> _PropertyType_VALUES_TO_NAMES;

std::ostream& operator<<(std::ostream& out, const PropertyType::type& val);

std::string to_string(const PropertyType::type& val);

struct ErrorCode {
  enum type {
    SUCCEEDED = 0,
    E_DISCONNECTED = -1,
    E_FAIL_TO_CONNECT = -2,
    E_RPC_FAILURE = -3,
    E_LEADER_CHANGED = -11,
    E_NO_HOSTS = -21,
    E_EXISTED = -22,
    E_NOT_FOUND = -23,
    E_INVALID_HOST = -24,
    E_UNSUPPORTED = -25,
    E_NOT_DROP = -26,
    E_BALANCER_RUNNING = -27,
    E_CONFIG_IMMUTABLE = -28,
    E_CONFLICT = -29,
    E_INVALID_PARM = -30,
    E_WRONGCLUSTER = -31,
    E_STORE_FAILURE = -32,
    E_STORE_SEGMENT_ILLEGAL = -33,
    E_BAD_BALANCE_PLAN = -34,
    E_BALANCED = -35,
    E_NO_RUNNING_BALANCE_PLAN = -36,
    E_NO_VALID_HOST = -37,
    E_CORRUPTTED_BALANCE_PLAN = -38,
    E_NO_INVALID_BALANCE_PLAN = -39,
    E_INVALID_PASSWORD = -41,
    E_IMPROPER_ROLE = -42,
    E_INVALID_PARTITION_NUM = -43,
    E_INVALID_REPLICA_FACTOR = -44,
    E_INVALID_CHARSET = -45,
    E_INVALID_COLLATE = -46,
    E_CHARSET_COLLATE_NOT_MATCH = -47,
    E_SNAPSHOT_FAILURE = -51,
    E_BLOCK_WRITE_FAILURE = -52,
    E_REBUILD_INDEX_FAILURE = -53,
    E_INDEX_WITH_TTL = -54,
    E_ADD_JOB_FAILURE = -55,
    E_STOP_JOB_FAILURE = -56,
    E_SAVE_JOB_FAILURE = -57,
    E_BALANCER_FAILURE = -58,
    E_JOB_NOT_FINISHED = -59,
    E_TASK_REPORT_OUT_DATE = -60,
    E_INVALID_JOB = -61,
    E_BACKUP_FAILURE = -70,
    E_BACKUP_BUILDING_INDEX = -71,
    E_BACKUP_SPACE_NOT_FOUND = -72,
    E_RESTORE_FAILURE = -80,
    E_UNKNOWN = -99
  };
};

extern const std::map<int, const char*> _ErrorCode_VALUES_TO_NAMES;

std::ostream& operator<<(std::ostream& out, const ErrorCode::type& val);

std::string to_string(const ErrorCode::type& val);

typedef int32_t GraphSpaceID;

typedef int32_t PartitionID;

typedef int32_t TagID;

typedef int32_t Port;

class ID;

class HostAddr;

class ExecResp;

class ColumnTypeDef;

class SpaceDesc;

class SpaceItem;

class CreateSpaceReq;

class SchemaProp;

class ColumnDef;

class SKVGraphSchema;

class CreateTagReq;

typedef struct _ID__isset {
  _ID__isset() : space_id(false), tag_id(false) {}
  bool space_id :1;
  bool tag_id :1;
} _ID__isset;

class ID : public virtual ::apache::thrift::TBase {
 public:

  ID(const ID&) noexcept;
  ID& operator=(const ID&) noexcept;
  ID() noexcept
     : space_id(0),
       tag_id(0) {
  }

  virtual ~ID() noexcept;
  GraphSpaceID space_id;
  TagID tag_id;

  _ID__isset __isset;

  void __set_space_id(const GraphSpaceID val);

  void __set_tag_id(const TagID val);

  bool operator == (const ID & rhs) const
  {
    if (__isset.space_id != rhs.__isset.space_id)
      return false;
    else if (__isset.space_id && !(space_id == rhs.space_id))
      return false;
    if (__isset.tag_id != rhs.__isset.tag_id)
      return false;
    else if (__isset.tag_id && !(tag_id == rhs.tag_id))
      return false;
    return true;
  }
  bool operator != (const ID &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const ID & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(ID &a, ID &b);

std::ostream& operator<<(std::ostream& out, const ID& obj);

typedef struct _HostAddr__isset {
  _HostAddr__isset() : host(false), port(false) {}
  bool host :1;
  bool port :1;
} _HostAddr__isset;

class HostAddr : public virtual ::apache::thrift::TBase {
 public:

  HostAddr(const HostAddr&);
  HostAddr& operator=(const HostAddr&);
  HostAddr() noexcept
           : host(),
             port(0) {
  }

  virtual ~HostAddr() noexcept;
  std::string host;
  Port port;

  _HostAddr__isset __isset;

  void __set_host(const std::string& val);

  void __set_port(const Port val);

  bool operator == (const HostAddr & rhs) const
  {
    if (!(host == rhs.host))
      return false;
    if (!(port == rhs.port))
      return false;
    return true;
  }
  bool operator != (const HostAddr &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const HostAddr & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(HostAddr &a, HostAddr &b);

std::ostream& operator<<(std::ostream& out, const HostAddr& obj);

typedef struct _ExecResp__isset {
  _ExecResp__isset() : code(false), id(false) {}
  bool code :1;
  bool id :1;
} _ExecResp__isset;

class ExecResp : public virtual ::apache::thrift::TBase {
 public:

  ExecResp(const ExecResp&) noexcept;
  ExecResp& operator=(const ExecResp&) noexcept;
  ExecResp() noexcept
           : code(static_cast<ErrorCode::type>(0)) {
  }

  virtual ~ExecResp() noexcept;
  /**
   * 
   * @see ErrorCode
   */
  ErrorCode::type code;
  ID id;

  _ExecResp__isset __isset;

  void __set_code(const ErrorCode::type val);

  void __set_id(const ID& val);

  bool operator == (const ExecResp & rhs) const
  {
    if (!(code == rhs.code))
      return false;
    if (!(id == rhs.id))
      return false;
    return true;
  }
  bool operator != (const ExecResp &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const ExecResp & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(ExecResp &a, ExecResp &b);

std::ostream& operator<<(std::ostream& out, const ExecResp& obj);

typedef struct _ColumnTypeDef__isset {
  _ColumnTypeDef__isset() : type_length(true) {}
  bool type_length :1;
} _ColumnTypeDef__isset;

class ColumnTypeDef : public virtual ::apache::thrift::TBase {
 public:

  ColumnTypeDef(const ColumnTypeDef&) noexcept;
  ColumnTypeDef& operator=(const ColumnTypeDef&) noexcept;
  ColumnTypeDef() noexcept
                : type(static_cast<PropertyType::type>(0)),
                  type_length(0) {
  }

  virtual ~ColumnTypeDef() noexcept;
  /**
   * 
   * @see PropertyType
   */
  PropertyType::type type;
  int16_t type_length;

  _ColumnTypeDef__isset __isset;

  void __set_type(const PropertyType::type val);

  void __set_type_length(const int16_t val);

  bool operator == (const ColumnTypeDef & rhs) const
  {
    if (!(type == rhs.type))
      return false;
    if (__isset.type_length != rhs.__isset.type_length)
      return false;
    else if (__isset.type_length && !(type_length == rhs.type_length))
      return false;
    return true;
  }
  bool operator != (const ColumnTypeDef &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const ColumnTypeDef & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(ColumnTypeDef &a, ColumnTypeDef &b);

std::ostream& operator<<(std::ostream& out, const ColumnTypeDef& obj);

typedef struct _SpaceDesc__isset {
  _SpaceDesc__isset() : space_name(false) {}
  bool space_name :1;
} _SpaceDesc__isset;

class SpaceDesc : public virtual ::apache::thrift::TBase {
 public:

  SpaceDesc(const SpaceDesc&);
  SpaceDesc& operator=(const SpaceDesc&);
  SpaceDesc() noexcept
            : space_name() {
  }

  virtual ~SpaceDesc() noexcept;
  std::string space_name;

  _SpaceDesc__isset __isset;

  void __set_space_name(const std::string& val);

  bool operator == (const SpaceDesc & rhs) const
  {
    if (!(space_name == rhs.space_name))
      return false;
    return true;
  }
  bool operator != (const SpaceDesc &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const SpaceDesc & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(SpaceDesc &a, SpaceDesc &b);

std::ostream& operator<<(std::ostream& out, const SpaceDesc& obj);

typedef struct _SpaceItem__isset {
  _SpaceItem__isset() : space_id(false), properties(false) {}
  bool space_id :1;
  bool properties :1;
} _SpaceItem__isset;

class SpaceItem : public virtual ::apache::thrift::TBase {
 public:

  SpaceItem(const SpaceItem&);
  SpaceItem& operator=(const SpaceItem&);
  SpaceItem() noexcept
            : space_id(0) {
  }

  virtual ~SpaceItem() noexcept;
  GraphSpaceID space_id;
  SpaceDesc properties;

  _SpaceItem__isset __isset;

  void __set_space_id(const GraphSpaceID val);

  void __set_properties(const SpaceDesc& val);

  bool operator == (const SpaceItem & rhs) const
  {
    if (!(space_id == rhs.space_id))
      return false;
    if (!(properties == rhs.properties))
      return false;
    return true;
  }
  bool operator != (const SpaceItem &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const SpaceItem & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(SpaceItem &a, SpaceItem &b);

std::ostream& operator<<(std::ostream& out, const SpaceItem& obj);

typedef struct _CreateSpaceReq__isset {
  _CreateSpaceReq__isset() : properties(false), if_not_exists(false) {}
  bool properties :1;
  bool if_not_exists :1;
} _CreateSpaceReq__isset;

class CreateSpaceReq : public virtual ::apache::thrift::TBase {
 public:

  CreateSpaceReq(const CreateSpaceReq&);
  CreateSpaceReq& operator=(const CreateSpaceReq&);
  CreateSpaceReq() noexcept
                 : if_not_exists(0) {
  }

  virtual ~CreateSpaceReq() noexcept;
  SpaceDesc properties;
  bool if_not_exists;

  _CreateSpaceReq__isset __isset;

  void __set_properties(const SpaceDesc& val);

  void __set_if_not_exists(const bool val);

  bool operator == (const CreateSpaceReq & rhs) const
  {
    if (!(properties == rhs.properties))
      return false;
    if (!(if_not_exists == rhs.if_not_exists))
      return false;
    return true;
  }
  bool operator != (const CreateSpaceReq &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const CreateSpaceReq & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(CreateSpaceReq &a, CreateSpaceReq &b);

std::ostream& operator<<(std::ostream& out, const CreateSpaceReq& obj);

typedef struct _SchemaProp__isset {
  _SchemaProp__isset() : ttl_duration(false), ttl_col(false), comment(false) {}
  bool ttl_duration :1;
  bool ttl_col :1;
  bool comment :1;
} _SchemaProp__isset;

class SchemaProp : public virtual ::apache::thrift::TBase {
 public:

  SchemaProp(const SchemaProp&);
  SchemaProp& operator=(const SchemaProp&);
  SchemaProp() noexcept
             : ttl_duration(0),
               ttl_col(),
               comment() {
  }

  virtual ~SchemaProp() noexcept;
  int64_t ttl_duration;
  std::string ttl_col;
  std::string comment;

  _SchemaProp__isset __isset;

  void __set_ttl_duration(const int64_t val);

  void __set_ttl_col(const std::string& val);

  void __set_comment(const std::string& val);

  bool operator == (const SchemaProp & rhs) const
  {
    if (__isset.ttl_duration != rhs.__isset.ttl_duration)
      return false;
    else if (__isset.ttl_duration && !(ttl_duration == rhs.ttl_duration))
      return false;
    if (__isset.ttl_col != rhs.__isset.ttl_col)
      return false;
    else if (__isset.ttl_col && !(ttl_col == rhs.ttl_col))
      return false;
    if (__isset.comment != rhs.__isset.comment)
      return false;
    else if (__isset.comment && !(comment == rhs.comment))
      return false;
    return true;
  }
  bool operator != (const SchemaProp &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const SchemaProp & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(SchemaProp &a, SchemaProp &b);

std::ostream& operator<<(std::ostream& out, const SchemaProp& obj);

typedef struct _ColumnDef__isset {
  _ColumnDef__isset() : default_value(false), nullable(true), comment(false) {}
  bool default_value :1;
  bool nullable :1;
  bool comment :1;
} _ColumnDef__isset;

class ColumnDef : public virtual ::apache::thrift::TBase {
 public:

  ColumnDef(const ColumnDef&);
  ColumnDef& operator=(const ColumnDef&);
  ColumnDef() noexcept
            : name(),
              default_value(),
              nullable(false),
              comment() {
  }

  virtual ~ColumnDef() noexcept;
  std::string name;
  ColumnTypeDef type;
  std::string default_value;
  bool nullable;
  std::string comment;

  _ColumnDef__isset __isset;

  void __set_name(const std::string& val);

  void __set_type(const ColumnTypeDef& val);

  void __set_default_value(const std::string& val);

  void __set_nullable(const bool val);

  void __set_comment(const std::string& val);

  bool operator == (const ColumnDef & rhs) const
  {
    if (!(name == rhs.name))
      return false;
    if (!(type == rhs.type))
      return false;
    if (__isset.default_value != rhs.__isset.default_value)
      return false;
    else if (__isset.default_value && !(default_value == rhs.default_value))
      return false;
    if (__isset.nullable != rhs.__isset.nullable)
      return false;
    else if (__isset.nullable && !(nullable == rhs.nullable))
      return false;
    if (__isset.comment != rhs.__isset.comment)
      return false;
    else if (__isset.comment && !(comment == rhs.comment))
      return false;
    return true;
  }
  bool operator != (const ColumnDef &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const ColumnDef & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(ColumnDef &a, ColumnDef &b);

std::ostream& operator<<(std::ostream& out, const ColumnDef& obj);

typedef struct _SKVGraphSchema__isset {
  _SKVGraphSchema__isset() : columns(false), schema_prop(false) {}
  bool columns :1;
  bool schema_prop :1;
} _SKVGraphSchema__isset;

class SKVGraphSchema : public virtual ::apache::thrift::TBase {
 public:

  SKVGraphSchema(const SKVGraphSchema&);
  SKVGraphSchema& operator=(const SKVGraphSchema&);
  SKVGraphSchema() noexcept {
  }

  virtual ~SKVGraphSchema() noexcept;
  std::vector<ColumnDef>  columns;
  SchemaProp schema_prop;

  _SKVGraphSchema__isset __isset;

  void __set_columns(const std::vector<ColumnDef> & val);

  void __set_schema_prop(const SchemaProp& val);

  bool operator == (const SKVGraphSchema & rhs) const
  {
    if (!(columns == rhs.columns))
      return false;
    if (__isset.schema_prop != rhs.__isset.schema_prop)
      return false;
    else if (__isset.schema_prop && !(schema_prop == rhs.schema_prop))
      return false;
    return true;
  }
  bool operator != (const SKVGraphSchema &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const SKVGraphSchema & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(SKVGraphSchema &a, SKVGraphSchema &b);

std::ostream& operator<<(std::ostream& out, const SKVGraphSchema& obj);

typedef struct _CreateTagReq__isset {
  _CreateTagReq__isset() : space_id(false), tag_name(false), skv_graph_schema(false), if_not_exists(false) {}
  bool space_id :1;
  bool tag_name :1;
  bool skv_graph_schema :1;
  bool if_not_exists :1;
} _CreateTagReq__isset;

class CreateTagReq : public virtual ::apache::thrift::TBase {
 public:

  CreateTagReq(const CreateTagReq&);
  CreateTagReq& operator=(const CreateTagReq&);
  CreateTagReq() noexcept
               : space_id(0),
                 tag_name(),
                 if_not_exists(0) {
  }

  virtual ~CreateTagReq() noexcept;
  GraphSpaceID space_id;
  std::string tag_name;
  SKVGraphSchema skv_graph_schema;
  bool if_not_exists;

  _CreateTagReq__isset __isset;

  void __set_space_id(const GraphSpaceID val);

  void __set_tag_name(const std::string& val);

  void __set_skv_graph_schema(const SKVGraphSchema& val);

  void __set_if_not_exists(const bool val);

  bool operator == (const CreateTagReq & rhs) const
  {
    if (!(space_id == rhs.space_id))
      return false;
    if (!(tag_name == rhs.tag_name))
      return false;
    if (!(skv_graph_schema == rhs.skv_graph_schema))
      return false;
    if (!(if_not_exists == rhs.if_not_exists))
      return false;
    return true;
  }
  bool operator != (const CreateTagReq &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const CreateTagReq & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot) override;
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const override;

  virtual void printTo(std::ostream& out) const;
};

void swap(CreateTagReq &a, CreateTagReq &b);

std::ostream& operator<<(std::ostream& out, const CreateTagReq& obj);



#endif
