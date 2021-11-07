/**
 * Autogenerated by Thrift Compiler (0.16.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#ifndef MetaService_H
#define MetaService_H

#include <thrift/TDispatchProcessor.h>
#include <thrift/async/TConcurrentClientSyncInfo.h>
#include <memory>
#include "skvGraphV2_types.h"



#ifdef _MSC_VER
  #pragma warning( push )
  #pragma warning (disable : 4250 ) //inheriting methods via dominance 
#endif

class MetaServiceIf {
 public:
  virtual ~MetaServiceIf() {}
  virtual int32_t createSpace(const CreateSpaceReq& req) = 0;
  virtual int32_t add(const int32_t num1, const int32_t num2) = 0;
};

class MetaServiceIfFactory {
 public:
  typedef MetaServiceIf Handler;

  virtual ~MetaServiceIfFactory() {}

  virtual MetaServiceIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) = 0;
  virtual void releaseHandler(MetaServiceIf* /* handler */) = 0;
  };

class MetaServiceIfSingletonFactory : virtual public MetaServiceIfFactory {
 public:
  MetaServiceIfSingletonFactory(const ::std::shared_ptr<MetaServiceIf>& iface) : iface_(iface) {}
  virtual ~MetaServiceIfSingletonFactory() {}

  virtual MetaServiceIf* getHandler(const ::apache::thrift::TConnectionInfo&) override {
    return iface_.get();
  }
  virtual void releaseHandler(MetaServiceIf* /* handler */) override {}

 protected:
  ::std::shared_ptr<MetaServiceIf> iface_;
};

class MetaServiceNull : virtual public MetaServiceIf {
 public:
  virtual ~MetaServiceNull() {}
  int32_t createSpace(const CreateSpaceReq& /* req */) override {
    int32_t _return = 0;
    return _return;
  }
  int32_t add(const int32_t /* num1 */, const int32_t /* num2 */) override {
    int32_t _return = 0;
    return _return;
  }
};

typedef struct _MetaService_createSpace_args__isset {
  _MetaService_createSpace_args__isset() : req(false) {}
  bool req :1;
} _MetaService_createSpace_args__isset;

class MetaService_createSpace_args {
 public:

  MetaService_createSpace_args(const MetaService_createSpace_args&);
  MetaService_createSpace_args& operator=(const MetaService_createSpace_args&);
  MetaService_createSpace_args() noexcept {
  }

  virtual ~MetaService_createSpace_args() noexcept;
  CreateSpaceReq req;

  _MetaService_createSpace_args__isset __isset;

  void __set_req(const CreateSpaceReq& val);

  bool operator == (const MetaService_createSpace_args & rhs) const
  {
    if (!(req == rhs.req))
      return false;
    return true;
  }
  bool operator != (const MetaService_createSpace_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const MetaService_createSpace_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};


class MetaService_createSpace_pargs {
 public:


  virtual ~MetaService_createSpace_pargs() noexcept;
  const CreateSpaceReq* req;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _MetaService_createSpace_result__isset {
  _MetaService_createSpace_result__isset() : success(false) {}
  bool success :1;
} _MetaService_createSpace_result__isset;

class MetaService_createSpace_result {
 public:

  MetaService_createSpace_result(const MetaService_createSpace_result&) noexcept;
  MetaService_createSpace_result& operator=(const MetaService_createSpace_result&) noexcept;
  MetaService_createSpace_result() noexcept
                                 : success(0) {
  }

  virtual ~MetaService_createSpace_result() noexcept;
  int32_t success;

  _MetaService_createSpace_result__isset __isset;

  void __set_success(const int32_t val);

  bool operator == (const MetaService_createSpace_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const MetaService_createSpace_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const MetaService_createSpace_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _MetaService_createSpace_presult__isset {
  _MetaService_createSpace_presult__isset() : success(false) {}
  bool success :1;
} _MetaService_createSpace_presult__isset;

class MetaService_createSpace_presult {
 public:


  virtual ~MetaService_createSpace_presult() noexcept;
  int32_t* success;

  _MetaService_createSpace_presult__isset __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

typedef struct _MetaService_add_args__isset {
  _MetaService_add_args__isset() : num1(false), num2(false) {}
  bool num1 :1;
  bool num2 :1;
} _MetaService_add_args__isset;

class MetaService_add_args {
 public:

  MetaService_add_args(const MetaService_add_args&) noexcept;
  MetaService_add_args& operator=(const MetaService_add_args&) noexcept;
  MetaService_add_args() noexcept
                       : num1(0),
                         num2(0) {
  }

  virtual ~MetaService_add_args() noexcept;
  int32_t num1;
  int32_t num2;

  _MetaService_add_args__isset __isset;

  void __set_num1(const int32_t val);

  void __set_num2(const int32_t val);

  bool operator == (const MetaService_add_args & rhs) const
  {
    if (!(num1 == rhs.num1))
      return false;
    if (!(num2 == rhs.num2))
      return false;
    return true;
  }
  bool operator != (const MetaService_add_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const MetaService_add_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};


class MetaService_add_pargs {
 public:


  virtual ~MetaService_add_pargs() noexcept;
  const int32_t* num1;
  const int32_t* num2;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _MetaService_add_result__isset {
  _MetaService_add_result__isset() : success(false) {}
  bool success :1;
} _MetaService_add_result__isset;

class MetaService_add_result {
 public:

  MetaService_add_result(const MetaService_add_result&) noexcept;
  MetaService_add_result& operator=(const MetaService_add_result&) noexcept;
  MetaService_add_result() noexcept
                         : success(0) {
  }

  virtual ~MetaService_add_result() noexcept;
  int32_t success;

  _MetaService_add_result__isset __isset;

  void __set_success(const int32_t val);

  bool operator == (const MetaService_add_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const MetaService_add_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const MetaService_add_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _MetaService_add_presult__isset {
  _MetaService_add_presult__isset() : success(false) {}
  bool success :1;
} _MetaService_add_presult__isset;

class MetaService_add_presult {
 public:


  virtual ~MetaService_add_presult() noexcept;
  int32_t* success;

  _MetaService_add_presult__isset __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

class MetaServiceClient : virtual public MetaServiceIf {
 public:
  MetaServiceClient(std::shared_ptr< ::apache::thrift::protocol::TProtocol> prot) {
    setProtocol(prot);
  }
  MetaServiceClient(std::shared_ptr< ::apache::thrift::protocol::TProtocol> iprot, std::shared_ptr< ::apache::thrift::protocol::TProtocol> oprot) {
    setProtocol(iprot,oprot);
  }
 private:
  void setProtocol(std::shared_ptr< ::apache::thrift::protocol::TProtocol> prot) {
  setProtocol(prot,prot);
  }
  void setProtocol(std::shared_ptr< ::apache::thrift::protocol::TProtocol> iprot, std::shared_ptr< ::apache::thrift::protocol::TProtocol> oprot) {
    piprot_=iprot;
    poprot_=oprot;
    iprot_ = iprot.get();
    oprot_ = oprot.get();
  }
 public:
  std::shared_ptr< ::apache::thrift::protocol::TProtocol> getInputProtocol() {
    return piprot_;
  }
  std::shared_ptr< ::apache::thrift::protocol::TProtocol> getOutputProtocol() {
    return poprot_;
  }
  int32_t createSpace(const CreateSpaceReq& req) override;
  void send_createSpace(const CreateSpaceReq& req);
  int32_t recv_createSpace();
  int32_t add(const int32_t num1, const int32_t num2) override;
  void send_add(const int32_t num1, const int32_t num2);
  int32_t recv_add();
 protected:
  std::shared_ptr< ::apache::thrift::protocol::TProtocol> piprot_;
  std::shared_ptr< ::apache::thrift::protocol::TProtocol> poprot_;
  ::apache::thrift::protocol::TProtocol* iprot_;
  ::apache::thrift::protocol::TProtocol* oprot_;
};

class MetaServiceProcessor : public ::apache::thrift::TDispatchProcessor {
 protected:
  ::std::shared_ptr<MetaServiceIf> iface_;
  virtual bool dispatchCall(::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, const std::string& fname, int32_t seqid, void* callContext) override;
 private:
  typedef  void (MetaServiceProcessor::*ProcessFunction)(int32_t, ::apache::thrift::protocol::TProtocol*, ::apache::thrift::protocol::TProtocol*, void*);
  typedef std::map<std::string, ProcessFunction> ProcessMap;
  ProcessMap processMap_;
  void process_createSpace(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext);
  void process_add(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext);
 public:
  MetaServiceProcessor(::std::shared_ptr<MetaServiceIf> iface) :
    iface_(iface) {
    processMap_["createSpace"] = &MetaServiceProcessor::process_createSpace;
    processMap_["add"] = &MetaServiceProcessor::process_add;
  }

  virtual ~MetaServiceProcessor() {}
};

class MetaServiceProcessorFactory : public ::apache::thrift::TProcessorFactory {
 public:
  MetaServiceProcessorFactory(const ::std::shared_ptr< MetaServiceIfFactory >& handlerFactory) noexcept :
      handlerFactory_(handlerFactory) {}

  ::std::shared_ptr< ::apache::thrift::TProcessor > getProcessor(const ::apache::thrift::TConnectionInfo& connInfo) override;

 protected:
  ::std::shared_ptr< MetaServiceIfFactory > handlerFactory_;
};

class MetaServiceMultiface : virtual public MetaServiceIf {
 public:
  MetaServiceMultiface(std::vector<std::shared_ptr<MetaServiceIf> >& ifaces) : ifaces_(ifaces) {
  }
  virtual ~MetaServiceMultiface() {}
 protected:
  std::vector<std::shared_ptr<MetaServiceIf> > ifaces_;
  MetaServiceMultiface() {}
  void add(::std::shared_ptr<MetaServiceIf> iface) {
    ifaces_.push_back(iface);
  }
 public:
  int32_t createSpace(const CreateSpaceReq& req) override {
    size_t sz = ifaces_.size();
    size_t i = 0;
    for (; i < (sz - 1); ++i) {
      ifaces_[i]->createSpace(req);
    }
    return ifaces_[i]->createSpace(req);
  }

  int32_t add(const int32_t num1, const int32_t num2) override {
    size_t sz = ifaces_.size();
    size_t i = 0;
    for (; i < (sz - 1); ++i) {
      ifaces_[i]->add(num1, num2);
    }
    return ifaces_[i]->add(num1, num2);
  }

};

// The 'concurrent' client is a thread safe client that correctly handles
// out of order responses.  It is slower than the regular client, so should
// only be used when you need to share a connection among multiple threads
class MetaServiceConcurrentClient : virtual public MetaServiceIf {
 public:
  MetaServiceConcurrentClient(std::shared_ptr< ::apache::thrift::protocol::TProtocol> prot, std::shared_ptr<::apache::thrift::async::TConcurrentClientSyncInfo> sync) : sync_(sync)
{
    setProtocol(prot);
  }
  MetaServiceConcurrentClient(std::shared_ptr< ::apache::thrift::protocol::TProtocol> iprot, std::shared_ptr< ::apache::thrift::protocol::TProtocol> oprot, std::shared_ptr<::apache::thrift::async::TConcurrentClientSyncInfo> sync) : sync_(sync)
{
    setProtocol(iprot,oprot);
  }
 private:
  void setProtocol(std::shared_ptr< ::apache::thrift::protocol::TProtocol> prot) {
  setProtocol(prot,prot);
  }
  void setProtocol(std::shared_ptr< ::apache::thrift::protocol::TProtocol> iprot, std::shared_ptr< ::apache::thrift::protocol::TProtocol> oprot) {
    piprot_=iprot;
    poprot_=oprot;
    iprot_ = iprot.get();
    oprot_ = oprot.get();
  }
 public:
  std::shared_ptr< ::apache::thrift::protocol::TProtocol> getInputProtocol() {
    return piprot_;
  }
  std::shared_ptr< ::apache::thrift::protocol::TProtocol> getOutputProtocol() {
    return poprot_;
  }
  int32_t createSpace(const CreateSpaceReq& req) override;
  int32_t send_createSpace(const CreateSpaceReq& req);
  int32_t recv_createSpace(const int32_t seqid);
  int32_t add(const int32_t num1, const int32_t num2) override;
  int32_t send_add(const int32_t num1, const int32_t num2);
  int32_t recv_add(const int32_t seqid);
 protected:
  std::shared_ptr< ::apache::thrift::protocol::TProtocol> piprot_;
  std::shared_ptr< ::apache::thrift::protocol::TProtocol> poprot_;
  ::apache::thrift::protocol::TProtocol* iprot_;
  ::apache::thrift::protocol::TProtocol* oprot_;
  std::shared_ptr<::apache::thrift::async::TConcurrentClientSyncInfo> sync_;
};

#ifdef _MSC_VER
  #pragma warning( pop )
#endif



#endif