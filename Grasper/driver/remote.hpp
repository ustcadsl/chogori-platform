/*
 * @Author: chunyuLi 
 * @Date: 2022-09-07 11:04:16 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-27 16:21:57
 */
#ifndef REMOTE_HPP_
#define REMOTE_HPP_

#include "third_party/zmq.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"

#include "base/core_affinity.hpp"
#include "base/node.hpp"
#include "base/type.hpp"
#include "base/thread_safe_queue.hpp"
#include "base/throughput_monitor.hpp"

#include "core/message.hpp"
#include "core/parser.hpp"
#include "core/buffer.hpp"
#include "core/id_mapper.hpp"
#include "core/rdma_mailbox.hpp"
#include "core/tcp_mailbox.hpp"
#include "core/experts_adapter.hpp"
#include "core/index_store.hpp"
#include "core/progress_monitor.hpp"
#include "core/result_collector.hpp"
#include "core/logical_plan.hpp"

#include "storage/data_store.hpp"
#include "storage/mpi_snapshot.hpp"

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <sys/socket.h>

#include "rlib/rdma_ctrl.hpp"

class Remote {
 public:
    Remote(Node my_node, vector<Node> & local_servers): my_node_(my_node), local_servers_(local_servers) {
        config_ = Config::GetInstance();
        sender_ = NULL;
        m_context_ = NULL;
        m_pd_ = NULL;
        m_cm_channel_ = NULL;
        m_listen_id_ = NULL;
        m_mr_ = NULL;

        buf_ = NULL;
        data_store_ = NULL;
    }

    ~Remote() {
        running_ = false;
        pthread_join(handler_tid_, NULL);
        delete sender_;   
    }

    void Init() {
        // TODO(big) TCP port 
        sender_ = new zmq::socket_t(context_, ZMQ_PUSH);

        char addr[64];
        sprintf(addr, "tcp://*:%d", my_node_.tcp_port + 1);

        sender_->bind(addr);
        cout << "Remote Server bind addr:" << addr << endl;
    }

    struct ibv_mr* RdmaRegisterMem(void* ptr, uint64_t size) {
        struct ibv_mr *mr = ibv_reg_mr(m_pd_, ptr, size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
        if(!mr) {
            perror("Fail to registe memory region");
            return nullptr;
        }
        return mr;
    }

    int CreateConnection(struct rdma_cm_id *cm_id, RemoteBuffer * buf) {
        if(!m_pd_) {
            perror("Fail to alloc pd");
            return -1;
        }

        struct ibv_comp_channel* comp_chan = ibv_create_comp_channel(m_context_);
        if(!comp_chan) {
            perror("Fail to create cq channel");
            return -1;
        }

        struct ibv_cq *cq = ibv_create_cq(m_context_, RC_MAX_SEND_SIZE, NULL, comp_chan, 0);
        if(!cq) {
            perror("Fail to create cq");
            return -1;
        }

        if(ibv_req_notify_cq(cq, 0)) {
            perror("Fail ibv_req_notify_cq");
            return -1;
        }

        struct ibv_qp_init_attr qp_attr;
        memset(&qp_attr, 0, sizeof(ibv_qp_init_attr));
        qp_attr.cap.max_send_wr = RC_MAX_SEND_SIZE;
        qp_attr.cap.max_recv_wr = 1;   /* Can be set to 1, if RC Two-sided is not required */
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_sge = 1;
        qp_attr.cap.max_inline_data = MAX_INLINE_SIZE;
        qp_attr.send_cq = cq;
        qp_attr.recv_cq = cq;
        qp_attr.qp_type - IBV_QPT_RC;

        if(rdma_create_qp(cm_id, m_pd_, &qp_attr)) {
            perror("Fail to create qp");
            return -1;
        }

        m_mr_ = RdmaRegisterMem((void *)buf->GetBuf(), buf->GetRemoteBufSize());
        if(!m_mr_) {
            perror("Fail to register buffer in mr");
            return -1;
        }
        
        struct rdma_conn_param conn_param;
        conn_param.private_data = nullptr; // TODO(question) this should be a command obj ptr? or can be buffer ptr
        conn_param.private_data_len = 0; // TODO(question)
        conn_param.responder_resources = 1;

        if(rdma_accept(cm_id, &conn_param)) {
            perror("Fail to accept rdma");
            return -1;
        }

        return 0;
    }

    void InitRDMA(RemoteBuffer* buf, int listen_port) {
        rdma_ctrl_ = make_shared<RdmaCtrl>(0, listen_port); // nodeid = 0 now
        RdmaCtrl::DevIdx idx{.dev_id =1, .port_id=1};
        rdma_ctrl_->open_thread_local_device(idx);
        RDMA_ASSERT(
            rdma_ctrl_->register_memory(REMOTE_MR_ID, buf->GetBuf(), buf->GetRemoteBufSize(), rdma_ctrl_->get_device()) == true
        );            
    }

    void * graphMetaConnection() {
        pthread_detach(pthread_self());
        
        ibinstream m;
        m << data_store_->GetGraphMeta();

        while(running_) {
            size_t metasize = m.size();
            zmq::message_t msg(&metasize, sizeof(size_t));
            sender_->send(msg);

            zmq::message_t metamsg(metasize);
            memcpy((void*)metamsg.data(), m.get_buf(), metasize);
            sender_->send(metamsg);
            break; // no need to polling
        }
        return NULL;
    }

    static void * graphMetaConnection_helper(void *context) {
        return ((Remote *)context)->graphMetaConnection();
    }
    
    void Start() {
        m_stop_  = false;

        // ===================prepare stage=================
        // Alloc remote memory
        buf_ = make_unique<RemoteBuffer>();
        cout << "Remote" << my_node_.get_local_rank() << ": DONE -> ALLOC REMOTE MEM, SIZE = " << buf_->GetRemoteBufSize() << endl;

        // Init RDMA
        // LCY: current hard code this port in code, note remote use two tcp port 
        //      to communicate with client
        //      1. bind to receiver_ to receive request and send graphmeta
        //      2. use by rdmactrl as a listen port? TODO figure this out
        InitRDMA(buf_.get(), my_node_.tcp_port);
        cout << "Remote" << my_node_.get_local_rank() << ": DONE -> RDMA->Init()" << endl;  

        // Init datastore
        data_store_ = make_unique<DataStore>(my_node_, nullptr, buf_.get());
        DataStore::StaticInstanceP(data_store_.get());
        data_store_->Init(local_servers_);
        cout << "Remote" << my_node_.get_local_rank() << ": DONE -> DataStore->Init()" << endl;

        // read snapshot area
        // datastore->ReadSnapshot();

        // LCY: LoadDataFromHDFS load dataset files in some tmp struct but not settle down
        data_store_->LoadDataFromHDFS();
        cout << "Remote: DONE -> DataStore->LoadDataFromHDFS()" << endl; 
        // worker_barrier(my_node_);

        // =======data shuffle==========
        // LCY: Since one server, no bother to shuffle data
        // datastore->Shuffle();
        // cout << "Remote" << my_node_.get_local_rank() << ": DONE -> DataStore->Shuffle()" << endl;
        // =======data shuffle==========

        data_store_->DataConverter();
        // worker_barrier(my_node_);
        cout << "Remote" << my_node_.get_local_rank()  << ": DONE -> Datastore->DataConverter()" << endl;

        // write snapshot area
        // LCY: tmp not use snapshot now, for operator >> in in/outstream not implement new defined vertextable and edgetable type
        // datastore->WriteSnapshot();

        // fflush(stdout);
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_create(&handler_tid_, &attr, Remote::graphMetaConnection_helper, this);
       
        
        cout << "Remote Server Are All Ready ..." << endl;
    }

    bool Run() {
    // Now server just waits for user typing quit to finish
    // Server's CPU is not used during one-sided RDMA requests from clients
    printf("====================================================================================================\n");
    printf(
        "Server now runs as a disaggregated mode. No CPU involvement during RDMA-based transaction processing\n"
        "Type c to run another round, type q if you want to exit :)\n");
    while (running_) {
        char ch;
        while(scanf("%c", &ch) == 0);
        if (ch == 'q') {
            return false;
        } else if (ch == 'c') {
            return true;
        } else {
            printf("Type c to run another round, type q if you want to exit :)\n");
        }
            usleep(2000);
        }
        return false;
    }

 private:
    // Node Config  
    Node my_node_;

    RdmaCtrlPtr rdma_ctrl_;
    
    Config * config_;
    vector<Node> local_servers_;
    
    ThreadSafeQueue<LogicPlan> queue_;

    struct ibv_context* m_context_;
    struct ibv_pd* m_pd_;
    struct rdma_event_channel* m_cm_channel_;
    struct rdma_cm_id* m_listen_id_;
    std::thread *m_conn_handler_;
    bool m_stop_;
    struct ibv_mr* m_mr_;

    zmq::context_t context_;
    // use tcp to send metadata to compute servers
    zmq::socket_t * sender_; 
    pthread_t handler_tid_;

    bool running_ = true;

    std::unique_ptr<RemoteBuffer> buf_;
    std::unique_ptr<DataStore> data_store_;
};

#endif /* WORKER_HPP_ */
