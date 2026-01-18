/* Copyright 2019 Husky Data Lab, CUHK

Authors: Changji Li (cjli@cse.cuhk.edu.hk)
         Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include <assert.h>
#include "storage/vkvstore_local.hpp"
#include "storage/mpi_snapshot.hpp"
#include "storage/snapshot_func.hpp"


using namespace std;

// // Get ikey_t by vpid or epid
// void VKVStore_Local::get_key_local(uint64_t pid, ikey_t & key) {
//     uint64_t bucket_id = pid % num_buckets;
//     while (true) {
//         for (int i = 0; i < ASSOCIATIVITY; i++) {
//             uint64_t slot_id = bucket_id * ASSOCIATIVITY + i;
//             if (i < ASSOCIATIVITY - 1) {
//                 // data part
//                 if (keys[slot_id].pid == pid) {
//                     // we found it
//                     key = keys[slot_id];
//                     return;
//                 }
//             } else {
//                 if (keys[slot_id].is_empty())
//                     return;

//                 bucket_id = keys[slot_id].pid;  // move to next bucket
//                 break;  // break for-loop
//             }
//         }
//     }
// }

// Get key by key remotely
void VKVStore_Local::get_key_remote(int tid, int dst_nid, uint64_t pid, ikey_t & key) {
    uint64_t bucket_id = pid % num_buckets;

    while (true) {
        char * buffer = buf_->GetSendBuf(tid);
        uint64_t off = offset + bucket_id * ASSOCIATIVITY * sizeof(ikey_t);
        uint64_t sz = ASSOCIATIVITY * sizeof(ikey_t);

        RDMA &rdma = RDMA::get_rdma();
        // timer::start_timer(tid);
        // RDMA_LOG(INFO) << "In vp get key remote";
        rdma.dev->RdmaRead(tid, dst_nid, buffer, sz, off);
        // timer::stop_timer(tid);

        ikey_t * keys = (ikey_t *)buffer;
        for (int i = 0; i < ASSOCIATIVITY; ++i) {
            if (i < ASSOCIATIVITY - 1) {
                if (keys[i].pid == pid) {
                    key = keys[i];
                    return;
                }
            } else {
                if (keys[i].is_empty())
                    return;  // not found

                bucket_id = keys[i].pid;  // move to next bucket
                break;  // break for-loop
            }
        }
    }
}

VKVStore_Local::VKVStore_Local(Buffer * buf) : buf_(buf) {
    // config_ = Config::GetInstance();
    // mem = config_->kvstore;
    // mem_sz = GiB2B(config_->global_vertex_property_kv_sz_gb);
    // offset = config_->kvstore_offset;
    // HD_RATIO = config_->key_value_ratio_in_rdma;

    // size for header and entry
    // uint64_t header_sz = mem_sz * HD_RATIO / 100;
    // uint64_t entry_sz = mem_sz - header_sz;

    // header region
    // num_slots = header_sz / sizeof(ikey_t);
    // num_buckets = mymath::hash_prime_u64((num_slots / ASSOCIATIVITY) * MHD_RATIO / 100);
    // num_buckets_ext = (num_slots / ASSOCIATIVITY) - num_buckets;
    // last_ext = 0;

    // entry region
    // num_entries = entry_sz;
    // last_entry = 0;

    // cout << "INFO: VKVStore_Local = " << header_sz + entry_sz << " bytes " << std::endl
    //      << "      header region: " << num_slots << " slots"
    //      << " (main = " << num_buckets << ", indirect = " << num_buckets_ext << ")" << std::endl
    //      << "      entry region: " << num_entries << " entries" << std::endl;

    // // Header
    // keys = (ikey_t *)(mem);
    // // Entry
    // values = (char *)(mem + num_slots * sizeof(ikey_t));

    // pthread_spin_init(&entry_lock, 0);
    // pthread_spin_init(&bucket_ext_lock, 0);
    // for (int i = 0; i < NUM_LOCKS; i++)
    //     pthread_spin_init(&bucket_locks[i], 0);
}

void VKVStore_Local::init(vector<Node> & nodes, uint64_t off, uint64_t slots_num, uint64_t buckets_num) {
    offset = off;
    num_slots = slots_num;
    num_buckets = buckets_num;
    // // initiate keys to 0 which means empty key
    // for (uint64_t i = 0; i < num_slots; i++) {
    //     keys[i] = ikey_t();
    // }

    // if (!config_->global_use_rdma) {
    //     requesters.resize(config_->global_num_workers);
    //     for (int nid = 0; nid < config_->global_num_workers; nid++) {
    //         Node & r_node = GetNodeById(nodes, nid + 1);
    //         string ibname = r_node.ibname;

    //         requesters[nid] = new zmq::socket_t(context, ZMQ_REQ);
    //         char addr[64] = "";
    //         sprintf(addr, "tcp://%s:%d", ibname.c_str(), r_node.tcp_port + 10 + config_->global_num_threads);
    //         requesters[nid]->connect(addr);
    //     }
    //     pthread_spin_init(&req_lock, 0);
    // }
}

// // Get properties by key locally
// void VKVStore_Local::get_property_local(uint64_t pid, value_t & val) {
//     ikey_t key;
//     // no need to hash pid
//     get_key_local(pid, key);

//     if (key.is_empty()) {
//         val.content.resize(0);
//         return;
//     }

//     uint64_t off = key.ptr.off;
//     uint64_t size = key.ptr.size - 1;

//     // type : char to uint8_t
//     val.type = values[off++];
//     val.content.resize(size);

//     char * ctt = &(values[off]);
//     std::copy(ctt, ctt+size, val.content.begin());
// }

// Get properties by key remotely
void VKVStore_Local::get_property_remote(int tid, int dst_nid, uint64_t pid, value_t & val) {
    ikey_t key;
    // no need to hash pid
    get_key_remote(tid, dst_nid, pid, key); // first RDMA read
        if (key.is_empty()) {
            val.content.resize(0);
            return;
        }

        char * buffer = buf_->GetSendBuf(tid);
        uint64_t r_off = offset + num_slots * sizeof(ikey_t) + key.ptr.off;
        uint64_t r_sz = key.ptr.size;

        RDMA &rdma = RDMA::get_rdma();
        // timer::start_timer(tid);
        // RDMA_LOG(INFO) << "In vp get property remote";
        rdma.dev->RdmaRead(tid, dst_nid, buffer, r_sz, r_off);
        // timer::stop_timer(tid);

        // type : char to uint8_t
        val.type = buffer[0];
        val.content.resize(r_sz-1);

        char * ctt = &(buffer[1]);
        std::copy(ctt, ctt + r_sz-1, val.content.begin());
}

// void VKVStore_Local::get_label_local(uint64_t pid, label_t & label) {
//     value_t val;
//     get_property_local(pid, val);
//     label = (label_t)Tool::value_t2int(val);
// }


void VKVStore_Local::get_label_remote(int tid, int dst_nid, uint64_t pid, label_t & label) {
    value_t val;
    get_property_remote(tid, dst_nid, pid, val);
    label = (label_t)Tool::value_t2int(val);
}
