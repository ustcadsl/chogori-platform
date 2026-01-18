/*
 * Copyright (c) 2016 Shanghai Jiao Tong University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://ipads.se.sjtu.edu.cn/projects/wukong
 *
 */

/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once

// #pragma GCC diagnostic warning "-fpermissive"

#include <vector>
#include <string>
#include <iostream>     // std::cout
#include <fstream>      // std::ifstream
#include "base/node.hpp"

using namespace std;

#include "utils/timer.hpp"
#include "rlib/rdma_ctrl.hpp"

using namespace rdmaio;

#define REMOTE_MR_ID 97
#define LOCAL_MR_ID 100
#define REMOTE_NID 0
#define RC_MAX_SEND_SIZE 1024

class RDMA_Device {
    // use new third party rdma lib
    // 1. update init function, in current impl, QPs are created and connected between different workers
    //    workers pass logic plan by msgs (not change now)
    //    but worker -> remote connection is not created
    // 2. update read/write API
    // 3. update API usage in **_expert
public:
    RdmaCtrlPtr global_rdma_ctrl = nullptr;

    RNicHandler * opened_rnic;

    RDMA_Device(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes, vector<Node> & memory_nodes);

    // 0 on success, -1 otherwise
    int RdmaRead(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off);

    int RdmaReadBatch(int dst_tid, int dst_nid, char *local, uint64_t size, vector<uint64_t> &off, uint64_t begin, uint64_t len);

    int RdmaWrite(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off);

private:   

    void GetMRMeta(const vector<Node>& memory_nodes);    
    
    void AllocMR(char *mem, uint64_t mem_sz);

    int num_threads_;

    vector<Node>& memory_nodes_;

    // LCY: if more remote nodes, change this to a map<node_id, MemoryAttr>
    MemoryAttr remote_mr_;

    // QPManager *qp_man_;        
    };

class RDMA {
 public:
    RDMA_Device *dev = NULL;

    RDMA() { }

    ~RDMA() { if (dev != NULL) delete dev; }

    void init_dev(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes, vector<Node> & memory_nodes) {
        dev = new RDMA_Device(num_nodes, num_threads, nid, mem, mem_sz, nodes, memory_nodes);
    }

    inline static bool has_rdma() { return true; }

    static RDMA &get_rdma() {
        static RDMA rdma;
        return rdma;
    }
};

void RDMA_init(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes, vector<Node> & memory_nodes);
