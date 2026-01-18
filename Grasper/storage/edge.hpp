/*
 * @Author: chunyuLi 
 * @Date: 2022-09-06 16:56:45 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-13 16:41:11
 */
#pragma once

#include <iostream>
#include <stdint.h>

#include "utils/config.hpp"
#include "utils/unit.hpp"
#include "core/remote_buffer.hpp"
#include "base/type.hpp"
#include "storage/layout.hpp"

/* Vertex:
 * Vertices are stored in an array
 * need to config IN_NBS, OUT_NBS
 * */

// class EdgeTable
// {
// public:
//     EdgeTable(RemoteBuffer * buf);

//     ~EdgeTable();

//     void init(GraphMeta * graph_meta);

//     Edge * insert(eid_t id);

//     char * get_ext();

//     uint64_t sync_alloc_ext(uint64_t size);

// private:
//     /* data */
//     Config * config_;
//     RemoteBuffer * buf_;
    
//     char* mem;
//     uint64_t mem_size;
//     uint64_t main_size;
//     uint64_t ext_size;
//     uint64_t offset;
//     static const int EXT_RATIO = 0;

//     Edge* edge_array;
//     uint64_t num_edges;

//     // LCY: in test dataset, enough to use one property
//     char* ext;
//     uint64_t last_ext_offset;
//     pthread_spinlock_t ext_lock;
// };
