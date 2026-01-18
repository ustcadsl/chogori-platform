/*
 * @Author: chunyuLi 
 * @Date: 2022-09-06 10:31:54 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-13 16:37:45
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
 * need to config IN_NBS, OUT_NBS in layout.hpp
 * */

class VertexTable
{
public:
    VertexTable(RemoteBuffer * buf);

    ~VertexTable();

    void init(GraphMeta * graph_meta);

    Vertex * insert(vid_t id);

    Vertex * find(vid_t id);

    char * get_ext();
    
    // sync alloc size bytes in ext space  
    uint64_t sync_alloc_ext(uint64_t size);

private:
    /* data */
    Config * config_;
    RemoteBuffer * buf_;
    
    char* mem;
    uint16_t vtx_in_nbs;
    uint16_t vtx_out_nbs;
    uint16_t vtx_table_num;
    uint16_t vtx_base_ratio;

    uint64_t mem_size;
    uint64_t main_size;
    uint64_t ext_size;
    uint64_t offset;
    static const int EXT_RATIO = 80; // ext size ratio

    // use these two ptr to find vtx and ext
    Vertex * vtx_array;
    uint64_t num_vertices;

    char* ext;
    uint64_t last_ext_offset;
    pthread_spinlock_t ext_lock;
};
