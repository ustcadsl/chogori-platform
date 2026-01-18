/*
 * @Author: chunyuLi 
 * @Date: 2022-09-06 10:32:39 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-13 17:58:11
 */

#include "storage/vertex.hpp"
#include "storage/data_store.hpp"

using namespace std;

VertexTable::VertexTable(RemoteBuffer * buf) : buf_(buf) {
    config_ = Config::GetInstance();
    mem = config_->vtx_store;
    vtx_in_nbs = config_->global_vertex_in_nbs;
    vtx_out_nbs = config_->global_vertex_out_nbs;
    vtx_table_num = config_->global_vertex_table_num;
    vtx_base_ratio = config_->global_vertex_base_ratio;
    mem_size= GiB2B(config_->global_vertex_sz_gb);
    offset = config_->vertex_offset;

    ext_size = mem_size * EXT_RATIO / 100;
    main_size = mem_size - ext_size;

    num_vertices = main_size / sizeof(Vertex);
    main_size = num_vertices * sizeof(Vertex);
    // ext_size = mem_size - main_size;

    // vertex size are divided into two part
    // 1. every vertex has a small fixed number vertex table
    // 2. super vertex extend in/out nbs by 100

    cout << "INFO: vertex = " << mem_size << " bytes " << std::endl
         << "      vertex number in main = " << num_vertices << std::endl
         << "      total ext size = " << ext_size << std::endl;

    vtx_array = (Vertex *)(mem);
    ext = (char *)(mem + main_size);
    
    last_ext_offset = 0;
    pthread_spin_init(&ext_lock, 0);
}

VertexTable::~VertexTable() {
    cout << "Delete vertex table" << endl;
}

void VertexTable::init(GraphMeta * graph_meta) {
    for(uint64_t i = 0; i < num_vertices; i++) {
        // LCY: Can I init like this?
        memset(&vtx_array[i], 0, sizeof(Vertex)); 
    }
    memset(ext, 0, ext_size);

    //init graph meta
    graph_meta->v_array_off = offset;
    graph_meta->v_ext_off = offset + main_size;
    graph_meta->v_num = 0;
}

// alloc size in ext and return orig offset
uint64_t VertexTable::sync_alloc_ext(uint64_t size) {
    uint64_t orig;
    pthread_spin_lock(&ext_lock);
    orig = last_ext_offset;
    last_ext_offset += size;
    if(last_ext_offset >= ext_size) {
        cout << "Vertex Table ERROR: out of ext region." << endl;
        assert(last_ext_offset < ext_size);
    }
    pthread_spin_unlock(&ext_lock);
    return orig;
}

Vertex * VertexTable::insert(vid_t id) {
    uint32_t i_id = id.value();
    if(i_id > num_vertices) 
        cout << "Vertex Table ERROR: out of vertex array region." << endl;
    assert(i_id < num_vertices);
    vtx_array[i_id].id = id;
    return &vtx_array[i_id];
}

char * VertexTable::get_ext() {
    return ext;
}

Vertex * VertexTable::find(vid_t id) {
    Vertex * v = &vtx_array[id.value()];
    assert(v != nullptr);
    return v;
}

