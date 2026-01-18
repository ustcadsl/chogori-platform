/*
 * @Author: chunyuLi 
 * @Date: 2022-09-06 16:58:16 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-13 17:58:32
 */
#include "storage/edge.hpp"
#include "storage/data_store.hpp"

// using namespace std;

// EdgeTable::EdgeTable(RemoteBuffer * buf) : buf_(buf) {
//     config_ = Config::GetInstance();

//     mem = config_->edge_store;
//     mem_size= GiB2B(config_->global_edge_sz_gb);
//     offset = config_->edge_offset;

//     uint64_t ext_size = mem_size * EXT_RATIO / 100;
//     uint64_t main_size = mem_size - ext_size;

//     num_edges = main_size / sizeof(Edge);
//     main_size = num_edges * sizeof(Edge);
//     ext_size = mem_size - main_size;

//     cout << "INFO: edge = " << mem_size << " bytes " << std::endl
//          << "      edge number = " << num_edges << std::endl
//          << "      total ext size = " << ext_size << std::endl;

//     edge_array = (Edge *)(mem);
//     ext = (char *)(mem + main_size);

//     last_ext_offset = 0;
//     pthread_spin_init(&ext_lock, 0);
// }

// EdgeTable::~EdgeTable() {
//     cout << "Delete edgetable" << endl;
// }

// void EdgeTable::init(GraphMeta * graph_meta) {
//     for(int i = 0; i < num_edges; ++i) {
//         // LCY: Can I init like this?
//         memset(&edge_array[i], 0, sizeof(Edge)); 
//     }
//     memset(ext, 0, ext_size);

//     // init graph meta
//     graph_meta->e_array_off = offset;
//     graph_meta->e_ext_off = offset + main_size;
//     graph_meta->e_num = 0;
// }

// char* EdgeTable::get_ext() {
//     return ext;
// }

// Edge * EdgeTable::insert(eid_t id) {
//     int v_id = id.tmp_hash(num_edges);
//     if(v_id > num_edges)
//         cout << "EdgeTable ERROR: out of edge array region." << endl;
//     // cout << "e_id = " << bitset<64>(id.value()) << " hashed_id == " << v_id << " edge array id = " << edge_array[v_id].id.value() << endl;
//     // assert(edge_array[v_id].id.value() ==  0);
//     if(edge_array[v_id].id.value() != 0) {
//         // cout << "e_id = " << bitset<64>(id.value()) << " hashed_id == " << v_id << " edge array id = " << edge_array[v_id].id.value() << endl;
//     }
//     edge_array[v_id].id = id;
//     return &edge_array[v_id];
// }

// uint64_t EdgeTable::sync_alloc_ext(uint64_t size) {
//     uint64_t orig;
//     pthread_spin_lock(&ext_lock);
//     orig = last_ext_offset;
//     last_ext_offset += size;
//     if(last_ext_offset >= ext_size) {
//         cout << "EdgeTable ERROR: out of ext region. " << endl;
//         assert(last_ext_offset < ext_size);
//     }
//     pthread_spin_unlock(&ext_lock);
//     return orig;
// }
