/*
 * @Author: chunyuLi 
 * @Date: 2022-09-12 14:26:18 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-13 17:54:49
 */


#include "storage/metadata.hpp"
#include "storage/mpi_snapshot.hpp"
#include "storage/snapshot_func.hpp"

MetaData::MetaData(Node & node, AbstractIdMapper * id_mapper, Buffer * buf): node_(node), id_mapper_(id_mapper), buffer_(buf) {
    // TODO(big) new MetaData both in remote and local server
    config_ = Config::GetInstance();
    vpstore_ = NULL;
    epstore_ = NULL;    
}

MetaData::~MetaData() {
    std::cout << "Delete metadata" << std::endl;    
    delete vpstore_;
    delete epstore_;
    #ifdef TEST_WITH_COUNT
        resultF.close();
    #endif // DEBUG
}

void MetaData::Init(vector<Node> & nodes, GraphMeta& graphmeta) {
    // TODO(big) init vkv ekv vtable etable in remote and local 
    // TODO(big) get v/e meta from remote
    v_array_off_ = graphmeta.v_array_off;
    v_ext_off_ = graphmeta.v_ext_off;
    v_num_ = graphmeta.v_num;
    
    vpstore_ = new VKVStore_Local(buffer_);
    epstore_ = new EKVStore_Local(buffer_);
    vpstore_->init(nodes, graphmeta.vp_off, graphmeta.vp_num_slots, graphmeta.vp_num_buckets);
    epstore_->init(nodes, graphmeta.ep_off, graphmeta.ep_num_slots, graphmeta.ep_num_buckets);

#ifdef TEST_WITH_COUNT
    InitCounter();
    access_counter_.resize(9);
    access_list_.clear(); 
    resultF.open("result.csv", ios_base::app);
    for(auto str : accessType)
        resultF << str << ",";
#endif
}

void MetaData::BuildVertexIndex(vid_t v_id, Vertex& v) {
    if(vertex_index.find(v_id.value()) == vertex_index.end()) {
        // TODO: code to limit size of vertex index
        vertex_index[v_id.value()].label = v.label;
        vertex_index[v_id.value()].in_nbs_ptr = v.ext_in_nbs_ptr;
        vertex_index[v_id.value()].out_nbs_ptr = v.ext_out_nbs_ptr;
    }
}
// index format
// string \t index [int]
/*
 *    unordered_map<string, label_t> str2el; //map to edge_label
 *    unordered_map<label_t, string> el2str;
 *    unordered_map<string, label_t> str2epk; //map to edge's property key
 *    unordered_map<label_t, string> epk2str;
 *    unordered_map<string, label_t> str2vl; //map to vtx_label
 *    unordered_map<label_t, string> vl2str;
 *    unordered_map<string, label_t> str2vpk; //map to vtx's property key
 *    unordered_map<label_t, string> vpk2str;
 */
void MetaData::GetVertex(int tid, vid_t v_id, Vertex& v) {
    char * send_buf = buffer_->GetSendBuf(tid);
    uint64_t v_off = v_array_off_ + v_id.value() * sizeof(Vertex); // LCY: check vid start from

    RDMA &rdma = RDMA::get_rdma();
    rdma.dev->RdmaRead(tid, REMOTE_NID, send_buf, sizeof(Vertex), v_off);

    memcpy(&v, send_buf, sizeof(Vertex));

    BuildVertexIndex(v_id, v);

    #ifdef TEST_WITH_COUNT
        // RecordVtx(sizeof(Vertex));
        RecordAccess(ACCESS_T::VTX);
    #endif // DEBUG
    return;
}

void MetaData::GetVertexBatch(int tid, vector<vid_t> v_ids, vector<Vertex>& vertice) {
    char * send_buf = buffer_->GetSendBuf(tid);
    vector<uint64_t> off;
    for(auto vid: v_ids) {
        off.push_back(v_array_off_ + vid.value() * sizeof(Vertex));
    }

    RDMA &rdma = RDMA::get_rdma();
    int len, begin = 0;
    int remain = off.size();   
    while(remain > 0) {
        len = remain > MTU/sizeof(Vertex) ? MTU/sizeof(Vertex): remain;

        rdma.dev->RdmaReadBatch(tid, REMOTE_NID, send_buf, sizeof(Vertex), off, begin, len);

        for(int i = 0; i < len; ++i) {
            Vertex tmp;
            memcpy(&tmp, send_buf + i * sizeof(Vertex), sizeof(Vertex));
            vertice.emplace_back(tmp);
            BuildVertexIndex(tmp.id, tmp);
        }

        remain -= len;
        begin += len;

        #ifdef TEST_WITH_COUNT
            RecordAccess(ACCESS_T::VTX);
        #endif
    }    
    return;
}

int MetaData::GetInNbs(int tid, vid_t v, vector<Nbs_pair>& in_nbs) {
    int size = 0;
    Vertex tmpv;
    auto res = vertex_index.find(v.value());
    if(res == vertex_index.end()) {
        GetVertex(tid, v, tmpv);

        // NEW: store all nbs in ext
        // for(size = 0; size < IN_NBS; ++size) {
        //     if(tmpv.in_nbs[size].vid == 0)
        //         break;
        //     in_nbs.push_back(tmpv.in_nbs[size]);
        // }
    } 
    else {
        tmpv.ext_in_nbs_ptr = res->second.in_nbs_ptr; 
        tmpv.ext_out_nbs_ptr = res->second.out_nbs_ptr; 
    }

    if(tmpv.ext_in_nbs_ptr.size != 0) {
        // has ext in nbs
        char * send_buf = buffer_->GetSendBuf(tid);
        uint64_t off = v_ext_off_ + tmpv.ext_in_nbs_ptr.off;
        uint64_t sz = tmpv.ext_in_nbs_ptr.size;
        
        RDMA &rdma = RDMA::get_rdma();
        rdma.dev->RdmaRead(tid, REMOTE_NID, send_buf, sz, off);
        
        int num = sz/sizeof(Nbs_pair);
        Nbs_pair* recv = (Nbs_pair*)send_buf;
        for(int i = 0; i<num; ++i) {
            in_nbs.push_back(recv[i]);
            size++;
        }

        #ifdef TEST_WITH_COUNT
            RecordAccess(ACCESS_T::INNBS);
            // RecordVin(num*sizeof(Nbs_pair));
        #endif
    }
#ifdef DEBUG
    std::cout << "In Nbs = ";
    for(auto it = in_nbs.begin(); it != in_nbs.end(); ++it) {
         std::cout << "(" << it->vid.value() << "," << it->label << ") ";
    }
    std::cout << std::endl;
#endif
    return size;
};

int MetaData::GetOutNbs(int tid, vid_t v, vector<Nbs_pair>& out_nbs) {
    int size = 0;
    Vertex tmpv;
    auto res = vertex_index.find(v.value());
    if(res == vertex_index.end()) {
        GetVertex(tid, v, tmpv);
    // for(size = 0; size < OUT_NBS; ++size) {
    //     if(v.out_nbs[size].vid == 0) 
    //         break; // TODO(big): check that no vid is 0
    //     out_nbs.push_back(v.out_nbs[size]);
    // }
    }
    else {
        tmpv.ext_in_nbs_ptr = res->second.in_nbs_ptr;
        tmpv.ext_out_nbs_ptr = res->second.out_nbs_ptr;
    }
    if(tmpv.ext_out_nbs_ptr.size != 0) {
        // has ext out nbs
        char * send_buf = buffer_->GetSendBuf(tid);
        uint64_t off = v_ext_off_ + tmpv.ext_out_nbs_ptr.off;
        uint64_t sz = tmpv.ext_out_nbs_ptr.size;
        
        RDMA &rdma = RDMA::get_rdma();
        rdma.dev->RdmaRead(tid, REMOTE_NID, send_buf, sz, off);
        
        int num = sz/sizeof(Nbs_pair);
        Nbs_pair * recv = (Nbs_pair*)send_buf;
        for(int i = 0; i<num; ++i) {
            out_nbs.push_back(recv[i]);
            size++;
        }

        #ifdef TEST_WITH_COUNT
            // RecordVout(num*sizeof(Nbs_pair));
            RecordAccess(ACCESS_T::OUTNBS);
        #endif
    }
#ifdef DEBUG
    std::cout << "Out Nbs = ";
    for(auto it = out_nbs.begin(); it != out_nbs.end(); ++it) {
         std::cout << "(" << it->vid.value() << "," << it->label << ") ";
    }
    std::cout << std::endl;
#endif
    return size;
};

void MetaData::GetAllVertices(int tid, vector<vid_t> & vid_list) {
    uint64_t buf_size = buffer_->GetSendBufSize();
    int per_read_num = buf_size/sizeof(Vertex);
    int count = 0;
    int remain = v_num_;
    while(remain > 0) {
        int read_sz = remain > per_read_num ? per_read_num : remain;
        
        char* send_buf = buffer_->GetSendBuf(tid);
        uint64_t off = v_array_off_ + count * sizeof(Vertex);
        uint64_t size = read_sz * sizeof(Vertex);

        RDMA &rdma = RDMA::get_rdma();
        rdma.dev->RdmaRead(tid, node_.get_local_rank(), send_buf, size, off);
        Vertex* v = (Vertex *)send_buf;

        for(int i = 0; i < read_sz; ++i) {
            BuildVertexIndex(v[i].id, v[i]);
		    vid_list.push_back(v[i].id);
        }

        count += read_sz;
        remain -= read_sz;
    }

    #ifdef TEST_WITH_COUNT
        // RecordVtx(sizeof(Vertex)*v_num_);
        PrintIndexMem();
    #endif
}

void MetaData::GetAllEdges(int tid, vector<eid_t> & eid_list) {
    for(auto it = vertex_index.begin(); it != vertex_index.end(); ++it) {
        vid_t vid;
        uint2vid_t(it->first,vid);
        vector<Nbs_pair> out_nbs;
        int nbs_sz = GetOutNbs(tid, vid, out_nbs);
        for(int j = 0; j < nbs_sz; ++j) {
            eid_list.push_back(eid_t(it->first, out_nbs[j].vid.value()));
        }
    }
}

bool MetaData::VPKeyIsLocal(vpid_t vp_id) {
    return false;
}

bool MetaData::EPKeyIsLocal(epid_t ep_id) {
    return false;
}

bool MetaData::GetPropertyForVertex(int tid, vpid_t vp_id, value_t & val) {
    vpstore_->get_property_remote(tid, REMOTE_NID, vp_id.value(), val);

    #ifdef TEST_WITH_COUNT
        // RecordVp(val.content.size());
        RecordAccess(ACCESS_T::VP);
    #endif
    if (val.content.size())
        return true;
    return false;
}

bool MetaData::GetPropertyForEdge(int tid, epid_t ep_id, value_t & val) {
    epstore_->get_property_remote(tid, id_mapper_->GetMachineIdForEProperty(ep_id), ep_id.value(), val);


    #ifdef TEST_WITH_COUNT
        // RecordEp(val.content.size());
        RecordAccess(ACCESS_T::EP);
    #endif
    if (val.content.size())
        return true;
    return false;
}

bool MetaData::GetLabelForVertex(int tid, vid_t vid, label_t & label) {
    auto res = vertex_index.find(vid.value());
    if(res != vertex_index.end()) {
        label = res->second.label;
    }
    else {
        Vertex v;
        GetVertex(tid, vid, v);
        label = v.label;
    }

    #ifdef TEST_WITH_COUNT
        // RecordAccess(ACCESS_T::VLABEL);
    #endif // DEBUG
    return true;
}

bool MetaData::GetLabelForEdge(int tid, eid_t eid, label_t & label) {
    vector<Nbs_pair> out_nbs;
    int nbs_sz = GetOutNbs(tid, eid.in_v, out_nbs);
    for(int i = 0; i < nbs_sz; ++i) {
        if(out_nbs[i].vid == eid.out_v) {
            label = out_nbs[i].label;
            break;
        }
    }
    #ifdef TEST_WITH_COUNT
        // RecordAccess(ACCESS_T::ELABEL);
    #endif // DEBUG
    return true;
}

int MetaData::GetMachineIdForVertex(vid_t v_id) {
    return id_mapper_->GetMachineIdForVertex(v_id);
}

int MetaData::GetMachineIdForEdge(eid_t e_id) {
    return id_mapper_->GetMachineIdForEdge(e_id);
}

void MetaData::GetNameFromIndex(Index_T type, label_t id, string & str) {
    unordered_map<label_t, string>::const_iterator itr;

    switch (type) {
        case Index_T::E_LABEL:
            itr = indexes.el2str.find(id);
            if (itr == indexes.el2str.end()) {
                return;
            } else {
                str = itr->second;
            }
            break;
        case Index_T::E_PROPERTY:
            itr = indexes.epk2str.find(id);
            if (itr == indexes.epk2str.end()) {
                return;
            } else {
                str = itr->second;
            }
            break;
        case Index_T::V_LABEL:
            itr = indexes.vl2str.find(id);
            if (itr == indexes.vl2str.end()) {
                return;
            } else {
                str = itr->second;
            }
            break;
        case Index_T::V_PROPERTY:
            itr = indexes.vpk2str.find(id);
            if (itr == indexes.vpk2str.end()) {
                return;
            } else {
                str = itr->second;
            }
            break;
        default:
            return;
    }
}

void MetaData::InsertAggData(agg_t key, vector<value_t> & data) {
    lock_guard<mutex> lock(agg_mutex);

    unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
    if (itr == agg_data_table.end()) {
        // Not Found, insert
        agg_data_table.insert(pair<agg_t, vector<value_t>>(key, data));
    } else {
        agg_data_table.at(key).insert(agg_data_table.at(key).end(), data.begin(), data.end());
    }
}

void MetaData::GetAggData(agg_t key, vector<value_t> & data) {
    lock_guard<mutex> lock(agg_mutex);

    unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
    if (itr != agg_data_table.end()) {
        data = itr->second;
    }
}

void MetaData::DeleteAggData(agg_t key) {
    lock_guard<mutex> lock(agg_mutex);

    unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
    if (itr != agg_data_table.end()) {
        agg_data_table.erase(itr);
    }
}

void MetaData::get_string_indexes() {
    // string index cached in local
    const string INDEX_PATH = "./data/sf0.1/index/";
    ifstream file;
    string string_line;

    string el_path = INDEX_PATH + "./edge_label";
    file.open(el_path);
    if(!file.is_open()) {
        std::cout << "Error when open edge label index file" << std::endl;
        return;
    }
    while(getline(file, string_line)) {
        char *line = const_cast<char*>(string_line.c_str());
        char * pch;
        pch = strtok(line, "\t");
        string key(pch);
        pch = strtok(NULL, "\t");
        label_t id = atoi(pch);

        // both string and ID are unique
        assert(indexes.str2el.find(key) == indexes.str2el.end());
        assert(indexes.el2str.find(id) == indexes.el2str.end());

        indexes.str2el[key] = id;
        indexes.el2str[id] = key;
    }
    file.close();

    string epk_path = INDEX_PATH + "./edge_property_index";
    file.open(epk_path);
    if(!file.is_open()) {
        std::cout << "Error when open edge property index file" << std::endl;
        return;
    }
    while (getline(file, string_line)) {
        char * line = const_cast<char*>(string_line.c_str());
        char * pch;
        pch = strtok(line, "\t");
        string key(pch);
        pch = strtok(NULL, "\t");
        label_t id = atoi(pch);
        pch = strtok(NULL, "\t");
        indexes.str2eptype[to_string(id)] = atoi(pch);

        // both string and ID are unique
        assert(indexes.str2epk.find(key) == indexes.str2epk.end());
        assert(indexes.epk2str.find(id) == indexes.epk2str.end());

        indexes.str2epk[key] = id;
        indexes.epk2str[id] = key;
    }
    file.close();

    string vl_path = INDEX_PATH + "./vtx_label";
    file.open(vl_path);
    if(!file.is_open()) {
        std::cout << "Error when open vertex label index file" << std::endl;
        return;
    }
    while (getline(file, string_line)) {
        char * line = const_cast<char*>(string_line.c_str());
        char * pch;
        pch = strtok(line, "\t");
        string key(pch);
        pch = strtok(NULL, "\t");
        label_t id = atoi(pch);

        // both string and ID are unique
        assert(indexes.str2vl.find(key) == indexes.str2vl.end());
        assert(indexes.vl2str.find(id) == indexes.vl2str.end());

        indexes.str2vl[key] = id;
        indexes.vl2str[id] = key;
    }
    file.close();

    string vpk_path = INDEX_PATH + "./vtx_property_index";
    file.open(vpk_path);
    if(!file.is_open()) {
        std::cout << "Error when open vertex property index file" << std::endl;
        return;
    }
    while (getline(file, string_line)) {
        char * line = const_cast<char*>(string_line.c_str());
        char * pch;
        pch = strtok(line, "\t");
        string key(pch);
        pch = strtok(NULL, "\t");
        label_t id = atoi(pch);
        pch = strtok(NULL, "\t");
        indexes.str2vptype[to_string(id)] = atoi(pch);

        // both string and ID are unique
        assert(indexes.str2vpk.find(key) == indexes.str2vpk.end());
        assert(indexes.vpk2str.find(id) == indexes.vpk2str.end());

        indexes.str2vpk[key] = id;
        indexes.vpk2str[id] = key;
    }
    file.close();
}

void MetaData::get_schema() {
    const string INDEX_PATH = "./data/sf0.1/index/";
    ifstream file;
    string string_line;

    string el_path = INDEX_PATH + "./vtx_schema";
    file.open(el_path);
    if(!file.is_open()) {
        std::cout << "Error when open edge label index file" << std::endl;
        return;
    }
    while(getline(file, string_line)) {
        char *line = const_cast<char*>(string_line.c_str());
        char * pch;
        pch = strtok(line, " ");
        string key(pch);
        label_t label = indexes.str2vl[key];
        vtx_schemas[label] = vector<label_t>{};

        pch = strtok(NULL, " ");
        int num = atoi(pch);

        for(int i = 0; i < num; ++i) {
            pch  = strtok(NULL, " ");
            string property(pch);
            vtx_schemas[label].push_back(indexes.str2vpk[property]);            
        }
    }

    file.close();

    el_path = INDEX_PATH + "./edge_schema";
    file.open(el_path);
    if(!file.is_open()) {
        std::cout << "Error when open edge label index file" << std::endl;
        return;
    }
    while(getline(file, string_line)) {
        char *line = const_cast<char*>(string_line.c_str());
        char * pch;
        pch = strtok(line, " ");
        string key(pch);
        label_t label = indexes.str2el[key];
        edge_schemas[label] = vector<label_t>{};
        pch = strtok(NULL, " ");
        int num = atoi(pch);

        for(int i = 0; i < num; ++i) {
            pch  = strtok(NULL, " ");
            string property(pch);
            edge_schemas[label].push_back(indexes.str2epk[property]);            
        }
    }
    #ifdef DEBUG
    for(auto it = edge_schemas.begin(); it != edge_schemas.end(); ++it) {
        cout << "Lable id = " << it->first << " Property id = ";
        vector<label_t>& schema = it->second;
        for(auto i = schema.begin(); i != schema.end(); ++i) {
            cout << *i << " "; 
        }
        cout << endl;
    }
    #endif // DEBUG
    file.close();
}

void MetaData::GetVPList(label_t label, vector<label_t>& vp_list) {
    vp_list.insert(vp_list.begin(), vtx_schemas[label].begin(), vtx_schemas[label].end());
    #ifdef TEST_WITH_COUNT
        // RecordAccess(ACCESS_T::VPList);
    #endif
    return;
}

void MetaData::GetEPList(label_t label, vector<label_t>& ep_list) {

    ep_list.insert(ep_list.begin(), edge_schemas[label].begin(), edge_schemas[label].end());
    #ifdef TEST_WITH_COUNT
        // RecordAccess(ACCESS_T::EPList);
    #endif
    return;
}

#ifdef TEST_WITH_COUNT
// test with counter functions
void MetaData::InitCounter() {
    vtx_counter_ = 0;
    vtx_sizes_.clear();
    edg_counter_ = 0;
    edg_sizes_.clear();
    vp_counter_ = 0;
    vp_sizes_.clear();
    ep_counter_ = 0;
    ep_sizes_.clear();
    vin_nbs_counter_ = 0;
    vin_nbs_sizes_.clear();
    vout_nbs_counter_ = 0;
    vout_nbs_sizes_.clear();
}

void MetaData::ResetTime() {
    total_time_ = 0;
    has_time_ = 0;
    traversal_time_ = 0;    
    for(auto it = access_counter_.begin(); it != access_counter_.end(); ++it) {
            *it = 0;
    }
    return;
}

void MetaData::AggTime(double time) {
    total_time_ += time;
    return;
}

void MetaData::GetTraselTime(double time) {
    traversal_time_ += time;
    return;
}

void MetaData::GetHasTime(double time) {
    has_time_ += time;
    return;
}

void MetaData::PrintTimeRatio() {
    std::cout << "Has Ratio = " << has_time_/total_time_ << std::endl;
    std::cout << "Traversal Ratio = " << traversal_time_/total_time_ << std::endl;
    std::cout << "Total time = " << total_time_ << std::endl;

    ofstream resultf;
    resultf.open("result.csv", ios::app);
    for(auto it = access_counter_.begin(); it != access_counter_.end(); ++it) {
            resultf << *it << ",";
    }
    // for(ACCESS_T type : access_list_) {
    //     resultf << accessType[static_cast<int>(type)] << " ";
    // }
    resultf << "," << total_time_/1000.0 << "\n";
    return;
}

void MetaData::RecordVtx(int size) {
    vtx_counter_ += 1;
    vtx_sizes_.push_back(size);
}
void MetaData::RecordVp(int size) {
    vp_counter_ += 1;
    vp_sizes_.push_back(size);
}
void MetaData::RecordEp(int size) {
    ep_counter_ += 1;
    ep_sizes_.push_back(size);
}
void MetaData::RecordVin(int size) {
    vin_nbs_counter_ += 1;
    vin_nbs_sizes_.push_back(size);
}
void MetaData::RecordVout(int size) {
    vout_nbs_counter_ += 1;
    vout_nbs_sizes_.push_back(size);
}
void MetaData::RecordVtxExt(int size) {
    vtx_ext_counter_ += 1;
    vtx_sizes_.push_back(size);
}
void MetaData::RecordAccess(ACCESS_T type) {
    access_counter_[static_cast<int>(type)]++;
    if(access_list_.empty())
        access_list_.push_back(type);
    else {
        if(access_list_.back() != type)
            access_list_.push_back(type);
    }
}
void MetaData::PrintIndexMem() {
    size_t total = sizeof(vertex_index) + vertex_index.size() *(sizeof(decltype(vertex_index)::key_type) + sizeof(decltype(vertex_index)::value_type));
    std::cout << "sizeof integer = " << sizeof(int) << endl;
    std::cout << "Vertex Index size = " << total << endl;
}

void MetaData::PrintCounter() {
    if(vtx_counter_ || vtx_ext_counter_ || edg_counter_ || vin_nbs_counter_ || vout_nbs_counter_ || vp_counter_ || ep_counter_ )
        std::cout << "============================================" << std::endl;

    if(vtx_counter_) {
        std::cout << "Visit vertex table " << vtx_counter_ << std::endl;
        long total = 0;
        for(auto it = vtx_sizes_.begin(); it != vtx_sizes_.end(); ++it) {
            total += *it;
            // std::cout << *it << " ";
        }
        std::cout << "Visit vertex size " << total << std::endl;
    }

    if(vtx_ext_counter_) {
        std::cout << "Visit vtx extension " << vtx_ext_counter_ << std::endl;
        long total = 0;
        for(auto it = vtx_ext_sizes_.begin(); it != vtx_ext_sizes_.end(); ++it) {
            total += *it;
        }
        std::cout << "Visit vtx extension size " << total << std::endl;
    }
    
    if(edg_counter_) {
        std::cout << "Visit edge table " << edg_counter_ << std::endl;
        long total = 0;
        for(auto it = edg_sizes_.begin(); it != edg_sizes_.end(); ++it) {
            total += *it;
            // std::cout << *it << " ";
        }
        std::cout << "Visit edge size " << total << std::endl;
    }

    if(vin_nbs_counter_) {
        std::cout << "Visit vin nbs " << vin_nbs_counter_ << std::endl;
        long total = 0;
        for(auto it = vin_nbs_sizes_.begin(); it != vin_nbs_sizes_.end(); ++it) {
            total += *it;
            // std::cout << *it << " ";
        }
        std::cout << "Visit vin nbs size " << total << std::endl;
    }

    if(vout_nbs_counter_) {
        std::cout << "Visit vout nbs " << vout_nbs_counter_ << std::endl;
        long total = 0;
        for(auto it = vout_nbs_sizes_.begin(); it != vout_nbs_sizes_.end(); ++it) {
            total += *it;
            // std::cout << *it << " ";
        }
        std::cout << "Visit vout nbs size " << total << std::endl;       
    }

    if(vp_counter_) {
        std::cout << "Visit vp table " << vp_counter_ << std::endl;
        long total = 0;
        for(auto it = vp_sizes_.begin(); it != vp_sizes_.end(); ++it) {
            total += *it;
            // std::cout << *it << " ";
        }
        std::cout << "Visit vp size " << total << std::endl;
        // std::cout << std::endl;
    }

    if(ep_counter_) {
        std::cout << "Visit ep table " << ep_counter_ << std::endl;
        long total = 0;
        for(auto it = ep_sizes_.begin(); it != ep_sizes_.end(); ++it) {
            total += *it;
            // std::cout << *it << " ";
        }
        std::cout << "Visit ep size " << total << std::endl;
        // std::cout << std::endl;
    }
    if(vtx_counter_ || vtx_ext_counter_ || edg_counter_ || vin_nbs_counter_ || vout_nbs_counter_ || vp_counter_ || ep_counter_ )
        std::cout << "============================================" << std::endl << std::endl;
}
#endif
