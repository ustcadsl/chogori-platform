/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include "storage/data_store.hpp"
#include "storage/mpi_snapshot.hpp"
#include "storage/snapshot_func.hpp"

DataStore::DataStore(Node & node, AbstractIdMapper * id_mapper, RemoteBuffer * buf): node_(node), id_mapper_(id_mapper), remote_buffer_(buf) {
    // TODO(big) new datastore both in remote and local server
    config_ = Config::GetInstance();
    v_table_ = NULL;
    vpstore_ = NULL;
    epstore_ = NULL;
}

DataStore::~DataStore() {
    #ifdef PRINT_MEM_USAGE
        vpstore_->print_mem_usage();
        epstore_->print_mem_usage();
    #endif
    delete v_table_;
    delete vpstore_;
    delete epstore_;
}

void DataStore::Init(vector<Node> & nodes) {
    // TODO(big) init vkv ekv vtable etable in remote and local 
    v_table_ = new VertexTable(remote_buffer_);
    vpstore_ = new VKVStore(remote_buffer_);
    epstore_ = new EKVStore(remote_buffer_);
    v_table_->init(&graph_meta_);
    vpstore_->init(&graph_meta_, nodes);
    epstore_->init(&graph_meta_, nodes);
}

GraphMeta DataStore::GetGraphMeta() {
    return graph_meta_;
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

void DataStore::LoadDataFromHDFS() {
    // MPISnapshot* snapshot = MPISnapshot::GetInstance();

    get_string_indexes();
    cout << "Remote Node " << node_.get_local_rank() << " get_string_indexes() DONE !" << endl;

    // if (!snapshot->TestRead("ekvstore")) {
    //     get_eplist();
    // }
    get_eplist();
    cout << "Remote Node " << node_.get_local_rank() << " Get_eplist() DONE !" << endl;
    #ifdef TEST_WITH_COUNT
        cout << "#ep = " << eplist.size() << endl;
    #endif
    get_vertices();
    cout << "Remote Node " << node_.get_local_rank() << " Get_vertices() DONE !" << endl; // tmp vertices have been prepared

    // if (!snapshot->TestRead("vkvstore")) {
    //     get_vplist();
    // }
    get_vplist();
    cout << "Remote Node " << node_.get_local_rank() << " Get_vplist() DONE !" << endl;
}

// void DataStore::ReadSnapshot() {
//     MPISnapshot* snapshot = MPISnapshot::GetInstance();

//     snapshot->ReadData("datastore_v_table", v_table_, ReadHashMapSerImpl);
//     snapshot->ReadData("datastore_e_table", e_table_, ReadHashMapSerImpl);

//     vpstore_->ReadSnapshot();
//     epstore_->ReadSnapshot();
// }

// void DataStore::WriteSnapshot() {
//     MPISnapshot* snapshot = MPISnapshot::GetInstance();

//     snapshot->WriteData("datastore_v_table", v_table_, WriteHashMapSerImpl);
//     snapshot->WriteData("datastore_e_table", e_table_, WriteHashMapSerImpl);

//     vpstore_->WriteSnapshot();
//     epstore_->WriteSnapshot();
// }

void DataStore::DataConverter() {
    // MPISnapshot* snapshot = MPISnapshot::GetInstance();
    // if (!snapshot->TestRead("datastore_v_table")) {
        for (int i = 0 ; i < vertices.size(); ++i) {
            Vertex* m_v = v_table_->insert(vertices[i]->id);
            memcpy((void *)m_v, (void *)vertices[i], sizeof(Vertex));
        }

        //==================insert vtx label to struct vertex
        for(auto it = vtx_label.begin(); it != vtx_label.end(); ++it) {
            Vertex* m_v = v_table_->find(vid_t(it->first));
            m_v->label = it->second;
        }

        // clean the vp_buf
        for (int i = 0 ; i < vp_buf.size(); i++) delete vp_buf[i];
        vector<vp_list*>().swap(vp_buf);
        vector<Vertex*>().swap(vertices);   
    // } else {
    //     if (node_.get_local_rank() == MASTER_RANK)
    //         printf("DataConverter snapshot->TestRead('datastore_v_table')\n");
    // }

    // LCY: tmply keep property insert funcitons
    //      but may need to buffer some hash meta(can reference FORD)  
    // if (!snapshot->TestRead("vkvstore")) {
        vpstore_->insert_vertex_properties(vplist);
        #ifdef TEST_WITH_COUNT
            cout << "#vp = " << vpstore_->vp_num_ << endl;
        #endif // DEBUG
        // clean the vp_list
        for (int i = 0 ; i < vplist.size(); i++) {
            // cout << vplist[i]->DebugString();  // TEST
            delete vplist[i];
        }
        vector<VProperty*>().swap(vplist);
    // }

    // if (!snapshot->TestRead("ekvstore")) {
        epstore_->insert_edge_properties(eplist);
        // clean the ep_list
        for (int i = 0 ; i < eplist.size(); i++) {
            // cout << eplist[i]->DebugString();  // TEST
            delete eplist[i];
        }
        vector<EProperty*>().swap(eplist);
    // }
}

void DataStore::get_string_indexes() {
    // TODO(big) string index cached in local
    hdfsFS fs = get_hdfs_fs();

    string el_path = config_->HDFS_INDEX_PATH + "./edge_label";
    hdfsFile el_file = get_r_handle(el_path.c_str(), fs);
    LineReader el_reader(fs, el_file);
    while (true) {
        el_reader.read_line();
        if (!el_reader.eof()) {
            char * line = el_reader.get_line();
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
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, el_file);

    string epk_path = config_->HDFS_INDEX_PATH + "./edge_property_index";
    hdfsFile epk_file = get_r_handle(epk_path.c_str(), fs);
    LineReader epk_reader(fs, epk_file);
    while (true) {
        epk_reader.read_line();
        if (!epk_reader.eof()) {
            char * line = epk_reader.get_line();
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
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, epk_file);

    string vl_path = config_->HDFS_INDEX_PATH + "./vtx_label";
    hdfsFile vl_file = get_r_handle(vl_path.c_str(), fs);
    LineReader vl_reader(fs, vl_file);
    while (true) {
        vl_reader.read_line();
        if (!vl_reader.eof()) {
            char * line = vl_reader.get_line();
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
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, vl_file);

    string vpk_path = config_->HDFS_INDEX_PATH + "./vtx_property_index";
    hdfsFile vpk_file = get_r_handle(vpk_path.c_str(), fs);
    LineReader vpk_reader(fs, vpk_file);
    while (true) {
        vpk_reader.read_line();
        if (!vpk_reader.eof()) {
            char * line = vpk_reader.get_line();
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
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, vpk_file);
    hdfsDisconnect(fs);
}

void DataStore::get_vertices() {
    // MPISnapshot* snapshot = MPISnapshot::GetInstance();
    // // break if the vtxs has already been finished.
    // if (snapshot->TestRead("datastore_v_table")) {
    //     if (node_.get_local_rank() == MASTER_RANK)
    //         printf("get_vertices snapshot->TestRead('datastore_v_table')\n");
    //     return;
    // }

    // // check path + arrangement
    const char * indir = config_->HDFS_VTX_SUBFOLDER.c_str();

    if (node_.get_local_rank() == MASTER_RANK) {
        if (dir_check(indir) == -1)
            exit(-1);
    }

    if (node_.get_local_rank() == MASTER_RANK) {
        vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
        // master_scatter(node_, false, arrangement);
        vector<string>& assigned_splits = arrangement[0];
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_vertices(it->c_str());
    } 
    else {
        vector<string> assigned_splits;
        slave_scatter(node_, false, assigned_splits);
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_vertices(it->c_str());
    }
}

void DataStore::load_vertices(const char* inpath) {
    // TODO(big) load vertex from remote server
    hdfsFS fs = get_hdfs_fs();
    hdfsFile in = get_r_handle(inpath, fs);
    LineReader reader(fs, in);
    while (true) {
        reader.read_line();
        if (!reader.eof()) {
            Vertex * v = to_vertex(reader.get_line());
            vertices.push_back(v);
            graph_meta_.v_num++;
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, in);
    hdfsDisconnect(fs);
}

// TODO(big) all these functions are used in load data, local or remote?
// Format
// vid [\t] #in_nbs [\t] nb1 [space] nb2 [space] ... #out_nbs [\t] nb1 [space] nb2 [space] ...
Vertex* DataStore::to_vertex(char* line) {
    Vertex * v = new Vertex;

    char * pch;
    pch = strtok(line, "\t");

    vid_t vid(atoi(pch));
    v->id = vid;

    pch = strtok(NULL, "\t");
    int num_in_nbs = atoi(pch);
    int in_ext_num = num_in_nbs - IN_NBS;
    uint64_t in_ext_offset;
    Nbs_pair* m_in_ext;
    if (in_ext_num > 0) {
        in_ext_offset = v_table_->sync_alloc_ext(sizeof(Nbs_pair)*in_ext_num);
        v->ext_in_nbs_ptr = ptr_t(sizeof(Nbs_pair)*in_ext_num, in_ext_offset);
        m_in_ext = (Nbs_pair*)(v_table_->get_ext() + in_ext_offset);
    }
    for (int i = 0 ; i < num_in_nbs; ++i) {
        pch = strtok(NULL, " ");
        int nb_vid = atoi(pch);
        eid_t nb_eid(nb_vid, vid.value());
        label_t nb_label = edges_label[nb_eid.value()];
        if (i < IN_NBS) {
            v->in_nbs[i].vid = nb_vid; 
            v->in_nbs[i].label = nb_label;
        }
        else {
            m_in_ext[i-IN_NBS].vid = nb_vid;
            m_in_ext[i-IN_NBS].label = nb_label;
        }
    }

    pch = strtok(NULL, "\t");
    int num_out_nbs = atoi(pch);
    int out_ext_num = num_out_nbs - OUT_NBS;
    uint64_t out_ext_offset;
    Nbs_pair* m_out_ext;
    if (out_ext_num > 0) {
        out_ext_offset = v_table_->sync_alloc_ext(sizeof(Nbs_pair)*out_ext_num);
        v->ext_out_nbs_ptr = ptr_t(sizeof(Nbs_pair)*out_ext_num, out_ext_offset);
        m_out_ext = (Nbs_pair *)(v_table_->get_ext() + out_ext_offset);
    }
    for (int i = 0 ; i < num_out_nbs; ++i) {
        pch = strtok(NULL, " ");
        int nb_vid = atoi(pch);
        eid_t nb_eid(vid.value(), nb_vid);
        label_t nb_label = edges_label[nb_eid.value()];
        if (i < OUT_NBS) {
            v->out_nbs[i].vid = nb_vid;
            v->out_nbs[i].label = nb_label;
        }
        else {
            m_out_ext[i-OUT_NBS].vid = nb_vid;
            m_out_ext[i-OUT_NBS].label = nb_label;
        }
    }
    return v;
}

void DataStore::get_vplist() {
    // check path + arrangement
    const char * indir = config_->HDFS_VP_SUBFOLDER.c_str();
    if (node_.get_local_rank() == MASTER_RANK) {
        if (dir_check(indir) == -1)
            exit(-1);
    }

    if (node_.get_local_rank() == MASTER_RANK) {
        vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
        // master_scatter(node_, false, arrangement);
        vector<string>& assigned_splits = arrangement[0];
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_vplist(it->c_str());
    } 
    // else {
    //     vector<string> assigned_splits;
    //     slave_scatter(node_, false, assigned_splits);
    //     // reading assigned splits (map)
    //     for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
    //         load_vplist(it->c_str());
    // }
}

void DataStore::load_vplist(const char* inpath) {
    hdfsFS fs = get_hdfs_fs();
    hdfsFile in = get_r_handle(inpath, fs);
    LineReader reader(fs, in);
    while (true) {
        reader.read_line();
        if (!reader.eof()) {
            to_vp(reader.get_line(), vplist, vp_buf);
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, in);
    hdfsDisconnect(fs);
}

// Format
// vid [\t] label[\t] [kid:value,kid:value,...]
void DataStore::to_vp(char* line, vector<VProperty*> & vplist, vector<vp_list*> & vp_buf) {
    VProperty * vp = new VProperty;
    vp_list * vpl = new vp_list;

    char * pch;
    pch = strtok(line, "\t");
    vid_t vid(atoi(pch));
    vp->id = vid;
    vpl->vid = vid;

    pch = strtok(NULL, "\t");
    label_t label = (label_t)atoi(pch);
    vtx_label[vid.value()] = label;

    pch = strtok(NULL, "");
    string s(pch);

    vector<string> kvpairs;
    Tool::splitWithEscape(s, "[],:", kvpairs);
    assert(kvpairs.size() % 2 == 0);
    for (int i = 0 ; i < kvpairs.size(); i += 2) {
        kv_pair p;
        Tool::get_kvpair(kvpairs[i], kvpairs[i+1], indexes.str2vptype[kvpairs[i]], p);
        V_KVpair v_pair;
        v_pair.key = vpid_t(vid, p.key);
        v_pair.value = p.value;

        // push to property_list of v
        vp->plist.push_back(v_pair);

        // for property index on v
        vpl->pkeys.push_back((label_t)p.key);
    }

    // sort p_list in vertex
    sort(vpl->pkeys.begin(), vpl->pkeys.end());
    vplist.push_back(vp);
    vp_buf.push_back(vpl);

    // cout << "####### " << vp->DebugString(); //DEBUG
}

void DataStore::get_eplist() {
    // check path + arrangement
    const char * indir = config_->HDFS_EP_SUBFOLDER.c_str();
    if (node_.get_local_rank() == MASTER_RANK) {
        if (dir_check(indir) == -1)
            exit(-1);
    }

    if (node_.get_local_rank() == MASTER_RANK) {
        vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
        // master_scatter(node_, false, arrangement);
        vector<string>& assigned_splits = arrangement[0];
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_eplist(it->c_str());
    } 
    // else {
    //     vector<string> assigned_splits;
    //     slave_scatter(node_, false, assigned_splits);
    //     // reading assigned splits (map)
    //     for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
    //         load_eplist(it->c_str());
    // }
}

void DataStore::load_eplist(const char* inpath) {
    hdfsFS fs = get_hdfs_fs();
    hdfsFile in = get_r_handle(inpath, fs);
    LineReader reader(fs, in);
    while (true) {
        reader.read_line();
        if (!reader.eof()) {
            to_ep(reader.get_line(), eplist);
         } else {
            break;
         }
    }
    hdfsCloseFile(fs, in);
    hdfsDisconnect(fs);
}

// Format
// in-v[\t] out-v[\t] label[\t] [kid:value,kid:value,...]
void DataStore::to_ep(char* line, vector<EProperty*> & eplist) {
    EProperty * ep = new EProperty;
    // TODO(big) which is out and in still need to check
    uint64_t atoi_time = timer::get_usec();
    char * pch;
    pch = strtok(line, "\t");
    int in_v = atoi(pch);
    pch = strtok(NULL, "\t");
    int out_v = atoi(pch);

    eid_t eid(in_v, out_v);
    ep->id = eid;

    pch = strtok(NULL, "\t");
    label_t label = (label_t)atoi(pch);
    edges_label[eid.value()] = label;

    pch = strtok(NULL, "");
    string s(pch);

    vector<string> kvpairs;
    Tool::splitWithEscape(s, "[],:", kvpairs);
    assert(kvpairs.size() % 2 == 0);

    for (int i = 0 ; i < kvpairs.size(); i += 2) {
        kv_pair p;
        Tool::get_kvpair(kvpairs[i], kvpairs[i+1], indexes.str2eptype[kvpairs[i]], p);

        E_KVpair e_pair;
        e_pair.key = epid_t(in_v, out_v, p.key);
        e_pair.value = p.value;

        ep->plist.push_back(e_pair);
    }
    if(kvpairs.size() > 0)
        eplist.push_back(ep);
}
