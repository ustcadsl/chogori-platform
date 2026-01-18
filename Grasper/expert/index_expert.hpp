/* Copyright 2019 Husky Data Lab, CUHK

Authors: Aaron Li (cjli@cse.cuhk.edu.hk)
         Nick Fang (jcfang6@cse.cuhk.edu.hk)
*/

#ifndef INDEX_EXPERT_HPP_
#define INDEX_EXPERT_HPP_

#include <string>
#include <vector>
#include <algorithm>

#include "expert/abstract_expert.hpp"
#include "core/message.hpp"
#include "core/index_store.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "storage/layout.hpp"
#include "storage/metadata.hpp"
#include "utils/tool.hpp"

class IndexExpert : public AbstractExpert {
 public:
    IndexExpert(int id, MetaData * metadata, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity, IndexStore * index_store) : AbstractExpert(id, metadata, core_affinity), num_thread_(num_thread), mailbox_(mailbox), index_store_(index_store), type_(EXPERT_T::HAS) {}

    void process(const vector<Expert_Object> & expert_objs, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = expert_objs[m.step];

        // Get Params
        assert(expert_obj.params.size() == 2);  // make sure input format
        Element_T inType = (Element_T) Tool::value_t2int(expert_obj.params[0]);
        int pid = Tool::value_t2int(expert_obj.params[1]);

        bool enabled = false;
        switch (inType) {
            case Element_T::VERTEX:
                enabled = BuildIndexVtx(tid, pid);
                break;
            case Element_T::EDGE:
                enabled = BuildIndexEdge(tid, pid);
                break;
            default:
                cout << "Wrong inType" << endl;
                return;
        }

        string ena = (enabled? "enabled":"disabled");
        string s = "Index is " + ena + " in node" + to_string(m.recver_nid);
        std::cout << "Index size = " << index_store_->GetIndexSize() / 1024 << " KB" << std::endl;
        value_t v;
        Tool::str2str(s, v);
        msg.data.emplace_back(history_t(), vector<value_t>{v});
        // Create Message
        vector<Message> msg_vec;
        msg.CreateNextMsg(expert_objs, msg.data, num_thread_, metadata_, core_affinity_, msg_vec);

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
    }

 private:
    // Number of Threads
    int num_thread_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    IndexStore * index_store_;

    map<int, bool> vtx_enabled_map;
    map<int, bool> edge_enabled_map;

    bool BuildIndexVtx(int tid, int pid) {
        if (vtx_enabled_map.find(pid) != vtx_enabled_map.end()) {
            return index_store_->SetIndexMapEnable(Element_T::VERTEX, pid, true);
        }
        vector<vid_t> vid_list;
        metadata_->GetAllVertices(tid, vid_list);

        map<value_t, vector<value_t>> index_map;
        vector<value_t> no_key_vec;

        for (auto& vid : vid_list) {
            value_t vtx_v;
            Tool::str2int(to_string(vid.value()), vtx_v);
            label_t v_label;
            metadata_->GetLabelForVertex(tid, vid, v_label);
            vector<label_t> vp_list;
            metadata_->GetVPList(v_label, vp_list);


            if (pid != 0 && find(vp_list.begin(), vp_list.end(), pid) == vp_list.end()) {
                no_key_vec.push_back(move(vtx_v));
            } else {
                vpid_t vp_id(vid, pid);
                value_t val_v;
                metadata_->GetPropertyForVertex(tid, vp_id, val_v);
                index_map[val_v].push_back(move(vtx_v));
            }
        }

        index_store_->SetIndexMap(Element_T::VERTEX, pid, index_map, no_key_vec);
        vtx_enabled_map[pid] = true;

        // TODO(future): set index enable after all node done building
        return index_store_->SetIndexMapEnable(Element_T::VERTEX, pid);
    }

    bool BuildIndexEdge(int tid, int pid) {
        if (edge_enabled_map.find(pid) != edge_enabled_map.end()) {
            return index_store_->SetIndexMapEnable(Element_T::EDGE, pid, true);
        }
        vector<eid_t> eid_list;
        metadata_->GetAllEdges(tid, eid_list);

        map<value_t, vector<value_t>> index_map;
        vector<value_t> no_key_vec;

        for (auto& eid : eid_list) {
            value_t edge_v;
            Tool::str2uint64_t(to_string(eid.value()), edge_v);
            label_t label;
            metadata_->GetLabelForEdge(tid, eid, label);
            vector<label_t> ep_list;
            metadata_->GetEPList(label, ep_list);
            if (find(ep_list.begin(), ep_list.end(), pid) == ep_list.end()) {
                no_key_vec.push_back(move(edge_v));
            } else {
                epid_t ep_id(eid, pid);
                value_t val_v;
                metadata_->GetPropertyForEdge(tid, ep_id, val_v);
                index_map[val_v].push_back(move(edge_v));
            }
        }

        index_store_->SetIndexMap(Element_T::EDGE, pid, index_map, no_key_vec);
        edge_enabled_map[pid] = true;

        // TODO(future): set index enable after all node done building
        return index_store_->SetIndexMapEnable(Element_T::EDGE, pid);
    }
};

#endif /* INDEX_EXPERT_HPP_ */
