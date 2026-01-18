/* Copyright 2019 Husky Data Lab, CUHK

Authors: Aaron Li (cjli@cse.cuhk.edu.hk)
*/
#ifndef HAS_EXPERT_HPP_
#define HAS_EXPERT_HPP_

#include <string>
#include <vector>
#include <algorithm>

#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "core/index_store.hpp"
#include "base/type.hpp"
#include "base/predicate.hpp"
#include "expert/abstract_expert.hpp"
#include "expert/expert_cache.hpp"
#include "storage/layout.hpp"
#include "storage/metadata.hpp"
#include "utils/tool.hpp"

class HasExpert : public AbstractExpert {
 public:
    HasExpert(int id, MetaData * metadata, int machine_id, int num_thread, AbstractMailbox * mailbox, CoreAffinity* core_affinity) : AbstractExpert(id, metadata, core_affinity), machine_id_(machine_id), num_thread_(num_thread), mailbox_(mailbox), type_(EXPERT_T::HAS) {
        config_ = Config::GetInstance();
    }

    // Has:
    // inType
    // [    key:  int
    //        is_indexed: bool
    //         pred: Predicate_T
    //         pred_param: value_t]
    // Has(params) :
    //     -> key = pid; pred = ANY; pred_params = value_t(one) : has(key)
    //     -> key = pid; pred = EQ; pred_params = value_t(one) : has(key, value)
    //     -> key = pid; pred = <others>; pred_params = value_t(one/two) : has(key, predicate)
    // HasValue(params) : values -> [key = -1; pred = EQ; pred_params = string(value)]
    // HasNot(params) : key -> [key = pid; pred = NONE; pred_params = -1]
    // HasKey(params) : keys -> [key = pid; pred = ANY; pred_params = -1]
    void process(const vector<Expert_Object> & expert_objs, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        // Get Expert_Object
        Meta & m = msg.meta;
        Expert_Object expert_obj = expert_objs[m.step];

        // store all predicate
        vector<pair<int, PredicateValue>> pred_chain;

        // Get Params
        assert(expert_obj.params.size() > 0 && (expert_obj.params.size() - 1) % 3 == 0);  // make sure input format
        Element_T inType = (Element_T) Tool::value_t2int(expert_obj.params.at(0));
        int numParamsGroup = (expert_obj.params.size() - 1) / 3;  // number of groups of params

        // Create predicate chain for this query
        for (int i = 0; i < numParamsGroup; i++) {
            int pos = i * 3 + 1;
            // Get predicate params
            int pid = Tool::value_t2int(expert_obj.params.at(pos));
            Predicate_T pred_type = (Predicate_T) Tool::value_t2int(expert_obj.params.at(pos + 1));
            vector<value_t> pred_params;
            Tool::value_t2vec(expert_obj.params.at(pos + 2), pred_params);
            pred_chain.emplace_back(pid, PredicateValue(pred_type, pred_params));
        }

        switch (inType) {
            case Element_T::VERTEX:
                EvaluateVertex(tid, msg.data, pred_chain);
                break;

            case Element_T::EDGE:
                EvaluateEdge(tid, msg.data, pred_chain);
                break;
            default:
                cout << "Wrong inType" << endl;
        }

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
    int machine_id_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    // Cache
    ExpertCache cache;
    Config* config_;

    void EvaluateVertex(int tid, vector<pair<history_t, vector<value_t>>> & data, vector<pair<int, PredicateValue>> & pred_chain) {
        auto checkFunction = [&](value_t& value) {
            vid_t v_id(Tool::value_t2int(value));
            label_t v_label;
            metadata_->GetLabelForVertex(tid, v_id, v_label);
            vector<label_t> vp_list;
            metadata_->GetVPList(v_label, vp_list);
            #ifdef DEBUG
            for(auto vp : vp_list) {
                cout << vp << " ";
            }
            cout << endl;
            #endif

            for (auto & pred_pair : pred_chain) {
                int pid = pred_pair.first;
                #ifdef DEBUG
                    cout << "pid = " << pid << endl;
                #endif
                PredicateValue pred = pred_pair.second;

                if (pid == -1) {
                    int counter = vp_list.size();
                    for (auto & pkey : vp_list) {
                        vpid_t vp_id(v_id, pkey);

                        value_t val;
                        get_properties_for_vertex(tid, vp_id, val);

                        if (!Evaluate(pred, &val)) {
                            counter--;
                        }
                    }

                    // Cannot match all properties, erase
                    if (counter == 0) {
                        return true;
                    }
                } else if(pid == 1) {
                    // property is id, can get from vertex directly, avoid to call get property function
                    // only can do this if input vertex id is the same as query version
                    if(pred.pred_type == Predicate_T::ANY)
                        continue;
                    
                    if(!Evaluate(pred, &value)) {
                        return true;
                    }                
                }else {
                    // Check whether key exists for this vtx
                    if (find(vp_list.begin(), vp_list.end(), pid) == vp_list.end()) {
                        // does not exist
                        if (pred.pred_type == Predicate_T::NONE)
                            continue;
                        return true;
                    }
                    if (pred.pred_type == Predicate_T::ANY)
                        continue;

                    // Get Properties
                    vpid_t vp_id(v_id, pid);
                    value_t val;
                    get_properties_for_vertex(tid, vp_id, val);

                    // Erase when doesnt match
                    if (!Evaluate(pred, &val)) {
                        return true;
                    }
                }
            }

            return false;
        };

        for (auto & data_pair : data) {
            data_pair.second.erase(remove_if(data_pair.second.begin(), data_pair.second.end(), checkFunction), data_pair.second.end());
        }
    }

    void EvaluateEdge(int tid, vector<pair<history_t, vector<value_t>>> & data, vector<pair<int, PredicateValue>> & pred_chain) {
        auto checkFunction = [&](value_t& value) {
            eid_t e_id;
            uint2eid_t(Tool::value_t2uint64_t(value), e_id);
            label_t label;
            metadata_->GetLabelForEdge(tid, e_id, label);
            vector<label_t> ep_list;
            metadata_->GetEPList(label, ep_list);

            for (auto & pred_pair : pred_chain) {
                int pid = pred_pair.first;
                PredicateValue pred = pred_pair.second;

                if (pid == -1) {
                    int counter = ep_list.size();
                    for (auto & pkey : ep_list) {
                        epid_t ep_id(e_id, pkey);
                        value_t val;
                        get_properties_for_edge(tid, ep_id, val);

                        if (!Evaluate(pred, &val)) {
                            counter--;
                        }
                    }

                    // Cannot match all properties, erase
                    if (counter == 0) {
                        return true;
                    }
                } else {
                    // Check whether key exists for this vtx
                    if (find(ep_list.begin(), ep_list.end(), pid) == ep_list.end()) {
                        // does not exist
                        if (pred.pred_type == Predicate_T::NONE)
                            continue;
                        return true;
                    }

                    if (pred.pred_type == Predicate_T::ANY)
                        continue;

                    // Get Properties
                    epid_t ep_id(e_id, pid);
                    value_t val;
                    get_properties_for_edge(tid, ep_id, val);

                    // Erase when doesnt match
                    if (!Evaluate(pred, &val)) {
                        return true;
                    }
                }
            }

            return false;
        };

        for (auto & data_pair : data) {
            data_pair.second.erase(remove_if(data_pair.second.begin(), data_pair.second.end(), checkFunction), data_pair.second.end());
        }
    }

    void get_properties_for_vertex(int tid, vpid_t vp_id, value_t & val) {
        if (metadata_->VPKeyIsLocal(vp_id) || !config_->global_enable_caching) {
            // No Need to check Cache for local or cache is disabled
            metadata_->GetPropertyForVertex(tid, vp_id, val);
        } else {
            if (!cache.get_property_from_cache(vp_id.value(), val)) {
                ////////////////////
                cout << "Get Property for vertex and is not cached" << endl;
                metadata_->GetPropertyForVertex(tid, vp_id, val);
                cache.insert_properties(vp_id.value(), val);
            }
        }
    }

    void get_properties_for_edge(int tid, epid_t ep_id, value_t & val) {
        if (metadata_->EPKeyIsLocal(ep_id) || !config_->global_enable_caching) {
            metadata_->GetPropertyForEdge(tid, ep_id, val);
        } else {
            if (!cache.get_property_from_cache(ep_id.value(), val)) {
                metadata_->GetPropertyForEdge(tid, ep_id, val);
                cache.insert_properties(ep_id.value(), val);
            }
        }
    }
};

#endif /* HAS_EXPERT_HPP_ */
