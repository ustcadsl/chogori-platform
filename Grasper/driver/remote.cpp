/*
 * @Author: chunyuLi 
 * @Date: 2022-09-14 22:22:17 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-27 16:15:21
 */


#include "base/node.hpp"
#include "base/node_util.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"
#include "driver/remote.hpp"


#include "glog/logging.h"

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);

    Node my_node;
    // InitMPIComm(&argc, &argv, my_node);

    // config file of remote node 
    string cfg_fname = argv[1]; 
    CHECK(!cfg_fname.empty());

    vector<Node> nodes = ParseFile(cfg_fname);
    CHECK(CheckUniquePort(nodes));
    my_node = nodes.back();
    nodes.pop_back();
    // tmp usage
    my_node.set_world_size(1);
    my_node.set_world_rank(0);
    my_node.set_local_size(1);
    my_node.set_local_rank(0);

    cout << my_node.DebugString();

    // set this as
    Node::StaticInstance(&my_node);

    Config* config = Config::GetInstance();
    config->Init();

    cout  << "DONE -> Remote Config->Init()" << endl;

    Remote remote(my_node, nodes);
    remote.Init();
    remote.Start();
    remote.Run();

    auto pid = getpid();
    string cp_cmd = "cp /proc/" + to_string(pid) + "/stat ./";
    system(cp_cmd.c_str());   

    return 0;
}
