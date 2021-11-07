#pragma once
#include "k2_includes.h"

class GraphK2Client {
public:
    GraphK2Client();
    ~GraphK2Client();

     // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();
private:
    k2::K23SIClient* _client;
    std::unordered_map<k2::dto::K23SI_MTR, k2::K2TxnHandle>* _txns;

    seastar::future<> _poller = seastar::make_ready_future();

}