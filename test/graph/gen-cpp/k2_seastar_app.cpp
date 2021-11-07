
#include "k2_seastar_app.h"

#include "k2_includes.h"
#include "k2_queue_defs.h"
#include "k2_txn.h"


GraphK2Client::GraphK2Client(){
    _client = new k2::K23SIClient(k2::K23SIClientConfig());
    _txns = new std::unordered_map<k2::dto::K23SI_MTR, k2::K2TxnHandle>();
}

GraphK2Client::~GraphK2Client(){
    delete _client;
    delete _txns;
}

seastar::future<> GraphK2Client::start(){
     if (seastar::this_shard_id() == 0) {
        K2LOG_I(log::k2ss, "Poller starting");
        _poller = _poller.then([this] {
            return seastar::do_until(
                [this] {
                    return _stop;
                },
                [this] {
                    return _pollForWork();
                }
            );
        });
    }
    return _client->start();

}