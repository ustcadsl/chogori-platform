/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once

#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>

namespace k2 {
namespace dto {

// Complete unique identifier of a transaction in the system
struct TxnId {
    // this is the routable key for the TR - we can route requests for the TR (i.e. PUSH)
    // based on the partition map of the collection.
    dto::Key trh;
    // the MTR for the transaction
    dto::K23SI_MTR mtr;

    size_t hash() const {
        return trh.hash() + mtr.hash();
    }

    bool operator==(const TxnId& o) const{
        return trh == o.trh && mtr == o.mtr;
    }

    bool operator!=(const TxnId& o) const{
        return !operator==(o);
    }

    friend std::ostream& operator<<(std::ostream& os, const TxnId& txnId) {
        return os << "{trh=" << txnId.trh << ", mtr=" << txnId.mtr <<"}";
    }

    K2_PAYLOAD_FIELDS(trh, mtr);
};

} // ns dto
} // ns d2

// Define std::hash for TxnIds so that we can use them in hash maps/sets
namespace std {
template <>
struct hash<k2::dto::TxnId> {
    size_t operator()(const k2::dto::TxnId& txnId) const {
        return txnId.hash();
    }
};  // hash
} // ns std

namespace k2 {
namespace dto {

// A record in the 3SI version cache.
struct DataRecord {
    dto::Key key;
    SerializeAsPayload<Payload> value;
    bool isTombstone = false;
    dto::TxnId txnId;
    enum Status: uint8_t {
        WriteIntent,  // the record hasn't been committed/aborted yet
        Committed     // the record has been committed and we should use the key/value
        // aborted WIs don't need state - as soon as we learn that a WI has been aborted, we remove it
    } status;
    K2_PAYLOAD_FIELDS(key, value, isTombstone, txnId, status);
};

enum class TxnRecordState : uint8_t {
        Created = 0,
        InProgress,
        ForceAborted,
        Aborted,
        Committed,
        Deleted
};

inline std::ostream& operator<<(std::ostream& os, const TxnRecordState& st) {
    const char* strstate = "bad state";
    switch (st) {
        case TxnRecordState::Created: strstate= "created"; break;
        case TxnRecordState::InProgress: strstate= "in_progress"; break;
        case TxnRecordState::ForceAborted: strstate= "force_aborted"; break;
        case TxnRecordState::Aborted: strstate= "aborted"; break;
        case TxnRecordState::Committed: strstate= "committed"; break;
        case TxnRecordState::Deleted: strstate= "deleted"; break;
        default: break;
    }
    return os << strstate;
}

// All of the inspect requests in this file are for test and debug purposes
// They return the current 3SI state without affecting it

// Requests all versions including WIs for a particular key
// without affecting transaction state
struct K23SIInspectRecordsRequest {
    // These fields make the request compatible with the PartitionRequest wrapper
    Partition::PVID pvid; // the partition version ID. Should be coming from an up-to-date partition map
    String collectionName;
    Key key; // the key to gather all records for
    K2_PAYLOAD_FIELDS(pvid, collectionName, key);
};

struct K23SIInspectRecordsResponse {
    std::vector<DataRecord> records;
    K2_PAYLOAD_FIELDS(records);
};

// Requests the TRH of a transaction without affecting transaction state
struct K23SIInspectTxnRequest {
    // These fields make the request compatible with the PartitionRequest wrapper
    Partition::PVID pvid; // the partition version ID. Should be coming from an up-to-date partition map
    String collectionName;
    Key key; // the key of the THR to request
    K23SI_MTR mtr;
    K2_PAYLOAD_FIELDS(pvid, collectionName, key, mtr);
};

// Contains the TRH data (struct TxnRecord) without the internal
// management members such as intrusive list hooks
struct K23SIInspectTxnResponse {
    TxnId txnId;

    // the keys to which this transaction wrote. These are delivered as part of the End request and we have to ensure
    // that the corresponding write intents are converted appropriately
    std::vector<dto::Key> writeKeys;

    // Expiry time point for retention window - these are driven off each TSO clock update
    dto::Timestamp rwExpiry;

    bool syncFinalize = false;

    TxnRecordState state;

    K2_PAYLOAD_FIELDS(txnId, writeKeys, rwExpiry, state);

    friend std::ostream& operator<<(std::ostream& os, const K23SIInspectTxnResponse& rec) {
        os << "{txnId=" << rec.txnId << ", writeKeys=[";
        os << rec.writeKeys.size();
        os << "], rwExpiry=" << rec.rwExpiry << ", syncfin=" << rec.syncFinalize << "}";
        return os;
    }
};

// Requests all WIs on a node for all keys
struct K23SIInspectWIsRequest {
    K2_PAYLOAD_EMPTY;
};

struct K23SIInspectWIsResponse {
    std::vector<DataRecord> WIs;
    K2_PAYLOAD_FIELDS(WIs);
};

// Request all TRHs on a node
struct K23SIInspectAllTxnsRequest {
    K2_PAYLOAD_EMPTY;
};

struct K23SIInspectAllTxnsResponse {
    std::vector<K23SIInspectTxnResponse> txns;
    K2_PAYLOAD_FIELDS(txns);
};

// Request all keys stored on a node
struct K23SIInspectAllKeysRequest {
    K2_PAYLOAD_EMPTY;
};

struct K23SIInspectAllKeysResponse {
    std::vector<Key> keys;
    K2_PAYLOAD_FIELDS(keys);
};

} // ns dto
} // ns k2

