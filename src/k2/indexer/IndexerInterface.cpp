// implementation

#include "IndexerInterface.h"
#include <k2/pbrb/pbrb_design.h>
namespace k2 {

dto::DataRecord *KeyValueNode::get_datarecord(const dto::Timestamp &timestamp, int &order, PBRB *pbrb) {
    std::cout << "@get_datarecord: " << timestamp << ", "<< order;
    for (int i = 0; i < 3; ++i)
        if (timestamp.tEndTSECount() >= valuedata[i].timestamp) {
            std::cout << key << ", " << valuedata[i].timestamp << ", " << valuedata[i].valuepointer << std::endl;
            order = i;
            return valuedata[i].valuepointer;
        }
    order = -1;
    dto::DataRecord *viter = valuedata[2].valuepointer;

    // Get pointer of cold version out of indexer.
    if (is_inmem(2)) 
        viter = static_cast<dto::DataRecord *>(pbrb->getPlogAddrRow(viter));
    else
        viter = viter->prevVersion;
    
    while (viter != nullptr && timestamp.compareCertain(viter->timestamp) < 0) {
        // skip newer records
        viter = viter->prevVersion;
    }
    return viter;
    }

NodeVerMetadata KeyValueNode::getNodeVerMetaData(int order, PBRB *pbrb) {
    dto::DataRecord *rec = valuedata[order].valuepointer;
    if (rec == nullptr) {
        NodeVerMetadata retVal;
        return retVal;
    }
    if (is_inmem(order)) {
        void *hotAddr = static_cast<void *>(rec);
        NodeVerMetadata retVal{
            .isHot=true,
            .timestamp=pbrb->getTimestampRow(hotAddr),
            .status=pbrb->getStatusRow(hotAddr),
            .isTombstone=pbrb->getIsTombstoneRow(hotAddr),
            .request_id=pbrb->getRequestIdRow(hotAddr)
        };
        retVal.print();
        return retVal;
    }
    else {
        NodeVerMetadata retVal{
            .isHot=false,
            .timestamp=rec->timestamp,
            .status=rec->status,
            .isTombstone=rec->isTombstone,
            .request_id=rec->request_id
        };
        retVal.print();
        return retVal;
    }
}
    // struct NodeVerMetadata{
    //     bool isHot;
    //     dto::Timestamp timestamp;
    //     dto::DataRecord::Status status;
    //     bool isTombstone;
    //     uint64_t request_id;
    //     void print() {
    //         K2LOG_I(log::pbrb, "Node Metadata: [isHot: {}, timestamp: {}, status: {}, isTombstone: {}, request_id: {}", isHot, timestamp, status, isTombStone, request_id);
    //     }
    // };
}