// implementation

#include "IndexerInterface.h"
#include <k2/pbrb/pbrb_design.h>
namespace k2 {

dto::DataRecord *KeyValueNode::get_datarecord(const dto::Timestamp &timestamp, int &order, PBRB *pbrb) {
    K2LOG_D(log::indexer, "get_datarecord: {}", timestamp);
    for (int i = 0; i < 3; ++i)
        if (timestamp.tEndTSECount() >= valuedata[i].timestamp) {
            // std::cout << key << ", " << valuedata[i].timestamp << ", " << valuedata[i].valuepointer << std::endl;
            order = i;
            return valuedata[i].valuepointer;
        }
    order = -1;

    // Get pointer of cold version out of indexer.
    dto::DataRecord *viter = _getColdVerPtr(2, pbrb)->prevVersion;
    K2LOG_I(log::indexer, "----Get pointer of cold version out of indexer");
    while (viter != nullptr && timestamp.compareCertain(viter->timestamp) < 0) {
        // skip newer records
        viter = viter->prevVersion;
    }
    return viter;
    }

NodeVerMetadata KeyValueNode::getNodeVerMetaData(int order, PBRB *pbrb) {
    if (order >= 3 || order < 0) {
        K2LOG_D(log::indexer, "order: {} out of range", order);
        NodeVerMetadata retVal {
            .isHot=false,
            .timestamp=dto::Timestamp(),
            .status=dto::DataRecord::Status(),
            .isTombstone=false,
            .request_id=0
        };
        return retVal;
    }
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

int KeyValueNode::insert_datarecord(dto::DataRecord *datarecord, PBRB *pbrb, const dto::Timestamp &timestamp, bool isUpdateAddr, int order) {
    //g_mutex.lock();
if(isUpdateAddr){
    if(order<0){
        for (int i = 0; i < 3; ++i) {
            if (timestamp.tEndTSECount() == valuedata[i].timestamp) {
                //set_inmem(i, 1);
                //datarecord->prevVersion = valuedata[i].valuepointer;
                valuedata[i].valuepointer = static_cast<dto::DataRecord *>(datarecord);
                set_inmem(i, 1);
                K2LOG_D(log::indexer, "cache to PBRB:{}", valuedata[i].timestamp);
                //g_mutex.unlock();
                return 0;
            }
        }
        K2LOG_I(log::indexer, "cache to PBRB:{} failed!!", timestamp.tEndTSECount());
        //g_mutex.unlock();
        return 1;
    } else {
        valuedata[order].valuepointer = datarecord;
        valuedata[order].timestamp = datarecord->timestamp.tEndTSECount();
        //g_mutex.unlock();
        return 0;
    }
} else {
    //updating = 1;
    if(size() > 0) {
        dto::DataRecord::Status status;
        if (!is_inmem(0)){
            status = valuedata[0].valuepointer->status;
        } else {
            //status = pbrb->getStatusRow(valuedata[0].valuepointer);
            dto::DataRecord *coldAddr = static_cast<dto::DataRecord *> (pbrb->getPlogAddrRow(valuedata[0].valuepointer));
            status = coldAddr->status;
        }
        
        if(status == dto::DataRecord::Committed) {
            for (int i = 0; i < 3; ++i) {
                if (datarecord->timestamp.tEndTSECount() == valuedata[i].timestamp) {
                    //set_inmem(i, 1);
                    //datarecord->prevVersion = valuedata[i].valuepointer;
                    K2LOG_I(log::indexer, "update KVNode:{}", valuedata[i].timestamp);
                    valuedata[i].valuepointer = datarecord;
                    //g_mutex.unlock();
                    return 0;
                }
            }
            //remove the last version of the KVNode and remove it from PBRB if it is cached
            if(is_inmem(2)){ //////
                void* HotRowAddr = static_cast<void *> (valuedata[2].valuepointer);
                auto pair = pbrb->findRowByAddr(HotRowAddr);
                BufferPage *pagePtr = pair.first;
                RowOffset rowOff = pair.second;
                pbrb->removeHotRow(pagePtr, rowOff);
                //size_dec();
                //K2LOG_I(log::indexer, "remove hot KV:{} from PBRB", valuedata[2].timestamp);
                /*#ifdef FIXEDFIELD_ROW
                    pbrb->releaseHeapSpace(pagePtr, HotRowAddr);
                #endif

                #ifdef PAYLOAD_ROW
                    pbrb->releasePayloadHeapSpace(pagePtr, HotRowAddr);
                #endif
                */
            } //////
            if(size() == 3) {
                size_dec();
            }
            //delete valuedata[2].valuepointer;//////
            //K2LOG_I(log::indexer, "remove KV:{} from KVNode", valuedata[2].timestamp);
            datarecord->prevVersion = valuedata[0].valuepointer;
            for (int i = 2; i > 0; i--) {
                valuedata[i] = valuedata[i - 1];
                set_tombstone(i, is_tombstone(i - 1));
                set_exist(i, is_exist(i - 1));
                set_inmem(i, is_inmem(i - 1));
            }
            set_zero_writeintent(); // WI = False;
        }
        else {
            size_dec();
            datarecord->prevVersion = valuedata[1].valuepointer;
            set_writeintent(); // WI = True;
            //K2LOG_I(log::indexer, "unCommitted:{}", valuedata[0].timestamp);
            if (is_inmem(0)) {        
                void* HotRowAddr = static_cast<void *> (valuedata[0].valuepointer);
                auto pair = pbrb->findRowByAddr(HotRowAddr);
                BufferPage *pagePtr = pair.first;
                RowOffset rowOff = pair.second;
                pbrb->removeHotRow(pagePtr, rowOff);
                /*#ifdef FIXEDFIELD_ROW
                    pbrb->releaseHeapSpace(pagePtr, HotRowAddr);
                #endif

                #ifdef PAYLOAD_ROW
                    pbrb->releasePayloadHeapSpace(pagePtr, HotRowAddr);
                #endif
                */
            } else {
                delete valuedata[0].valuepointer;
            }
        }
    } else {
        K2LOG_D(log::indexer, "size:{}", size());
    }
                
    valuedata[0].valuepointer = datarecord;
    valuedata[0].timestamp = datarecord->timestamp.tEndTSECount();
    size_inc();
    set_tombstone(0, datarecord->isTombstone);
    set_exist(0, 1);
    set_inmem(0, 0);
    //updating = 0;
    K2LOG_D(log::indexer, "After insert new datarecord:{}, size:{}, t:{}", datarecord->timestamp.tEndTSECount(), size(), timestamp);
    //printAll();
    }
    //g_mutex.unlock();
    return 0;
}

inline dto::DataRecord *KeyValueNode::_getColdVerPtr(int order, PBRB *pbrb) {
    if (is_inmem(order))
        return static_cast<dto::DataRecord *>(pbrb->getPlogAddrRow(valuedata[order].valuepointer));
    else
        return valuedata[order].valuepointer;
}

int KeyValueNode::remove_datarecord(int order, PBRB *pbrb) {
    if (order >= 3 || valuedata[order].valuepointer == nullptr) {
        return 1;
    }

    if (is_inmem(order)) {
        K2LOG_I(log::indexer, "need to evict hot version here:{}", key->rangeKey);
        // TODO: evict hot version here
        void* HotRowAddr = static_cast<void *> (valuedata[order].valuepointer);
        auto pair = pbrb->findRowByAddr(HotRowAddr);
        BufferPage *pagePtr = pair.first;
        RowOffset rowOff = pair.second;
        pbrb->removeHotRow(pagePtr, rowOff);
        //size_dec();
        /*#ifdef FIXEDFIELD_ROW
            pbrb->releaseHeapSpace(pagePtr, HotRowAddr);
        #endif

        #ifdef PAYLOAD_ROW
            pbrb->releasePayloadHeapSpace(pagePtr, HotRowAddr);
        #endif
        */
    }

    dto::DataRecord *toremove = _getColdVerPtr(order, pbrb);

    if (order > 0) {
        dto::DataRecord *predPtr = _getColdVerPtr(order - 1, pbrb);
        dto::DataRecord *succPtr = _getColdVerPtr(order, pbrb);
        
        predPtr->prevVersion = succPtr->prevVersion;
    }
    for (int j = order; j < 2; ++j) {
        valuedata[j] = valuedata[j + 1];
        set_tombstone(j, is_tombstone(j + 1));
        set_exist(j, is_exist(j + 1));
        set_inmem(j, is_inmem(j + 1));
    }
    if (size_dec()==1) 
        K2LOG_D(log::indexer, "try to remove with no versions, Key: {} {} {}", key->schemaName, key->partitionKey, key->rangeKey);
    if (valuedata[2].valuepointer != nullptr) {
        dto::DataRecord *OldestVer = _getColdVerPtr(2, pbrb);
        if(OldestVer->prevVersion != nullptr){
            valuedata[2].valuepointer = OldestVer->prevVersion;
            set_tombstone(2, valuedata[2].valuepointer->isTombstone);
            set_inmem(2, 0);
        }
        else {
            set_zero(2);
            set_tombstone(2, 0);
            set_exist(2, 0);
            set_inmem(2, 0);
        }
    }

    delete toremove;
    return 0;
}

int KeyValueNode::remove_datarecord(dto::DataRecord *datarecord, PBRB *pbrb) {
    if (datarecord == nullptr) {
        return 1;
    }

    dto::DataRecord *toremove;
    for (int i = 0; i < 3; ++i) {
        if (valuedata[i].valuepointer == nullptr) {
            return 1;
        }
        if (valuedata[i].valuepointer == datarecord) {
            return this->remove_datarecord(i, pbrb);
        }
    }

    dto::DataRecord *viter = _getColdVerPtr(2, pbrb);
    while (viter->prevVersion != nullptr && viter->prevVersion != datarecord) {
        // skip newer records
        viter = viter->prevVersion;
    }
    if (viter->prevVersion == nullptr) {
        return 1;
    }
    toremove = viter->prevVersion;
    viter->prevVersion = toremove->prevVersion;
    size_dec();
    delete toremove;
    return 0;
}

}