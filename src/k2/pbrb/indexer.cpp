#include "indexer.h"
#include "pbrb_design.h"

namespace k2 {

bool KVN::insertRow(void *newAddr, int ts, bool isHot, PBRB *pbrb)
{

    int pos = findVerByTs(ts);

    if (pos >= 0)
    {
        isCached[pos] = isHot;
        addr[pos] = newAddr;
        return true;
    }

    int insertPos;

    for (insertPos = 0; insertPos < rowNum; insertPos++)
        if (ts > timestamp[insertPos])
        //if (ts.compareCertain(timestamp[insertPos]) > 0)
            break;

    if (insertPos >= 3)
    {
        std::cout << "This record is older than 3 versions here." << std::endl;
        return false;
    }
    else if (insertPos < rowNum)
    {

        // evict the oldest hot version
        if (rowNum == 3 && isCached[2])
        {
            hotToCold(2, pbrb->evictRow(addr[2]));
        }

        // move rows from insertPos to the last ver;
        for (int i = 2; i > insertPos; i--)
        {
            timestamp[i] = timestamp[i - 1];
            isCached[i] = isCached[i - 1];
            addr[i] = addr[i - 1];
        }
    }

    // insert into insertPos
    timestamp[insertPos] = ts;
    isCached[insertPos] = isHot;
    addr[insertPos] = newAddr;

    if (rowNum < 3)
        rowNum++;

    return true;
}

}