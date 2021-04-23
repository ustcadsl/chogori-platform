#include<iostream>
#include<cstdlib>
#include<algorithm>
#include<map>

class PBRB;

struct KVN {
    bool isCached[3] = {false};
    int timestamp[3] = {0};
    void *addr[3] = {nullptr};
    int rowNum = 0;
    uint32_t recentVer = {0};

    bool hasCachedVer() { return isCached[0] || isCached[1] || isCached[2]; }
    bool isAllCached() { return isCached[0] && isCached[1] && isCached[2];}

    int findVerByTs(int ts) {
        for (int i = 0; i < rowNum; i++) {
            if (timestamp[i] == ts) 
                return i;
        }
        return -1;
    }

    void *findColdAddrByTs(int ts) {
        for (int i = 0; i < rowNum; i++) {
            if (timestamp[i] == ts && !isCached[i]) 
                return addr[i];
        }
        return nullptr;
    }

    bool insertRow(void *newAddr, int ts, bool isHot, PBRB *pbrb);

    void hotToCold(int x, void *pAddr){
        isCached[x] = false;
        addr[x] = pAddr;
    }

    void coldToHot(int x, void *hAddr){
        isCached[x] = true;
        addr[x] = hAddr;
    }
};

typedef std::map<std::string, KVN> Index;