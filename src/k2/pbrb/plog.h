#include<iostream>
#include<cstdlib>
#include<new>

const int plogSize = 4 * 1024;

using PLogAddr = void *;
using SchemaId = uint32_t;

class SimplePlog {

struct PlogPage {
    uint8_t content[plogSize];
};

public:
    void *cursor;
    PlogPage *plog;
    SimplePlog() {
        auto aligned_val = std::align_val_t{plogSize};
        plog = static_cast<PlogPage *>(operator new(sizeof(PlogPage), aligned_val));
        cursor = static_cast<void *>(plog);
        std::cout << "Created Plog in: " << plog << std::endl;
    };

    void writeSId(SchemaId schemaId) {
        memcpy(cursor, &schemaId, 4);
        std::cout << "Write Data of SchemaId " << schemaId << " in Addr: " << cursor << std::endl;
        cursor = (void *) ((uint8_t *)cursor + 4);
    }
    void write(const void *srcPtr, size_t size) {
        memcpy(cursor, srcPtr, size);
        cursor = (void *) ((uint8_t *)cursor + size);
    }

};

template <typename T>
void writeToPlog(PLogAddr pAddr, const T *srcPtr, size_t size)
{
    const void *sPtr = static_cast<const void *>(srcPtr);
    void *destPtr = static_cast<void *>(pAddr);
    memcpy(destPtr, sPtr, size);
}

// Read from pagePtr + offset with size to srcPtr;
template <typename T>
T readFromPlog(const PLogAddr pAddr, size_t size)
{
    T result;
    void *dPtr = static_cast<void *>(&result);
    const void *sPtr = static_cast<const void *>(pAddr);
    memcpy(dPtr, sPtr, size);
    return result;
}