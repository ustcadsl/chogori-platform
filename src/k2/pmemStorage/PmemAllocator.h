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
#include <k2/transport/Payload.h>
#include <k2/transport/Status.h>
#include "PmemEngine.h"
#include <tuple>
namespace k2
{
    
template <typename Elem>
class FreeList
{
private:
    static FreeList<Elem> *freelist;
public:
    Elem element;
    FreeList *next;
    FreeList(const Elem& elem, FreeList* next=NULL);
    FreeList(FreeList* next=NULL);
    void* operator new(size_t);    
    void operator delete(void*);   
};


template <typename Elem>
FreeList<Elem>* FreeList<Elem>::freelist = NULL;

template <typename Elem>
FreeList<Elem>::FreeList(const Elem& elem, FreeList* next)
{
    this->element = elem;
    this->next = next;
}

template <typename Elem>
FreeList<Elem>::FreeList(FreeList* next)
{
    this->next = next;
}

template <typename Elem>
void* FreeList<Elem>::operator new(size_t)
{
    /*freelist没有可用空间，就从系统分配*/
    if(freelist == NULL)  
        return ::new FreeList;

    /*否则，从freelist表头摘取结点*/
    FreeList<Elem>* temp = freelist;
    freelist = freelist->next;
    return temp;
}

template <typename Elem>
void FreeList<Elem>::operator delete(void* ptr)
{
    ((FreeList<Elem>*)ptr)->next = freelist;
    freelist = (FreeList<Elem>*)ptr;
}


class PmemAllocator{
    public:
    
    PmemAllocator(){}

    Status createPool(const char* poolPath, uint64_t poolSize, uint64_t chunkSize);

    char * allocate(uint32_t chunkCount = 1);

    Status free(char * ptr);
    
    private:
    
    char* _pmemPool;

    uint64_t _poolSize;

    uint64_t _tailOffset;

    uint64_t _chunkSize;

    std::




};


}