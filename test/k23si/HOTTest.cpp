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

#define CATCH_CONFIG_MAIN

#include <vector>

#include <k2/indexer/HOTIndexer.h>
#include <k2/indexer/IndexerInterface.h>
#include "catch2/catch.hpp"

namespace k2 {
    SCENARIO("HOT Insert tests") {
        HOTindexer _hotIndexer;
        std::vector<dto::Key> testKeys;

        testKeys.push_back(dto::Key{.schemaName="test", .partitionKey="part", .rangeKey="rKey1"});
        testKeys.push_back(dto::Key{.schemaName="hot", .partitionKey="part", .rangeKey="rKey2"});
        testKeys.push_back(dto::Key{.schemaName="insert", .partitionKey="part", .rangeKey="rKey3"});
        testKeys.push_back(dto::Key{.schemaName="and", .partitionKey="part", .rangeKey="rKey4"});
        testKeys.push_back(dto::Key{.schemaName="find", .partitionKey="part", .rangeKey="averyLongRangeKey"});
        testKeys.push_back(dto::Key{.schemaName="test", .partitionKey="part", .rangeKey="r0"});

        for(auto it = testKeys.begin(); it != testKeys.end(); ++it) {
            KeyValueNode* ret1 = _hotIndexer.insert(*it);
            REQUIRE(ret1 != nullptr);
            HotIterator findRes = _hotIndexer.find(*it);
            REQUIRE(_hotIndexer.extractFromIter(findRes) == ret1); 
        }
        REQUIRE(_hotIndexer.size() == testKeys.size());        
    }
}