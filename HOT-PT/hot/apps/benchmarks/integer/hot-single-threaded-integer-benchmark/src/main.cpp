#include <algorithm>
#include <cassert>
#include <chrono>
#include <iostream>
#include <map>

#include <hot/singlethreaded/HOTSingleThreaded.hpp>
#include <idx/benchmark/Benchmark.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

/**
 * Wrapper class to fulfill the requirements of the benchmarking framework
 */
class HotSingleThreadedIntegerBenchmarkWrapper {
	using TrieType = hot::singlethreaded::HOTSingleThreaded<uint64_t, idx::contenthelpers::IdentityKeyExtractor>;
	TrieType mTrie;

public:
	inline bool insert(uint64_t key) {
     //std::cout<<"insert key= "<<key<<std::endl;
		return mTrie.insert(key);
	}

	/*inline bool remove(uint64_t key) {
		return mTrie.remove(key);
	}*/
	
	inline bool search(uint64_t key) {
		auto result = mTrie.find(key);
   //std::cout<<"read key= "<<key<<"   ; read result= "<<*result<<std::endl;
		return result!=mTrie.END_ITERATOR & (*result== key);
	}

	/*inline bool iterateAll(std::vector<uint64_t> const & iterateKeys) {
		size_t i=0;
		bool iteratedAll = true;
		for(uint64_t value : mTrie) {
			iteratedAll = iteratedAll & (value == iterateKeys[i]);
			++i;
		}
		return iteratedAll & (i == iterateKeys.size());
	}*/

	idx::benchmark::IndexStatistics getStatistics() {
		std::pair<size_t, std::map<std::string, double>> stats = mTrie.getStatistics();

		return { stats.first, stats.second };
	}
};


int main(int argc, char** argv) {
	idx::benchmark::Benchmark<HotSingleThreadedIntegerBenchmarkWrapper> benchmark(argc, argv, "HotSingleThreadedIntegerBenchmark");
	return benchmark.run();
}