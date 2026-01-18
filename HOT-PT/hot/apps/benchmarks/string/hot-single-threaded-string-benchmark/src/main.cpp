#include <hot/singlethreaded/HOTSingleThreaded.hpp>
#include <idx/benchmark/StringBenchmark.hpp>

#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

#define _GNU_SOURCE
#include <sched.h>
#include<cstdlib>
#include<ctime>
#include<map>
#include<vector>

class HotSingleThreadedBenchmarkWrapper {
	using TrieType = hot::singlethreaded::HOTSingleThreaded<const char*, idx::contenthelpers::IdentityKeyExtractor>;
	TrieType mTrie;
 int m_access_p[6];
 int m_bound[6];
 int m_all_access;
 int scandata[4];

public:
	bool insert(std::pair<char*, size_t> const & key) {
		return mTrie.insert(key.first);
	}

	/*bool remove(std::pair<char*, size_t> const & key) {
		return mTrie.remove(key.first);
	}*/

	bool search(std::pair<char*, size_t> const & lookupKey) {
		auto result = mTrie.find(lookupKey.first);
		return result!=mTrie.END_ITERATOR;
	}


	/*bool iterate(std::vector<std::pair<char*, size_t>> const & iterateKeys) {
		size_t i=0;
		bool iteratedAll = true;
		for(const char* value : mTrie) {
			iteratedAll = iteratedAll && (value == iterateKeys[i].first);
			++i;
		}
		return iteratedAll && i == iterateKeys.size();
	}*/
 
  /*bool iterate(std::vector<std::pair<char*, size_t>> const & iterateKeys) {
		size_t i=0;
    m_all_access=0;
		int ksize=iterateKeys.size();
		bool iteratedAll = true;
		auto scanstartit=iterateKeys.begin();
   m_access_p[0]=0;
   m_bound[0]=16;
   for(int tempi=1;tempi<6;tempi++){
   m_access_p[tempi]=0;
   m_bound[tempi]=m_bound[tempi-1]*4;}
   m_bound[0]=8;
   scandata[0]=scandata[1]=scandata[2]=0;
		while(i < ksize)
		{
      //std::cout<<"scan begin data: "<<scanstartit->first<<std::endl;
			auto it=mTrie.find(scanstartit->first);
      auto lastptr=it.mNodeStack[it.mCurrentDepth].getCurrent();
      auto nowptr=it.mNodeStack[it.mCurrentDepth].getCurrent();
			size_t length=0;
			while(it!=mTrie.END_ITERATOR && length<ksize && i<ksize)
			{
				length++;
				i++;
        nowptr=it.mNodeStack[it.mCurrentDepth].getCurrent();
        int dis=abs((int)(nowptr-lastptr)*8);
        //std::cout<<"dis="<<dis<<std::endl;
        for(int tempi=0;tempi<5;tempi++)
         if(dis>m_bound[tempi] && dis<=m_bound[tempi+1])
         {
           m_access_p[tempi]++;
           break;
         }
        if(dis>m_bound[5]) m_access_p[5]++;
        m_all_access++;
        //std::cout<<"dis="<<abs((int)(nowptr-lastptr)*8)<<std::endl;
        lastptr=it.mNodeStack[it.mCurrentDepth].getCurrent();
        //std::cout<<"scan data: "<<*it<<std::endl;
				it.addself(scandata);
			}
      if(i<ksize) std::cerr<<"scan range error!"<<std::endl;
      ++scanstartit;
		}
		return iteratedAll && i == ksize;
	}*/
 /*bool iterate(std::vector<std::pair<char*, size_t>> const & iterateKeys) {
		size_t i=0;
		bool iteratedAll = true;
		for(const char* value : mTrie) {
			//iteratedAll = iteratedAll && (value == iterateKeys[i].first);
			++i;
		}
		return iteratedAll && i == iterateKeys.size();
	}*/
 
	idx::benchmark::IndexStatistics getStatistics() {
		std::pair<size_t, std::map<std::string, double>> stats = mTrie.getStatistics();
   std::string prefix_name("m_access_over");
   for(int i=0;i<6;i++)
   {
     std::string name=prefix_name+std::to_string(i)+"   "+std::to_string(m_bound[i]);
     stats.second[name]=m_access_p[i];
   }
   stats.second["m_total"]=m_all_access;
   stats.second["i_local"]=scandata[0];
   stats.second["i_upside"]=scandata[1];
   stats.second["i_downside"]=scandata[2];
   stats.second["i_avglength"]=double(scandata[0])/scandata[3];
		return { stats.first, stats.second };
	}

	void writeDotRepresentation(std::ostream & /* output */) {
		return;
	}
};

class BaselineSingleThreadedBenchmarkWrapper {
struct myConstCharStarComparator
{
	bool operator()(const char* s1, const char* s2) const
	{
		return strcmp(s1, s2) < 0;
	}
};
	using TrieType = std::map<const char*, const char*,myConstCharStarComparator>;
	TrieType mRBT;

public:
	bool insert(std::pair<char*, size_t> const& key) {
		auto it = mRBT.insert({ key.first, key.first });
   //std::cout<<"map insert data: "<<key.first<<std::endl;
		return it.second;
	}

	/*bool remove(std::pair<char*, size_t> const& key) {
		auto iter = mRBT.find(key.first);
		if (iter != mRBT.end()) {
			mRBT.erase(iter);
			return true;
		}
		else {
			return false;
		}
	}*/

	bool search(std::pair<char*, size_t> const& lookupKey) {
		auto it = mRBT.find(lookupKey.first);
		return it != mRBT.end();
	}

	/*bool iterate(std::vector<std::pair<char*, size_t>> const& iterateKeys) {
		size_t i = 0;
		bool iteratedAll = true;
		for (auto it = mRBT.begin(); it != mRBT.end(); ++it) {
			iteratedAll = iteratedAll && (it->first == iterateKeys[i].first);
			++i;
		}
		return iteratedAll && i == iterateKeys.size();
	}*/
  /*bool iterate(std::vector<std::pair<char*, size_t>> const & iterateKeys) {
		size_t i=0;
		int ksize=iterateKeys.size();
		bool iteratedAll = true;
		auto scanstartit=iterateKeys.begin();
		while(i < ksize)
		{
      //std::cout<<"scan begin data: "<<scanstartit->first<<std::endl;
			auto it=mRBT.find(scanstartit->first);
			size_t length=0;
			while(it!=mRBT.end() && length<ksize && i<ksize)
			{
				length++;
				i++;
        //std::cout<<"scan data: "<<it->first<<std::endl;
				it++;
			}
      ++scanstartit;
		}
		return iteratedAll && i == ksize;
	}*/
	idx::benchmark::IndexStatistics getStatistics() {
		size_t totalSize = sizeof(mRBT);
		for (auto it = mRBT.begin(); it != mRBT.end(); it++) {
			totalSize += sizeof(it->first) + sizeof(it->second);
		}
		std::map<std::string, double> additionalInformation;

		return { totalSize, additionalInformation};
	}

	void writeDotRepresentation(std::ostream& /* output */) {
		return;
	}
};


int main(int argc, char** argv) {
  cpu_set_t mask;
  CPU_ZERO(&mask);
  int cpu_id = 51;
  CPU_SET(cpu_id, &mask);
  if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
  {
    std::cout << "WARNING: Could not set CPU Affinity, continuing" << std::endl;
  }
  else
  {
    std::cout << "Bind process to cpu id: " << cpu_id << std::endl<<"running at CPU no. "<<sched_getcpu()<<std::endl;
  }
	idx::benchmark::StringBenchmark<HotSingleThreadedBenchmarkWrapper> baseline(argc, argv, "HotSingleThreadedStringsBenchmark");
	idx::benchmark::StringBenchmark<BaselineSingleThreadedBenchmarkWrapper> benchmark(argc, argv, "Baseline std::MAP StringBenchmark");
	baseline.run();
	//benchmark.run();
	return 0;
};
