#ifndef __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_ITERATOR__
#define __HOT__SINGLE_THREADED__HOT_SINGLE_THREADED_ITERATOR__

#include <cstdint>
#include <array>

#include "hot/singlethreaded/HOTSingleThreadedChildPointer.hpp"
#include "hot/singlethreaded/HOTSingleThreadedNodeBase.hpp"
#include "idx/contenthelpers/TidConverters.hpp"

namespace hot {
	namespace singlethreaded {

		class HOTSingleThreadedIteratorStackEntry {
			HOTSingleThreadedChildPointer const* mBegin;
			HOTSingleThreadedChildPointer const* mCurrent;
			HOTSingleThreadedChildPointer const* mEnd;

		public:
			//leaf uninitialized
			HOTSingleThreadedIteratorStackEntry() {
			}

			HOTSingleThreadedChildPointer const* init(HOTSingleThreadedChildPointer const* begin, HOTSingleThreadedChildPointer const* current, HOTSingleThreadedChildPointer const* end) {
				mBegin = begin;
				mEnd = end;
				mCurrent = current;
				return mCurrent;
			}

			HOTSingleThreadedChildPointer const* getCurrent() const {
				return mCurrent;
			}

			bool isExhausted(bool direction) {
				return direction ? mCurrent == mEnd : mCurrent == mBegin-1;
			}

			void advance(bool direction) {
				if (direction) {
					if (mCurrent != mEnd) {
						++mCurrent;
					}
				}
				else {
					if (mCurrent != mBegin-1) {
						--mCurrent;
					}
				}
			}
		};

		template<typename ValueType, template <typename> typename KeyExtractor> class HOTSingleThreaded; //Forward Declaration of SIMDCobTrie for usage as friend class

		template<typename ValueType> class HOTSingleThreadedIterator {
			template<typename ValueType2, template <typename> typename KeyExtractor> friend class hot::singlethreaded::HOTSingleThreaded;

			static HOTSingleThreadedChildPointer END_TOKEN;

			alignas (std::alignment_of<HOTSingleThreadedIteratorStackEntry>()) char mRawNodeStack[sizeof(HOTSingleThreadedIteratorStackEntry) * 64];
			HOTSingleThreadedIteratorStackEntry* mNodeStack;
			size_t mCurrentDepth = 0;

		public:
			HOTSingleThreadedIterator(HOTSingleThreadedChildPointer const* mSubTreeRoot, bool direction = true) : HOTSingleThreadedIterator(mSubTreeRoot, mSubTreeRoot + 1) {
				// When direction is true, Iterator points to begin position of SubTree
				// When direction is false, Iterator points to last position of SubTree
				descend(direction);
			}

			HOTSingleThreadedIterator(HOTSingleThreadedIterator const& other) : mNodeStack(reinterpret_cast<HOTSingleThreadedIteratorStackEntry*>(mRawNodeStack)) {
				std::memcpy(this->mRawNodeStack, other.mRawNodeStack, sizeof(HOTSingleThreadedIteratorStackEntry) * (other.mCurrentDepth + 1));
				mCurrentDepth = other.mCurrentDepth;
			}

			HOTSingleThreadedIterator& operator=(HOTSingleThreadedIterator const& other) {
				mNodeStack = reinterpret_cast<HOTSingleThreadedIteratorStackEntry*>(mRawNodeStack);
				std::memcpy(this->mRawNodeStack, other.mRawNodeStack, sizeof(HOTSingleThreadedIteratorStackEntry) * (other.mCurrentDepth + 1));
				mCurrentDepth = other.mCurrentDepth;

				return *this;
			}

			HOTSingleThreadedIterator() : mNodeStack(reinterpret_cast<HOTSingleThreadedIteratorStackEntry*>(mRawNodeStack)) {
				mNodeStack[0].init(&END_TOKEN, &END_TOKEN, &END_TOKEN);
			}

		public:
			ValueType operator*() const {
				return idx::contenthelpers::tidToValue<ValueType>(mNodeStack[mCurrentDepth].getCurrent()->getTid());
			}

			HOTSingleThreadedIterator<ValueType>& operator++() {
				mNodeStack[mCurrentDepth].advance(true);
				while ((mCurrentDepth > 0) & (mNodeStack[mCurrentDepth].isExhausted(true))) {
					--mCurrentDepth;
					mNodeStack[mCurrentDepth].advance(true);
				}
				if (mNodeStack[0].isExhausted(true)) {
					mNodeStack[0].init(&END_TOKEN, &END_TOKEN, &END_TOKEN);
				}
				else {
					descend(true);
				}
				return *this;
			}

			HOTSingleThreadedIterator<ValueType>& operator--() {
				mNodeStack[mCurrentDepth].advance(false);
				while ((mCurrentDepth > 0) & (mNodeStack[mCurrentDepth].isExhausted(false))) {
					--mCurrentDepth;
					mNodeStack[mCurrentDepth].advance(false);
				}
				if (mNodeStack[0].isExhausted(false)) {
					mNodeStack[0].init(&END_TOKEN, &END_TOKEN, &END_TOKEN);
				}
				else {
					descend(false);
				}
				return *this;
			}

			bool operator==(HOTSingleThreadedIterator<ValueType> const& other) const {
				return (*mNodeStack[mCurrentDepth].getCurrent()) == (*other.mNodeStack[other.mCurrentDepth].getCurrent());
			}

			bool operator!=(HOTSingleThreadedIterator<ValueType> const& other) const {
				return (*mNodeStack[mCurrentDepth].getCurrent()) != (*other.mNodeStack[other.mCurrentDepth].getCurrent());
			}

		private:
			HOTSingleThreadedIterator(HOTSingleThreadedChildPointer const* currentRoot, HOTSingleThreadedChildPointer const* rootEnd) : mNodeStack(reinterpret_cast<HOTSingleThreadedIteratorStackEntry*>(mRawNodeStack)) {
				mNodeStack[0].init(currentRoot, currentRoot, rootEnd);
			}

			void descend(bool direction = true) {
				HOTSingleThreadedChildPointer const* currentSubtreeRoot = mNodeStack[mCurrentDepth].getCurrent();
				while (currentSubtreeRoot->isAValidNode()) {
					HOTSingleThreadedNodeBase* currentSubtreeRootNode = currentSubtreeRoot->getNode();
					currentSubtreeRoot = descend(currentSubtreeRootNode->begin(), direction ? currentSubtreeRootNode->begin() : currentSubtreeRootNode->end()-1, currentSubtreeRootNode->end());
				}
			}

			HOTSingleThreadedChildPointer const* descend(HOTSingleThreadedChildPointer const* begin, HOTSingleThreadedChildPointer const* current, HOTSingleThreadedChildPointer const* end) {
				return mNodeStack[++mCurrentDepth].init(begin, current, end);
			}
		};

		template<typename KeyHelper> HOTSingleThreadedChildPointer HOTSingleThreadedIterator<KeyHelper>::END_TOKEN{};

	}
}

#endif