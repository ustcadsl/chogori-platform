project(hot-single-threaded)

include_directories(include)

set(NAMESPACE hot/singlethreaded)
set(HDR_NAMESPACE ${CMAKE_CURRENT_SOURCE_DIR}/include/${NAMESPACE})
set(INSTALL_DIR include/k2/indexer/${NAMESPACE})

set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedChildPointer.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedChildPointerInterface.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedDeletionInformation.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedInsertStackEntry.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/MemoryPool.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreaded.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedInterface.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedIterator.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedNode.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedNodeBase.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedNodeBaseInterface.hpp)
set(hot-single-threaded-HEADERS ${hot-single-threaded-HEADERS} ${HDR_NAMESPACE}/HOTSingleThreadedNodeInterface.hpp)

add_library(hot-single-threaded-lib INTERFACE)
target_sources(hot-single-threaded-lib INTERFACE $<BUILD_INTERFACE:${HDR_NAMESPACE}/HOTSingleThreadedChildPointer.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/HOTSingleThreadedChildPointer.hpp>)
target_sources(hot-single-threaded-lib INTERFACE $<BUILD_INTERFACE:${HDR_NAMESPACE}/HOTSingleThreadedChildPointerInterface.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/HOTSingleThreadedChildPointerInterface.hpp>)
target_sources(hot-single-threaded-lib INTERFACE $<BUILD_INTERFACE:${HDR_NAMESPACE}/HOTSingleThreadedDeletionInformation.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/HOTSingleThreadedDeletionInformation.hpp>)
target_sources(hot-single-threaded-lib INTERFACE $<BUILD_INTERFACE:${HDR_NAMESPACE}/HOTSingleThreadedInsertStackEntry.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/HOTSingleThreadedInsertStackEntry.hpp>)
target_sources(hot-single-threaded-lib INTERFACE $<BUILD_INTERFACE:${HDR_NAMESPACE}/MemoryPool.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/MemoryPool.hpp>)
target_sources(hot-single-threaded-lib INTERFACE $<BUILD_INTERFACE:${HDR_NAMESPACE}/HOTSingleThreaded.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/HOTSingleThreaded.hpp>)
target_sources(hot-single-threaded-lib INTERFACE $<BUILD_INTERFACE:${HDR_NAMESPACE}/HOTSingleThreadedInterface.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/HOTSingleThreadedInterface.hpp>)
target_sources(hot-single-threaded-lib INTERFACE $<BUILD_INTERFACE:${HDR_NAMESPACE}/HOTSingleThreadedIterator.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/HOTSingleThreadedIterator.hpp>)
target_sources(hot-single-threaded-lib INTERFACE $<BUILD_INTERFACE:${HDR_NAMESPACE}/HOTSingleThreadedNode.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/HOTSingleThreadedNode.hpp>)
target_sources(hot-single-threaded-lib INTERFACE $<BUILD_INTERFACE:${HDR_NAMESPACE}/HOTSingleThreadedNodeBase.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/HOTSingleThreadedNodeBase.hpp>)
target_sources(hot-single-threaded-lib INTERFACE $<BUILD_INTERFACE:${HDR_NAMESPACE}/HOTSingleThreadedNodeBaseInterface.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/HOTSingleThreadedNodeBaseInterface.hpp>)
target_sources(hot-single-threaded-lib INTERFACE $<BUILD_INTERFACE:${HDR_NAMESPACE}/HOTSingleThreadedNodeInterface.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/HOTSingleThreadedNodeInterface.hpp>)
target_include_directories(hot-single-threaded-lib INTERFACE 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include/k2/indexer/${NAMESPACE}>
)
target_link_libraries(hot-single-threaded-lib INTERFACE hot-commons-lib content-helpers-lib)

# export the library in the common k2Targets
install(TARGETS hot-single-threaded-lib EXPORT k2Targets)
install(FILES ${hot-single-threaded-HEADERS} DESTINATION ${INSTALL_DIR})
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS hot-single-threaded-lib NAMESPACE k2:: FILE hot-single-threaded-config.cmake)