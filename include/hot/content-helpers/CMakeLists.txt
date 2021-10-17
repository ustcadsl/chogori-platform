set(NAMESPACE idx/contenthelpers/)
set(HDR_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/${NAMESPACE})
SET(INSTALL_DIR include/k2/indexer/${NAMESPACE})

set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/ContentEquals.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/IdentityKeyExtractor.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/PairKeyExtractor.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/PairPointerKeyExtractor.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/CStringComparator.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/OptionalValue.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/KeyComparator.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/KeyUtilities.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/TidConverters.hpp)
set(contenthelpers-HEADERS ${contenthelpers-HEADERS} ${HDR_DIRECTORY}/ValueToKeyTypeMapper.hpp)

add_library(content-helpers-lib INTERFACE)
target_sources(content-helpers-lib INTERFACE $<BUILD_INTERFACE:${HDR_DIRECTORY}/ContentEquals.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/ContentEquals.hpp>)
target_sources(content-helpers-lib INTERFACE $<BUILD_INTERFACE:${HDR_DIRECTORY}/IdentityKeyExtractor.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/IdentityKeyExtractor.hpp>)
target_sources(content-helpers-lib INTERFACE $<BUILD_INTERFACE:${HDR_DIRECTORY}/PairKeyExtractor.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/PairKeyExtractor.hpp>)
target_sources(content-helpers-lib INTERFACE $<BUILD_INTERFACE:${HDR_DIRECTORY}/PairPointerKeyExtractor.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/PairPointerKeyExtractor.hpp>)
target_sources(content-helpers-lib INTERFACE $<BUILD_INTERFACE:${HDR_DIRECTORY}/CStringComparator.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/CStringComparator.hpp>)
target_sources(content-helpers-lib INTERFACE $<BUILD_INTERFACE:${HDR_DIRECTORY}/OptionalValue.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/OptionalValue.hpp>)
target_sources(content-helpers-lib INTERFACE $<BUILD_INTERFACE:${HDR_DIRECTORY}/KeyComparator.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/KeyComparator.hpp>)
target_sources(content-helpers-lib INTERFACE $<BUILD_INTERFACE:${HDR_DIRECTORY}/KeyUtilities.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/KeyUtilities.hpp>)
target_sources(content-helpers-lib INTERFACE $<BUILD_INTERFACE:${HDR_DIRECTORY}/TidConverters.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/TidConverters.hpp>)
target_sources(content-helpers-lib INTERFACE $<BUILD_INTERFACE:${HDR_DIRECTORY}/ValueToKeyTypeMapper.hpp> $<INSTALL_INTERFACE:${INSTALL_DIR}/ValueToKeyTypeMapper.hpp>)

target_include_directories(content-helpers-lib INTERFACE 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include/k2/indexer/${NAMESPACE}>
)

# export the library in the common k2Targets
install(TARGETS content-helpers-lib EXPORT k2Targets)
install(FILES ${contenthelpers-HEADERS} DESTINATION include/k2/indexer/${NAMESPACE})
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS content-helpers-lib NAMESPACE k2:: FILE content-helper-config.cmake)