file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(pmem_storage STATIC ${HEADERS} ${SOURCES})
target_link_libraries (pmem_storage PRIVATE k23si Seastar::seastar pmem)


# export the library in the common k2Targets
install(TARGETS pmem_storage EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/pmemStorage)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS pmem_storage NAMESPACE k2:: FILE pmem_storage-config.cmake)
