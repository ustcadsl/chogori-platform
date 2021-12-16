file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (pmem_log_test ${HEADERS} PmemLogTest.cpp)
add_executable (pmem_log_bench ${HEADERS} PmemLogBench.cpp)
target_link_libraries (pmem_log_test PRIVATE transport dto common pmem_storage)
target_link_libraries (pmem_log_bench PRIVATE transport dto common pmem_storage benchmark::benchmark)
add_test(NAME pmem_log COMMAND pmem_log_test)