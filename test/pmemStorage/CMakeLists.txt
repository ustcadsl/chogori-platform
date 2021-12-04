file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (pmem_log_test ${HEADERS} PmemLogTest.cpp)
target_link_libraries (pmem_log_test PRIVATE transport common pmem_storage)
add_test(NAME pmem_log COMMAND pmem_log_test)