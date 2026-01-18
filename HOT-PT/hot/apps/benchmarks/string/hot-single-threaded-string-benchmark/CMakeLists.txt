#find_package(PkgConfig REQUIRED)
#pkg_check_modules (JEMALLOC jemalloc)

#pkg_search_module(JEMALLOC REQUIRED jemalloc)
#include_directories("/home/kvgroup/lcy/jemalloc/jemalloc-5.2.1/include/")
SET(JEMALLOC_INCLUDE_DIR /home/kvgroup/lcy/jemalloc/jemalloc-5.2.1/include)
SET(JEMALLOC_LIB /home/kvgroup/lcy/jemalloc/jemalloc-5.2.1/lib)
include_directories(${JEMALLOC_INCLUDE_DIR})
LINK_DIRECTORIES(${JEMALLOC_LIB})

add_executable(hot-single-threaded-strings-benchmark "src/main.cpp")
target_link_libraries(hot-single-threaded-strings-benchmark hot-single-threaded-lib benchmark-helpers-lib content-helpers-lib jemalloc)
