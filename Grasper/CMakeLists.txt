#Copyright 2019 Husky Data Lab, CUHK
#Authors: Hongzhi Chen

cmake_minimum_required(VERSION 3.3.0)
project(GRASPER)

# set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/release/)
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/debug/)

#CONFIGURE ME!!!
#Please manually SET the paths for MPI C/CXX compiler.
set(CMAKE_C_COMPILER   "/usr/local/mpich/bin/mpicc")
set(CMAKE_CXX_COMPILER "/usr/local/mpich/bin/mpic++")

# set(CMAKE_BUILD_TYPE Release CACHE STRING "set build type to release")
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -m64")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")

set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} -fpie")


set(COMMON_LINK_LIBS "-lrt")

#CONFIGURE ME!!!
#Please manually SET the dependency paths for the following libraries.
set(ZMQ_ROOT "/usr")
set(GLOG_ROOT "/usr")
set(HDFS2_ROOT "/usr/local/hadoop-2.6.0")
set(IBVERB_ROOT "/usr")
set(TBB_ROOT "/usr")
set(MKL_ROOT "/opt/intel/compilers_and_libraries_2017.5.239/linux")


add_definitions(-DUSE_HDFS2_)  #use hdfs v2.x.x by defalut
add_definitions(-DHAS_RDMA)  #use RDMA

find_package(Threads)

# External Dependencies
include(cmake/modules/glog.cmake)
include(cmake/modules/hdfs.cmake)
include(cmake/modules/ibverbs.cmake)
include(cmake/modules/zmq.cmake)
include(cmake/modules/tbb.cmake)
# MKL
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${MKL_ROOT}/mkl/include")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -L${MKL_ROOT}/mkl/lib/intel64")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lmkl_blas95_lp64 -lmkl_lapack95_lp64 -lmkl_intel_lp64 -lmkl_sequential -lmkl_core  -DMKL_ILP64")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lmkl_scalapack_ilp64 -Wl,--no-as-needed -lmkl_cdft_core -lmkl_intel_ilp64 -lmkl_sequential -lmkl_core -lpthread -lm -ldl -lrdmacm -fPIE")


set(DEPS_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/third_party)
set(GRASPER_EXTERNAL_INCLUDES  ${GLOG_INCLUDE_DIR} ${HDFS2_INCLUDE_DIR} ${IBVERB_INCLUDE_DIR}  ${ZMQ_INCLUDE_DIR} ${TBB_INCLUDE_DIR} ${DEPS_INCLUDE_DIR})
set(GRASPER_EXTERNAL_LIBRARIES ${GLOG_LIBRARIES} ${HDFS2_LIBRARIES} ${IBVERB_LIBRARIES} ${ZMQ_LIBRARIES} ${TBB_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} ${COMMON_LINK_LIBS})
set(PUT_EXTERNAL_LIBRARIES ${HDFS2_LIBRARIES} ${COMMON_LINK_LIBS})

INCLUDE_DIRECTORIES(expert)
add_subdirectory(base)
add_subdirectory(core)
add_subdirectory(storage)
add_subdirectory(utils)
add_subdirectory(third_party)

add_subdirectory(driver)
add_subdirectory(put)
