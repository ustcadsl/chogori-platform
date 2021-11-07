#cmake_minimum_required(VERSION 2.8)
#project(BASICTHRIFT)

#file(GLOB HEADERS "*.h")
#file(GLOB SOURCES "*.cpp")

aux_source_directory(./gen-cpp  DIR_SRC)
add_executable(skv_graph_test ${DIR_SRC})


#find_package(k2 REQUIRED)
#find_package (Seastar REQUIRED)
#find_package (fmt REQUIRED)
#find_package(nlohmann_json REQUIRED)
#find_package (yaml-cpp REQUIRED)


target_link_libraries (skv_graph_test -lthrift tso_client   k23si_client cpo_client appbase dto transport Seastar::seastar )
