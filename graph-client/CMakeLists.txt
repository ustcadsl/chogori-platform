cmake_minimum_required(VERSION 2.8)

project(BASICTHRIFT)

aux_source_directory(./gen-cpp DIR_SRC)	

add_executable(graph_client_v6 ${DIR_SRC})

target_link_libraries(graph_client_v6 -lthrift)
