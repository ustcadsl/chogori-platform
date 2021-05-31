file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (pbrb_test ${HEADERS} PbrbTest.cpp)

target_link_libraries (pbrb_test PRIVATE appbase pbrb dto)

add_test(NAME pbrb COMMAND pbrb_test)