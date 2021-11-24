file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(pbrb STATIC ${HEADERS} ${SOURCES})
set_target_properties(pbrb PROPERTIES LINKER_LANGUAGE CXX)

# export the library in the common k2Targets
install(TARGETS pbrb EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/pbrb)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS pbrb NAMESPACE k2:: FILE pbrb-config.cmake)