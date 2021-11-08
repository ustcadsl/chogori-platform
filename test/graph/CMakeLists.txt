aux_source_directory(./gen-cpp  DIR_SRC)
add_executable(skv_test_v6 ${DIR_SRC})
#target_link_libraries (skv_test_v6 -lthrift tso_client k23si_client cpo_client appbase dto transport Seastar::seastar)

target_link_libraries (skv_test_v6 -lthrift tso_client k23si_client cpo_client appbase dto transport Seastar::seastar)
