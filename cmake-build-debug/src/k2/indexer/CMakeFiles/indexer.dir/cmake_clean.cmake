file(REMOVE_RECURSE
  "libindexer.a"
  "libindexer.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang )
  include(CMakeFiles/indexer.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
