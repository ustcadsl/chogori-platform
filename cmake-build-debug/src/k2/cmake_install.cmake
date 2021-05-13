# Install script for directory: /home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Debug")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "1")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/appbase/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/assignmentManager/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/cmd/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/collectionMetadataCache/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/common/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/config/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/cpo/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/dto/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/indexer/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/module/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/nodePoolMonitor/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/partitionManager/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/persistence/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/transport/cmake_install.cmake")
  include("/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/tso/cmake_install.cmake")

endif()

