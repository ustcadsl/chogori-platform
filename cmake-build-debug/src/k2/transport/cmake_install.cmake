# Install script for directory: /home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport

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

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/k2" TYPE STATIC_LIBRARY FILES "/home/kvgroup/lcy/chogori-platform-indexer-tmp/cmake-build-debug/src/k2/transport/libtransport.a")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/k2/transport" TYPE FILE FILES
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/AutoRRDMARPCProtocol.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/BaseTypes.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/Discovery.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/DiscoveryDTO.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/IRPCProtocol.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/Payload.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/PayloadFileUtil.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/PayloadSerialization.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/Prometheus.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/RPCDispatcher.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/RPCHeader.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/RPCParser.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/RPCProtocolFactory.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/RPCTypes.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/RRDMARPCChannel.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/RRDMARPCProtocol.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/Request.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/RetryStrategy.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/Status.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/TCPRPCChannel.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/TCPRPCProtocol.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/TXEndpoint.h"
    "/home/kvgroup/lcy/chogori-platform-indexer-tmp/src/k2/transport/VirtualNetworkStack.h"
    )
endif()

