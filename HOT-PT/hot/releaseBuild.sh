#!/bin/sh
CURRENT_DIRECTORY=$(pwd)
SOURCE_ROOT_DIRECTORY=/home/kvgroup/lcy/hot
rm -rf $SOURCE_ROOT_DIRECTORY/testbuild
mkdir $SOURCE_ROOT_DIRECTORY/testbuild
cd $SOURCE_ROOT_DIRECTORY/testbuild
cmake ../ -DCMAKE_BUILD_TYPE=Release "$@"
make -j8
