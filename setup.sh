#!/bin/bash
# docker run -it --rm --network host -v ${PWD}:/host zsstrike0503/builder-with-seastar /bin/bash /host/setup.sh 
cd /host
./install_deps.sh
cp /usr/local/lib/libcrc32c* /usr/lib/
rm -rf build && mkdir build && cd build && cmake .. && make -j && make install
cp test/k23si/write_async_test /usr/local/bin/
echo "Build success! Sleeping..."
sleep 300