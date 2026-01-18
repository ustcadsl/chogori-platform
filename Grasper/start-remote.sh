memory=$((1024*1024*1))
core="0-1"
# $1 ib.config
cd build
cmake ..
make remote
cd ..

ulimit -m ${memory}
taskset -c ${core} ./debug/remote $1
