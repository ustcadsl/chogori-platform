memory=$((200*1024))
core="0-15"

ulimit -m ${memory}
cd build
cmake ..
make server client
cd ..
# $1 PRO_NUM   $2 IB.CONF
taskset -c ${core} /usr/local/mpich/bin/mpiexec -np $1 \
    -env UCX_NET_DEVICES=mlx5_1:1 \
    ./debug/server $2
