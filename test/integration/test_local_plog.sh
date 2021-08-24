#!/bin/bash
topname=$(dirname "$0")
cd ${topname}/../..
set -e
CPODIR=/tmp/___cpo_integ_test
rm -rf ${CPODIR}
EPS="tcp+k2rpc://0.0.0.0:10000 tcp+k2rpc://0.0.0.0:10001 tcp+k2rpc://0.0.0.0:10002"

PERSISTENCE=tcp+k2rpc://0.0.0.0:22001
CPO=tcp+k2rpc://0.0.0.0:20000
TSO=tcp+k2rpc://0.0.0.0:23000
LOCALPLOG=/mnt/pmem0/plog_data

# start CPO on 2 cores
./build/src/k2/cmd/controlPlaneOracle/cpo_main -c1 --tcp_endpoints ${CPO} 20001 --data_dir ${CPODIR} --enable_tx_checksum true --reactor-backend epoll --prometheus_port 9610 --assignment_timeout=1s &
cpo_child_pid=$!

# start nodepool on 3 cores
./build/src/k2/cmd/nodepool/nodepool --log_level INFO k2::skv_server=DEBUG -c3 --tcp_endpoints ${EPS} --enable_tx_checksum true --k23si_persistence_endpoint ${PERSISTENCE} --k23si_local_plog_path ${LOCALPLOG} --reactor-backend epoll --prometheus_port 9611 --k23si_cpo_endpoint ${CPO} --tso_endpoint ${TSO} &
nodepool_child_pid=$!

# start persistence on 1 cores
./build/src/k2/cmd/persistence/persistence -c1 --tcp_endpoints ${PERSISTENCE} --enable_tx_checksum true --reactor-backend epoll --prometheus_port 9612 &
persistence_child_pid=$!

# start tso on 2 cores
./build/src/k2/cmd/tso/tso -c2 --tcp_endpoints ${TSO} 23001 --enable_tx_checksum true --reactor-backend epoll --prometheus_port 9613 &
tso_child_pid=$!

echo $cpo_child_pid >> ./build/proc/cpo
echo $nodepool_child_pid >> ./build/proc/nodepool
echo $persistence_child_pid >> ./build/proc/persistence
echo $tso_child_pid >> ./build/proc/tso

function finish {
  rv=$?
  # cleanup code
  rm -rf ${CPODIR}

  kill ${cpo_child_pid}
  echo "Waiting for cpo child pid: ${cpo_child_pid}"
  wait ${cpo_child_pid}

  kill ${nodepool_child_pid}
  echo "Waiting for nodepool child pid: ${nodepool_child_pid}"
  wait ${nodepool_child_pid}

  kill ${persistence_child_pid}
  echo "Waiting for persistence child pid: ${persistence_child_pid}"
  wait ${persistence_child_pid}

  kill ${tso_child_pid}
  echo "Waiting for tso child pid: ${tso_child_pid}"
  wait ${tso_child_pid}
  echo ">>>> Test ${0} finished with code ${rv}"
}
# trap finish EXIT

sleep 2

./build/test/k23si/k23si_test --cpo_endpoint ${CPO} --k2_endpoints ${EPS} --enable_tx_checksum true --reactor-backend epoll --prometheus_port 9614
