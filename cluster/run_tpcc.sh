#!/bin/bash
logdir=log_n10_l10_c10_wh20_pbrb_true
echo ${logdir}/tpcc_load.log

./run.py --config_file configs/cluster.cfg  --start cpo tso persist nodepool
sleep 20
./run.py --config_file configs/cluster.cfg  --start load
sleep 600
./run.py --config_file configs/cluster.cfg  --log load > ${logdir}/tpcc_load.log 2>&1
./run.py --config_file configs/cluster.cfg  --stop load
sleep 5
./run.py --config_file configs/cluster.cfg  --remove load
sleep 5
./run.py --config_file configs/cluster.cfg  --start client
sleep 400
./run.py --config_file configs/cluster.cfg  --log client > ${logdir}/tpcc_client.log 2>&1
sleep 5
./run.py --config_file configs/cluster.cfg  --stop cpo tso persist nodepool client
sleep 20
./run.py --config_file configs/cluster.cfg  --log cpo > ${logdir}/cpo.log 2>&1
./run.py --config_file configs/cluster.cfg  --log tso > ${logdir}/tso.log 2>&1
./run.py --config_file configs/cluster.cfg  --log persist > ${logdir}/persist.log 2>&1
./run.py --config_file configs/cluster.cfg  --log nodepool > ${logdir}/nodepool.log 2>&1
sleep 20
./run.py --config_file configs/cluster.cfg  --remove cpo tso persist nodepool client