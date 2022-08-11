#!/bin/bash

./run.py --config_file configs/cluster.cfg  --start cpo tso persist nodepool
sleep 30
./run.py --config_file configs/cluster.cfg  --log nodepool > nodepool.log 2>&1
./run.py --config_file configs/cluster.cfg  --start load
sleep 5
./run.py --config_file configs/cluster.cfg  --log load > tpcc_load.log 2>&1
sleep 300
./run.py --config_file configs/cluster.cfg  --stop load
sleep 20
./run.py --config_file configs/cluster.cfg  --remove load
sleep 10
./run.py --config_file configs/cluster.cfg  --start client
sleep 5
./run.py --config_file configs/cluster.cfg  --log client > tpcc_client.log 2>&1
sleep 300
./run.py --config_file configs/cluster.cfg  --stop cpo tso persist nodepool client
sleep 30
./run.py --config_file configs/cluster.cfg  --remove cpo tso persist nodepool client