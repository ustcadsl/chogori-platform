#!/bin/bash
concurrent_list=(1 2 3 4 5 6 7 8)
# concurrent_list=(9 10 11 12 13 14 15 16)
concurrent_list=(1)

for num in ${concurrent_list[@]}; do
    sed -i "s/concurrent_num = [^ ]\+/concurrent_num = ${num}/g" ./configs/test_write_async.cfg
    echo "concurrent num = ${num}"
    sudo killall persistence cpo_main tso nodepool nebula-metad tpcc_client > /dev/null 2>&1
    ./run.py --config_file configs/cluster.cfg  --start cpo tso persist nodepool > /dev/null
    sleep 50
    ./run.py --config_file configs/cluster.cfg  --start test_write_async > /dev/null
    sleep 5     
    logfile="./logs/test_write_async_${num}.log"
    ./run.py --config_file configs/cluster.cfg  --log test_write_async > ${logfile} 2>&1
    while ! grep "Shutdown was successful!" ${logfile} > /dev/null; do
        if grep "Segmentation fault" ${logfile} > /dev/null; then
            break
        fi
        echo "waiting..."
        sleep 5
        ./run.py --config_file configs/cluster.cfg  --log test_write_async > ${logfile} 2>&1
    done
    ./run.py --config_file configs/cluster.cfg  --stop cpo tso persist nodepool test_write_async > /dev/null
    ./run.py --config_file configs/cluster.cfg  --remove cpo tso persist nodepool test_write_async > /dev/null    
done
