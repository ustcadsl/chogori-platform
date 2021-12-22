#!/bin/bash
txns_count_list=(300 200 100)
keys_count_list=(500 400 300 200 100)

for txns_count in ${txns_count_list[@]}; do 
    for keys_count in ${keys_count_list[@]}; do
        for single_partition in false true; do
            for write_async in false true; do
                sed -i "s/txns_count = [^ ]\+/txns_count = ${txns_count}/g" ./configs/test_write_async.cfg
                sed -i "s/keys_count = [^ ]\+/keys_count = ${keys_count}/g" ./configs/test_write_async.cfg
                sed -i "s/single_partition = [^ ]\+/single_partition = ${single_partition}/g" ./configs/test_write_async.cfg
                sed -i "s/write_async = [^ ]\+/write_async = ${write_async}/g" ./configs/test_write_async.cfg
                echo "${txns_count} ${keys_count} ${single_partition} ${write_async}";
                # sudo ./test_write_async.sh > "test_write_async_${txns_count}_${keys_count}_${single_partition}_${write_async}.log" 2>&1
                ./run.py --config_file configs/cluster.cfg  --start cpo tso persist nodepool > /dev/null
                sleep 2
                ./run.py --config_file configs/cluster.cfg  --start test_write_async > /dev/null
                sleep 5 
                logfile="./logs/test_write_async_${txns_count}_${keys_count}_${single_partition}_${write_async}.log"
                ./run.py --config_file configs/cluster.cfg  --log test_write_async > ${logfile} 2>&1
                while ! grep "======= Test ended ========" ${logfile} > /dev/null; do
                    echo "sleeping"
                    sleep 5
                    ./run.py --config_file configs/cluster.cfg  --log test_write_async > ${logfile} 2>&1
                done
                ./run.py --config_file configs/cluster.cfg  --stop cpo tso persist nodepool test_write_async > /dev/null
                ./run.py --config_file configs/cluster.cfg  --remove cpo tso persist nodepool test_write_async > /dev/null
            done
        done
    done
done