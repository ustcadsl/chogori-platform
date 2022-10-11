#!/bin/bash
space_list=(750 500 250 100 50 25 10)
# space_list=(1000000 500000 100000 50000 10000 5000 1000 750 500 250 100 50 25 10)
# concurrent_list=(9)
# concurrent_list=(4)

for num in ${space_list[@]}; do
    unfinished=1
    while [ $unfinished -eq 1 ]
    do
        sed -i "s/key_space = [^ ]\+/key_space = ${num}/g" ./configs/test_write_async.cfg
        echo "key_space = ${num}"
        error=0
        docker container rm -f cpo0 tso0 persist0 nodepool0 test_write_async0 tpcc_client0 cpo1 tso1 nodepool1 tpcc_client1 > /dev/null
        rm -rf state.p > /dev/null        
        sudo killall persistence cpo_main tso nodepool nebula-metad tpcc_client > /dev/null 2>&1
        sleep 5
        ./run.py --config_file configs/cluster.cfg  --start cpo tso persist nodepool > /dev/null
        sleep 20
        ./run.py --config_file configs/cluster.cfg  --start test_write_async > /dev/null
        sleep 5     
        logfile="./logs/test_write_async_${num}.log"
        ./run.py --config_file configs/cluster.cfg  --log test_write_async > ${logfile} 2>&1
        while ! grep "Shutdown was successful!" ${logfile} > /dev/null; do
            if grep "Segmentation fault" ${logfile} > /dev/null; then
                error=1
                break
            fi
            echo "waiting..."
            sleep 5
            ./run.py --config_file configs/cluster.cfg  --log nodepool > nodepool.log 2>&1
            ./run.py --config_file configs/cluster.cfg  --log test_write_async > ${logfile} 2>&1
        done
        ./run.py --config_file configs/cluster.cfg  --stop cpo tso persist nodepool test_write_async > /dev/null
        ./run.py --config_file configs/cluster.cfg  --remove cpo tso persist nodepool test_write_async > /dev/null
        sleep 10 
        if [ $error -eq 0 ]
        then
            unfinished=0
        fi
    done   
done
