#!/bin/bash
# servers=(1 2 3 4 5 6 7 8)
servers=(1 2 3 4)

for num in ${servers[@]}; do
    unfinished=1
    while [ $unfinished -eq 1 ]
    do
        # sed -i "22s/cpus = [0-9]\+/cpus = ${num}/g" ./configs/cluster.cfg 
        sed -i "52s/cpus = [0-9]\+/cpus = ${num}/g" ./configs/cluster.cfg
        echo "server num = ${num}"
        error=0
        # clean up mess
        docker container rm -f cpo0 tso0 persist0 nodepool0 test_write_async0 tpcc_client0 cpo1 tso1 nodepool1 tpcc_client1 > /dev/null
        rm -rf state.p > /dev/null        
        sudo killall persistence cpo_main tso nodepool nebula-metad tpcc_client > /dev/null 2>&1
        ./run.py --config_file configs/cluster.cfg  --start cpo tso persist nodepool > /dev/null
        sleep 50
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
            ./run.py --config_file configs/cluster.cfg  --log test_write_async > ${logfile} 2>&1
        done
        ./run.py --config_file configs/cluster.cfg  --stop cpo tso persist nodepool test_write_async > /dev/null
        # sleep 5
        ./run.py --config_file configs/cluster.cfg  --remove cpo tso persist nodepool test_write_async > /dev/null   
        if [ $error -eq 0 ]
        then
            unfinished=0
        fi
    done
done
