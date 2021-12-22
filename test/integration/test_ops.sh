#!/bin/bash
txns_count_list=(10 100 1000)
keys_count_list=(1 10 20 30 40 50 60 70 80 90 100)

for txns_count in ${txns_count_list[@]}; do 
    for keys_count in ${keys_count_list[@]}; do
        for single_partition in false true; do
            for write_async in false true; do
                sed -i "s/--txns_count=[^ ]\+/--txns_count=${txns_count}/g" ./test_write_async.sh
                sed -i "s/--keys_count=[^ ]\+/--keys_count=${keys_count}/g" ./test_write_async.sh
                sed -i "s/--single_partition=[^ ]\+/--single_partition=${single_partition}/g" ./test_write_async.sh
                sed -i "s/--write_async=[^ ]\+/--write_async=${write_async}/g" ./test_write_async.sh
                echo "${txns_count} ${keys_count} ${single_partition} ${write_async}";
                sudo ./test_write_async.sh > "test_write_async_${txns_count}_${keys_count}_${single_partition}_${write_async}.log" 2>&1
                sleep 3
            done
        done
    done
done