#!/bin/bash
# hugepage config
# sudo hugeadm --create-group-mounts kvgroup
sudo hugeadm --pool-pages-min 2MB:35000
# increase mtu
# will lead to failure to ssh knode1, but if not set, cannot establish the rdma channel
sudo ip link set ens4f1 mtu 9000	
# sudo ip link set ens4f0 mtu 9000
# other system config
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
echo 0 | sudo tee /proc/sys/kernel/numa_balancing
echo 0 | sudo tee /proc/sys/kernel/watchdog
echo 0 | sudo tee /proc/sys/kernel/nmi_watchdog
for i in {0..79}
do
    echo 0 | sudo tee /sys/devices/system/machinecheck/machinecheck${i}/check_interval
done
echo 5 | sudo tee /proc/sys/vm/stat_interval
# turn off swap
# swapoff -a