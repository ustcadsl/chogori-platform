GRASPER="/home/kvgroup/lcy/Grasper"
IMAGE="grasper:v4"

docker run -it -d --privileged \
    --name grasper_server_test\
    --net host \
    --device=/dev/infiniband/rdma_cm \
    --device=/dev/infiniband/uverbs0 \
    --device=/dev/infiniband/uverbs1 \
    --ulimit memlock=-1 \
    -v $GRASPER:/host \
    $IMAGE /bin/bash

    # -p 11010:10010 \
    # -p 11011:10011 \