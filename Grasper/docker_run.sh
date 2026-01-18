GRASPER="/home/kvgroup/lcy/Grasper"
IMAGE="grasper:v4"

docker run -it --privileged \
    --name grasper_client\
    --net host \
    --device=/dev/infiniband/rdma_cm \
    --device=/dev/infiniband/uverbs0 \
    --device=/dev/infiniband/uverbs1 \
    --ulimit memlock=-1 \
    -v $GRASPER:/host \
    $IMAGE /bin/bash