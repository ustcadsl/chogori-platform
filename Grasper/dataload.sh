#!/bin/bash 
DIR="sf0.1"

hadoop fs -rm -r /tmp/$DIR
hadoop fs -mkdir /tmp/$DIR

hadoop fs -mkdir /tmp/$DIR/vtx_property
hadoop fs -put ./data/$DIR/vtx_property/* /tmp/$DIR/vtx_property

hadoop fs -mkdir /tmp/$DIR/vertices
hadoop fs -put ./data/$DIR/vertices/* /tmp/$DIR/vertices

hadoop fs -mkdir /tmp/$DIR/edge_property
hadoop fs -put ./data/$DIR/edge_property/* /tmp/$DIR/edge_property

hadoop fs -mkdir /tmp/$DIR/index
hadoop fs -put ./data/$DIR/index/* /tmp/$DIR/index
