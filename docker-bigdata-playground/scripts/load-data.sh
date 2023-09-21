#!/bin/bash

DATA_DIR=/data
HDFS_DATA_DIR=/data-team

hdfs dfs -mkdir $HDFS_DATA_DIR

for file in $DATA_DIR/*.json; do
    filename=$(basename $file)
    hdfs dfs -put $file $HDFS_DATA_DIR/$filename
done