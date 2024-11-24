#!/bin/bash

# Stop HBase
stop-hbase.sh

# Clean HBase directories in HDFS
hdfs dfs -rm -r -skipTrash /hbase/archive/*
hdfs dfs -rm -r -skipTrash /hbase/oldWALs/*
hdfs dfs -rm -r -skipTrash /hbase/data/*
hdfs dfs -rm -r -skipTrash /hbase/.tmp/*
hdfs dfs -rm -r -skipTrash /hbase/.logs/*

# Clean local HBase data
rm -rf /usr/local/hbase/logs/*
rm -rf /usr/local/hbase/data/*
rm -rf /usr/local/hbase/zookeeper/*

# Recreate HDFS directory
hdfs dfs -mkdir -p /hbase
hdfs dfs -chmod 777 /hbase

# Start HBase
start-hbase.sh

# Wait for HBase to start
sleep 10

# Verify status
echo "status" | hbase shell