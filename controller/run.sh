#!/bin/bash

# Environment variables
export HADOOP_HOME=/usr/local/hadoop-3.3.1
export HBASE_HOME=/usr/local/hbase
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64

# Ensure Hadoop and HBase are running
if ! jps | grep -q "NameNode"; then
    mapred --daemon start historyserver
    start-dfs.sh
    start-yarn.sh
fi

if ! jps | grep -q "HMaster"; then
    start-hbase.sh
fi

# Build and run
mvn clean compile assembly:single
java -jar target/controller-0.0.1-jar-with-dependencies.jar