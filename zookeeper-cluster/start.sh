#!/bin/bash

echo "Starting ZooKeeper servers..."

./apache-zookeeper-server-1/bin/zkServer.sh start
./apache-zookeeper-server-2/bin/zkServer.sh start
./apache-zookeeper-server-3/bin/zkServer.sh start

echo "All ZooKeeper servers started."