#!/bin/bash
source /home/valen/etl-kafka/myenv/bin/activate
$ZK_HOME/bin/zkServer.sh start
$KAFKA_HOME/bin/kafka-server-start.sh config/server.properties
