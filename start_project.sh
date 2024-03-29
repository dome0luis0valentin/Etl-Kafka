#!/bin/bash

dir_init = "/home/valen/etl-kafka"

echo dir_init
echo "Initiating the enviroment"
source /home/valen/etl-kafka/myenv/bin/activate
echo    "Starting Zookeeper and Kafka"
$ZK_HOME/bin/zkServer.sh start > /home/valen/etl-kafka/kafka.log 2>&1

echo "Starting Kafka"
cd $KAFKA_HOME 

./bin/kafka-server-start.sh config/server.properties &> /home/valen/etl-kafka/kafka.log

cd /home/valen/etl-kafka/Etl-Kafka

echo "Produce data to Kafka"

python obtener_token.py
echo "Token obtenido"
python producer.py
echo "Data producida"
python datos_fijos_producer.py
echo "Data fija producida"

echo "Consume data from Kafka"

python stream_empresas.py 2> /home/valen/etl-kafka/Etl-Kafka/kafka.log
python consumer_fijos.py 2> /home/valen/etl-kafka/Etl-Kafka/kafka.log

echo "Finished"


