# Etl-Kafka
This repository contain a ETL that use: Kafka, Airflow, PosGreSQL and Snowflake

*How to start the zookeeper*
bin/zkServer.sh start

_Delete topic_
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic api_data_topic

_Create topic_
# Reemplaza 'localhost:9092' con tu dirección de bootstrap server si es diferente
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic api_data_topic --partitions 1 --replication-factor 1

