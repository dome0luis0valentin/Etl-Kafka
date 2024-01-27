from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# Function to parse the JSON messages
def parse_json(message):
    try:
        return json.loads(message[1])
    except json.decoder.JSONDecodeError:
        print(f"Error decoding JSON: {message[1]}")
        return None

# Function to process the messages from Kafka
def process_messages(messages):
    for message in messages:
        print(f"Received message: {message}")

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "KafkaSparkStreaming")
ssc = StreamingContext(sc, 1)

# Define Kafka parameters
kafka_params = {
    "bootstrap.servers": "localhost:9092",
    "auto.offset.reset": "smallest",
    "group.id": "spark-streaming-consumer-group"
}

# Create a DStream that represents streaming data from Kafka
kafka_stream = KafkaUtils.createDirectStream(ssc, ["api_data_topic"], kafka_params, valueDecoder=parse_json)

# Process the messages
kafka_stream.foreachRDD(lambda rdd: rdd.foreachPartition(process_messages))

# Start the streaming context
ssc.start()
ssc.awaitTermination()
