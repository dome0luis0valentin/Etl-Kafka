from confluent_kafka import Producer
import json
import requests
from api import obtener_precios_acciones
from api import obtener_valor

def fetch_api_data(api_url):
    response = requests.get(api_url)
    return response.json()

def kafka_producer():
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    data_acciones = obtener_precios_acciones()

    producer.produce('api_data_topic', key="acciones_argentinas", value=json.dumps(data_acciones))

    api_urls = ["https://api.estadisticasbcra.com/merval_usd",
                "https://api.estadisticasbcra.com/inflacion_mensual_oficial",
                "https://api.estadisticasbcra.com/plazo_fijo"]

    for api_url in api_urls:
        api_data = obtener_valor(api_url)
        print(api_url)
        producer.produce('api_data_topic', key=api_url, value=json.dumps(api_data))

    producer.flush() # Ensure all messages are sent before exiting

#Cheque que el topic tenga elementos:
#
#$KAFKA_HOME/bin/kafka-console-consumer.sh --topic api_data_topic --bootstrap-server localhost:9092 --from-beginning


if __name__ == "__main__":
    kafka_producer()
