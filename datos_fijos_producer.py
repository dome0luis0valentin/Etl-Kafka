from confluent_kafka import Producer
import json
import requests
from api import obtener_precios_acciones
from api import obtener_valor

from pprint import pprint
def fetch_api_data(api_url):
    response = requests.get(api_url)
    return response.json()

def modificar_formato(lista, name):
    nueva_lista = []
    for diccionario in lista:
        nuevo_diccionario = {
            "name": name,
            "date": diccionario["d"],
            "value": diccionario["v"]
        }
        nueva_lista.append(nuevo_diccionario)
    return nueva_lista

def kafka_producer():
    """
    Function that produces data to a Kafka topic.

    This function retrieves stock prices and API data, and sends them to a Kafka topic called 'api_data_topic'.

    Parameters:
    None

    Returns:
    None
    """
    
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    api_urls = ["https://api.estadisticasbcra.com/inflacion_mensual_oficial",
                "https://api.estadisticasbcra.com/plazo_fijo",
                "https://api.estadisticasbcra.com/usd"]

    for api_url in api_urls:
        api_data = obtener_valor(api_url)
        api_data = api_data[len(api_data)-30:]
        print(api_url)

        titulo = api_url.split("/")[-1]
        dict = modificar_formato(api_data, titulo)

        for i, diccionario in enumerate(dict):
        #Agregamos el mensaje recibido de la API al topic
            producer.produce('datos_fijos', key=titulo, value=json.dumps(diccionario))

    producer.flush() # Ensure all messages are sent before exiting

#Cheque que el topic tenga elementos:
#
#$KAFKA_HOME/bin/kafka-console-consumer.sh --topic api_data_topic --bootstrap-server localhost:9092 --from-beginning


if __name__ == "__main__":
    kafka_producer()
