from confluent_kafka import Producer
import json
import requests
from api import obtener_precios_acciones
from api import obtener_valor
from pprint import pprint
def fetch_api_data(api_url):
    response = requests.get(api_url)
    return response.json()

def modificar_formato_acciones(lista):
    nueva_lista = []
    for diccionario in lista:
        nuevo_diccionario = {
            "key": "acciones_argentinas",
            "name": diccionario["simbolo"],
            "date": diccionario["fecha"].split("T")[0],
            "value": diccionario["ultimoPrecio"],
            "description": diccionario["descripcion"]
        }
        nueva_lista.append(nuevo_diccionario)
    return nueva_lista

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

    data_acciones = obtener_precios_acciones()

    if type(data_acciones) == str:
        data_acciones = obtener_precios_acciones()
    data_acciones = modificar_formato_acciones(data_acciones["titulos"])
   
    index = 0
    for i, diccionario in enumerate(data_acciones):
            
        #Agregamos el mensaje recibido de la API al topic
            producer.produce('empresas', key="acciones_arg", value=json.dumps(diccionario))
    # producer.produce('api_data_topic', key="acciones_argentinas", value=json.dumps(data_acciones))

    # api_urls = ["https://api.estadisticasbcra.com/merval_usd",
    #             "https://api.estadisticasbcra.com/inflacion_mensual_oficial",
    #             "https://api.estadisticasbcra.com/plazo_fijo"]

    # for api_url in api_urls:
    #     api_data = obtener_valor(api_url)
    #     print(api_url)

    #     titulo = api_url.split("/")[-1]
    #     dict = modificar_formato(api_data, titulo)
    


    #     for i, diccionario in enumerate(dict):
    #     #Agregamos el mensaje recibido de la API al topic
    #         producer.produce('api_data_topic', key=titulo, value=json.dumps(diccionario))

    producer.flush() # Ensure all messages are sent before exiting

#Cheque que el topic tenga elementos:
#
#$KAFKA_HOME/bin/kafka-console-consumer.sh --topic api_data_topic --bootstrap-server localhost:9092 --from-beginning


if __name__ == "__main__":
    kafka_producer()
