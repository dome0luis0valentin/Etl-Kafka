import requests
import os
from pprint import pprint

from dotenv import load_dotenv
from obtener_token import obtener_token

# Cargar variables de entorno desde el archivo .env
load_dotenv()


def obtener_precios_acciones():
    """
    Esta funcion hace una consulta a la API de IOL para las acciones argentinas
    """
    token = os.environ.get("ACCESS_TOKEN")
    url = "https://api.invertironline.com/api/v2/Cotizaciones/acciones/argentina/Todos"
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
        }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an exception in case of HTTP error
        return response.json()
        
    except requests.exceptions.RequestException as e:
        obtener_token()
        return obtener_precios_acciones()
        
 
def obtener_valor(url):
    """
    Las respuestas de las consultas al API devuelven JSON y salvo la de /milestones tienen la siguiente estructura:

            [
                {
                    "d": fecha en formato MySQL,
                    "v": valor del indicador para esa fecha.
                }
            ]
        
    """ 
    token = os.environ.get("TOKEN")
    url = url
    
    headers = {
        'Authorization': f'BEARER {token}'
        }

    response = requests.get(url, headers=headers)

    return response.json()






def obtener_precio_dolar_oficial():
    token = os.environ.get("DOLAR_TOKEN")
    url = "https://api.estadisticasbcra.com/merval_usd"
    
    headers = {
        'Authorization': f'BEARER {token}'
        }

    response = requests.get(url, headers=headers)

    # Imprime el código de estado y el contenido de la respuesta
    print("Código de estado:", response.status_code)
    print("Respuesta:")
    print(response.json())

def obtener_inflacion():
    token = os.environ.get("DOLAR_TOKEN")
    url = "https://api.estadisticasbcra.com/inflacion_mensual_oficial"
    
    headers = {
        'Authorization': f'BEARER {token}'
        }

    response = requests.get(url, headers=headers)

    # Imprime el código de estado y el contenido de la respuesta
    print("Código de estado:", response.status_code)
    print("Respuesta:")
    return response.json()

def obtener_plazo_fijo():
    token = os.environ.get("DOLAR_TOKEN")
    url = "https://api.estadisticasbcra.com/plazo_fijo"
    headers = {
        'Authorization': f'BEARER {token}'
        }

    response = requests.get(url, headers=headers)

    # Imprime el código de estado y el contenido de la respuesta
    print("Código de estado:", response.status_code)
    print("Respuesta:")
    return response.json()

