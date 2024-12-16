import pandas as pd
import requests


def connect_to_rickandmorty_api(url_param, name_param):
    try:
    
        if not url_param.startswith("https://"):
            raise ValueError("La URL debe comenzar con https://")
       
        url = f"{url_param}{name_param}"
     
        response = requests.get(url)
        response.raise_for_status()  
      
        json_response = response.json()
     
        data = json_response['results']
        df = pd.json_normalize(data)

        print("Archivo JSON leído correctamente y normalizado")
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la petición: {e}")
    except ValueError as e:
        print(f"Error de validación: {e}")
    except KeyError as e:
        print(f"La respuesta JSON no tiene la clave esperada: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")

    return None