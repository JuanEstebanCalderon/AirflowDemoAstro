import sys
import os
import argparse
import requests
import pandas as pd
from sqlalchemy import create_engine

# Agregar el directorio raíz del proyecto al sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.extract import Extractor
from models.transform import Transformer
from models.load import DataLoader
from config import DataConexion


class ETLController:
    def __init__(self, API_URL):
        self.API_URL = API_URL
        self.extractor = Extractor(API_URL)  # Instanciamos Extractor
        self.transformer = Transformer(self.extractor)  # Pasamos la instancia de Extractor a Transformer
        self.loader = DataLoader()  # Instanciamos el cargador de datos


    @staticmethod
    def obtener_url():
        """Obtener la URL completa a partir de los argumentos."""
        parser = argparse.ArgumentParser(description='Script para extraer datos de una API usando una URL construida.')
        parser.add_argument('--base_url', type=str, default='https://github.com/sferez/BybitMarketData/raw/main/data/', help='La URL base para construir la URL completa.')
        parser.add_argument('-r', '--url', type=str, required=True, help='Parte variable de la URL (ejemplo: datafile.json).')
        args = parser.parse_args()
        full_url = args.base_url + args.url
        return full_url



    def conexion_API(self):
        """Verificar conexión a la API."""
        print(f"Verificando la URL: {self.API_URL}")
        response = requests.get(self.API_URL)
        print(f'status de la conexion API => {response.status_code}')
        if response.status_code == 200:
            print("Conexión exitosa a la API")
        else:
            print("No se puede conectar a la API")


    def run_etl_extract(self):
        """Extraer datos de la API y devolver el nombre del archivo JSONL."""
        try:
            jsonl_filename = self.extractor.extract_data_from_api()
            print(f"Nombre del archivo JSONL: {jsonl_filename}")
            return jsonl_filename  # Retornar el nombre del archivo JSONL
        except Exception as e:
            print(f"Error al extraer los datos: {e}")
            return None




    def show_dataframe(self, jsonl_filename2):
        """Mostrar el DataFrame a partir del archivo JSONL."""
        try:
            print(f"Archivo JSONL: {jsonl_filename2}")
            df = self.transformer.jsonl_to_dataframe(jsonl_filename2)  # Convertir JSONL a DataFrame
            self.transformer.show_dataframe_header(df)  # Mostrar el encabezado del DataFrame

            # Imprimir las columnas del DataFrame
            print("Columnas del DataFrame:", df.columns)
            return df
        except Exception as e:
            print(f"Error al procesar el archivo JSONL: {e}")
            return None




    

    def load_data_to_db(self, df):
        self.loader.print_dataframe(df)
        self.loader.show_tables()
        self.loader.insert_sample_data(df)





if __name__ == "__main__":
    # Obtener la URL completa con los argumentos
    API_URL = ETLController.obtener_url()
    print(f'URL completa: {API_URL}')
    
    # Instanciar y configurar el controlador ETL
    etl_controller = ETLController(API_URL)
    
    # Verificar la conexión con la API
    etl_controller.conexion_API()

    # Extraer y transformar los datos
    jsonl_filename2 = etl_controller.run_etl_extract()
    if jsonl_filename2:
        df = etl_controller.show_dataframe(jsonl_filename2)  # Transformar el JSONL a DataFrame

        # Cargar los datos en la base de datos
        etl_controller.load_data_to_db(df)
    
    



"""
https://github.com/sferez/BybitMarketData/raw/main/data/BTC/2024-02-12/trades_BTC_2024-02-12.zip
https://github.com/sferez/BybitMarketData/raw/main/data/ETH/2024-02-12/trades_ETH_2024-02-12.zip
https://github.com/sferez/BybitMarketData/raw/main/data/SOL/2024-02-12/trades_SOL_2024-02-12.zip

"""
