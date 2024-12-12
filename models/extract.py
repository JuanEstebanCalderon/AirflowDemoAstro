import zipfile
import requests
import io
import json


class Extractor:
    def __init__(self, api_url):
        self.api_url = api_url

    def extract_data_from_api(self):
        """Obtener el archivo JSONL desde la API sin descargarlo por completo."""
        response = requests.get(self.api_url)
        response.raise_for_status()  # Verificar que la solicitud fue exitosa

        # Abrir el archivo ZIP directamente desde la respuesta
        zipfile1 = zipfile.ZipFile(io.BytesIO(response.content))
        zipfile2 = zipfile1.namelist()  # Obtener los archivos dentro del ZIP

        # Suponemos que solo hay un archivo JSONL en el ZIP, el primero de la lista
        jsonl_filename = zipfile2[0] if zipfile2 else None
        return jsonl_filename  # Retornar el nombre del archivo JSONL
        

    def read_jsonl_file(self, jsonl_filename):
        """Leer los datos de un archivo JSONL dentro de un archivo ZIP sin descargarlo a disco."""
        with zipfile.ZipFile(io.BytesIO(requests.get(self.api_url).content)) as zipfile1:
            with zipfile1.open(jsonl_filename) as f:
                for line in f:
                    # Decodificar cada línea a JSON
                    yield json.loads(line.decode('utf-8'))
