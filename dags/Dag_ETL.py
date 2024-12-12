from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import requests

# URL de ejemplo; puedes modificarla o pasarla como argumento en el DAG
API_URL = "https://github.com/sferez/BybitMarketData/raw/main/data/BTC/2024-02-12/trades_BTC_2024-02-12.zip"

# Agregar el directorio raíz del proyecto al sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from plugins.ETLController import ETLController  # Asegúrate de que este controlador esté disponible en el PYTHONPATH

# Instanciar el controlador ETL
etl_controller = ETLController(API_URL)

# Configuración básica del DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
with DAG(
    dag_id='etl_controller_dag',
    default_args=default_args,
    description='DAG que ejecuta el ETL utilizando el controlador.',
    schedule_interval="@daily",  # Puedes cambiar este intervalo según sea necesario
    start_date=datetime(2024, 12, 11),
    catchup=False
) as dag:

    # Tarea 1: Verificar conexión con la API
    def check_api_connection(**kwargs):
        """Verificar conexión a la API."""
        response = requests.get(API_URL)
        if response.status_code != 200:
            raise Exception("Conexión fallida con la API")
        print("Conexión exitosa con la API")

    check_api_connection_task = PythonOperator(
        task_id='check_api_connection',
        python_callable=check_api_connection,
        provide_context=True,
    )

    # Tarea 2: Extraer los datos
    def extract_task(**kwargs):
        """Extraer datos y devolver el nombre del archivo JSONL."""
        jsonl_filename = etl_controller.run_etl_extract()
        kwargs['ti'].xcom_push(key='jsonl_filename', value=jsonl_filename)

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_task,
        provide_context=True,
    )

    # Tarea 3: Transformar los datos
    def transform_task(**kwargs):
        """Transformar los datos extraídos y devolver el DataFrame."""
        jsonl_filename = kwargs['ti'].xcom_pull(key='jsonl_filename', task_ids='extract_data')
        df = etl_controller.show_dataframe(jsonl_filename)
        kwargs['ti'].xcom_push(key='df', value=df)  # Guardar el DataFrame en XCom

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task,
        provide_context=True,
    )

    # Tarea 4: Cargar los datos a la base de datos
    def load_task(**kwargs):
        """Cargar los datos transformados en la base de datos."""
        df = kwargs['ti'].xcom_pull(key='df', task_ids='transform_data')
        etl_controller.load_data_to_db(df)

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_task,
        provide_context=True,
    )

    # Definir dependencias entre las tareas
    check_api_connection_task >> extract_data >> transform_data >> load_data
