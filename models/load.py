import pandas as pd
import os,uuid,random,sys
from datetime import datetime
from sqlalchemy import create_engine, text,inspect
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DataConexion
from models.transform import Transformer  # Asegúrate de importar tu clase Transformer
from models.models import TipoTransaccion,Transacciones,Fecha



class DataLoader:
    def __init__(self):
        self.data_conexion = DataConexion()
        self.engine = self.data_conexion.engine
        self.Session = sessionmaker(bind=self.engine)

    def print_dataframe(self, df):
        if df is not None and not df.empty:
            print("DataFrame cargado en el archivo load.py :")
            print(df)
        else:
            print("No se pudo cargar el DataFrame. El archivo podría estar vacío o malformado.")

    def show_tables(self):
        """Mostrar las tablas de la base de datos."""
        with self.Session() as session:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()  # Obtener el nombre de las tablas
            if tables:
                print("Tablas en la base de datos:")
                for table in tables:
                    print(f"- {table}")
            else:
                print("No se encontraron tablas en la base de datos.")



    


    def insert_sample_data(self, df):
        """Insertar registros de ejemplo en las tablas usando un DataFrame."""
        with self.Session() as session:
            # Iterar sobre cada fila del DataFrame
            for index, row in df.iterrows():
                # Insertar un registro en tipo_transaccion basado en la columna 'L'
                tipo_transaccion = TipoTransaccion(descripcion=row['L'])  # 'L' es el tipo de tick
                session.add(tipo_transaccion)
                session.commit()  # Confirmar la transacción para guardar el registro

                # Obtener el ID del tipo de transacción recién insertado
                tipo_transaccion_id = tipo_transaccion.id

                # Desglosar la columna 'T' para obtener la fecha y crear un registro en fecha
                timestamp = row['T'] / 1000  # Convertir de milisegundos a segundos
                fecha_datetime = datetime.fromtimestamp(timestamp)

                # Insertar un registro en fecha
                fecha = Fecha(
                    fecha=fecha_datetime.date(),
                    dia=fecha_datetime.day,
                    mes=fecha_datetime.month,
                    anio=str(fecha_datetime.year)
                )
                session.add(fecha)
                session.commit()  # Confirmar la transacción para guardar el registro

                # Obtener el ID de la fecha recién insertada
                fecha_id = fecha.id

                # Insertar un registro en Transacciones usando el ID de la transacción del DataFrame
                transaccion = Transacciones(
                    fecha_id=fecha_id,
                    transaccion_tipo_id=tipo_transaccion_id,
                    simbolo=row['s'],  # 's' es el símbolo
                    precio=row['p'],    # 'p' es el precio
                    valor=row['v'],     # 'v' es el volumen
                )
                session.add(transaccion)
                session.commit()  # Confirmar la transacción

            print("Registros de ejemplo insertados en la base de datos.")