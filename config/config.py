# config.py

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, exc

load_dotenv()

DATABASE_CONFIG = {
    'server': os.getenv('DB_SERVER'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_DATABASE'),
    'username': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD')
}

def get_connection_string():
    server = DATABASE_CONFIG['server']
    port = DATABASE_CONFIG['port']
    database = DATABASE_CONFIG['database']
    username = DATABASE_CONFIG['username']
    password = DATABASE_CONFIG['password']

    connection_string = (
        f"postgresql+psycopg2://{username}:{password}@{server}:{port}/{database}"
    )
    
    return connection_string

class DataConexion:
    def __init__(self):
        self.connection_string = get_connection_string()
        self.engine = create_engine(self.connection_string)

    def check_connection(self):
        """Verificar conexión a la base de datos."""
        try:
            with self.engine.connect() as connection:
                print("Conexión exitosa a la base de datos.")
                return True
        except exc.SQLAlchemyError as e:
            print(f"Error conectando a la base de datos: {e}")
            return False
