import pandas as pd
import json

"""
    The trade data is collected from the trade.{symbol} channel, which provides the following information:
    T: the timestamp of the trade
    s: the trading pair
    S: the side of the trade (Buy or Sell)
    v: the quantity of the trade
    p: the price of the trade
    L: the tick direction of the trade
    i: the trade ID
    BT: whether the trade is a block trade

"""



class Transformer:
    def __init__(self, extractor):  # Recibimos la instancia de Extractor
        self.extractor = extractor  # Almacenamos la instancia de Extractor

    def jsonl_to_dataframe(self, jsonl_filename):
        """Convertir un archivo JSONL a un DataFrame de pandas."""
        data = self.read_jsonl_data(jsonl_filename)  # Leer los datos del archivo JSONL
        df = pd.DataFrame(data)  # Crear el DataFrame a partir de la lista de datos

        # Procesar la columna 'd' para expandirla en nuevas columnas
        if 'd' in df.columns:
            df = self.expand_column_d(df)

        # Eliminar filas que contienen al menos un valor nulo
        df = self.remove_rows_with_nulls(df)

        return df

    def read_jsonl_data(self, jsonl_filename):
        """Leer datos de un archivo JSONL y devolver una lista de diccionarios."""
        data = []
        for line in self.extractor.read_jsonl_file(jsonl_filename):  # Usamos la instancia de Extractor para leer el archivo
            data.append(line)
        return data

    def expand_column_d(self, df):
        """Expandir la columna 'd' que contiene listas de diccionarios en nuevas columnas."""
        # Expande la columna 'd' en varias columnas basadas en las claves del diccionario
        d_expanded = pd.json_normalize(df['d'].explode())  # Normaliza la columna 'd' (que tiene listas de diccionarios)

        # Concatenar las nuevas columnas con el DataFrame original
        df = pd.concat([df.drop(columns=['d']), d_expanded], axis=1)

        return df

    def remove_rows_with_nulls(self, df):
        """Eliminar filas que contienen valores nulos en alguna columna."""
        return df.dropna()  # Elimina todas las filas que tienen al menos un valor nulo

    def show_dataframe_header(self, df):
        """Imprimir el encabezado, las primeras filas del DataFrame, cantidad de columnas y filas, y valores nulos."""
        if not df.empty:  # Verificar que el DataFrame no esté vacío
            print("Columnas del DataFrame:")
            print(df.columns.tolist())  # Muestra todas las columnas del DataFrame
            print("\nEncabezado del DataFrame:")
            print(df.head())  # Mostrar las primeras filas del DataFrame
            
            # Mostrar la cantidad de filas y columnas
            print(f"\nCantidad de columnas: {df.shape[1]}")  # Número de columnas
            print(f"Cantidad de filas: {df.shape[0]}")  # Número de filas
            print(f'El tipe de dataframe es => {type(df)}')
            

        else:
            print("El DataFrame está vacío. No se puede mostrar encabezado.")
