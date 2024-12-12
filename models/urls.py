import argparse

def obtener_url():
    # Crear el parser
    parser = argparse.ArgumentParser(description='URL para extraer datos.')

    # Agregar una opción para la URL base con un valor por defecto
    parser.add_argument('--base_url', type=str, default='https://github.com/sferez/BybitMarketData/raw/main/data/', help='URL base')

    # Agregar una opción para la parte variable de la URL
    parser.add_argument('-r', '--url', type=str, required=True, help='Parte variable de la URL')

    # Parsear los argumentos
    args = parser.parse_args()

    # Crear la URL completa concatenando la base URL con la parte variable
    full_url = args.base_url + args.url

    # Retornar la URL completa
    return full_url

# Llamar a la función y obtener la URL
API_URL = obtener_url()

# Imprimir la URL completa
print(f'URL completa: {API_URL}')
