"""Script de pipeline de Apache Beam para procesar datos de ejemplo.
Recibe como parámetros argparse un directorio de entrada y uno de salida.
Lee los archivos json del directorio de entrada, también los csv."""

import argparse
import os
import json
import logging
import apache_beam as beam  
from apache_beam.options.pipeline_options import PipelineOptions

# Se importa el módulo de tooling dentro de la misma carpeta de scripts
import etl_tools as lt

# Se cargan los archivos JSON y CSV usando las funciones del módulo de tooling
def normalize_and_filter(element):
    """Funcion para normalizar el campo Race ID, pasando todo a minusculas, y quitar espacios y guiones bajos.
    Además se filtran los registros que DeviceType es distinto de 'Other'."""
    # Normal
    race_id = element.get('RaceID', '')
    if race_id:
        dic_replace = {" ": "", "_": "", "-": "", ":": ""}
        for old, new in dic_replace.items():
            race_id = race_id.replace(old, new)
        element['RaceID'] = race_id.strip().lower()
    # Filtrado distinto de other y distinto de None
    if element.get('DeviceType', '').lower() != 'other':
        return element

def enrich_data(element, csv_data_dict):
    """Función para enriquecer los datos JSON con información de un CSV."""
    # Cada elemento JSON se enrique desde el campo ViewerLocationCountry para buscar información correspondiente en el diccionario de csv_data
    # Se reemplaza el valor de ViewerLocationCountry por una estructura anidada que contiene los datos de las columnas
    # Country, Capital, Continent, Main Official Language y Currency
    
    # Extracción del pais en data json
    country = element.get('ViewerLocationCountry', '').strip()
    # Búsqueda en el diccionario del CSV
    country_info = csv_data_dict.get(country)

    # En caso de encontrar información, se crea la estructura anidada
    if country_info:
        element['ViewerLocationCountry'] = {
            'Country': country_info.get('Country'),
            'Capital': country_info.get('Capital'),
            'Continent': country_info.get('Continent'),
            'Main Official Language': country_info.get('Main_Official_Language'),
            'Currency': country_info.get('Currency')
        }
    return element    

def run():
    """Función principal para ejecutar el pipeline de Apache Beam."""
    parser = argparse.ArgumentParser(description="Pipeline de Apache Beam para procesar datos JSON y CSV.")
    parser.add_argument("--input_path", required=True, help="Ruta del directorio de entrada que contiene archivos JSON y CSV.")
    parser.add_argument("--output_path", required=True, help="Ruta del directorio de salida para guardar los resultados procesados.")
    known_args, pipeline_args = parser.parse_known_args()

    os.makedirs(known_args.output_path, exist_ok=True)

    # Configuración de las opciones del pipeline
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Cargar datos JSON
        json_data = lt.load_json_files(pipeline, known_args.input_path)

        # Cargar datos CSV
        csv_data = lt.load_csv_files(pipeline, known_args.input_path)
        csv_dict = lt.PCollection2Dict(csv_data, key_field='Country')

        # Se eliminan los campos cuyo valor es None y se aplican las transformaciones
        # Aplicar transformaciones: normalización y filtrado.
        processed_data = (
            json_data 
            | "Normalizar Y Filtrar" >> beam.Map(normalize_and_filter)
            | "Filtrar Nones" >> beam.Filter(lambda x: x is not None)
            | "Enriquecer Datos" >> beam.Map(enrich_data, csv_data_dict=beam.pvalue.AsDict(csv_dict))
        )

        # Escribir los datos procesados en formato JSON line-delimited
        output_file = os.path.join(known_args.output_path, "processed_data")
        (processed_data 
         | "Convertir a JSON" >> beam.Map(json.dumps)
         | "Escribir Datos Procesados" >> beam.io.WriteToText(output_file, file_name_suffix=".json", shard_name_template='')
        )
        
if __name__ == "__main__":
    # Esto es bastante interesante para ver logs en consola, 
    # los niveles son DEBUG, INFO, WARNING, ERROR, CRITICAL
    # Sirve especificamente para ver el progreso del pipeline a un nivel de DataFlow
    logging.getLogger().setLevel(logging.WARNING)
    run()
