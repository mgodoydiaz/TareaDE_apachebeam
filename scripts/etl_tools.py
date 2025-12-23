"""Tooling para leer datos usando apache beam"""

import csv
import json
import os
import apache_beam as beam

def load_json_files(pipeline, file_path):
    """Funcion para leer archivos JSON desde una ruta dada con Apache Beam."""
    # Se asume JSON line-delimited
    json_files = [f for f in os.listdir(file_path) if f.endswith(".json")]
    if not json_files:
        return pipeline | "CreateEmptyJson" >> beam.Create([])

    file_pattern = os.path.join(file_path, "*.json")
    return (
        pipeline
        | "ReadJsonFiles" >> beam.io.ReadFromText(file_pattern)
        | "ParseJson" >> beam.Map(json.loads)
    )


def load_csv_files(pipeline, file_path):
    """Funcion para leer archivos CSV, se devuelve un diccionario por fila, donde la llave es Country en minusculas."""
    # Filtrar archivos CSV, se asume que todos tienen la misma estructura
    csv_files = [f for f in os.listdir(file_path) if f.endswith(".csv")]
    if not csv_files:
        return pipeline | "CreateEmptyCsv" >> beam.Create([])

    header_path = os.path.join(file_path, csv_files[0])
    with open(header_path, "r", encoding="utf-8-sig") as f:
        header_line = f.readline()
    fieldnames = next(csv.reader([header_line]))

    file_pattern = os.path.join(file_path, "*.csv")
    return (
        pipeline
        | "ReadCsvFiles" >> beam.io.ReadFromText(file_pattern, skip_header_lines=1)
        | "ParseCsv"
        >> beam.Map(lambda line: dict(zip(fieldnames, next(csv.reader([line])))))
    )

def PCollection2Dict(pcoll, key_field):
    """Convierte un PCollection de diccionarios en un diccionario de Python usando un campo como llave."""
    def to_kv(element):
        key = element.get(key_field, '').strip()
        return (key, element)

    return (
        pcoll
        | "ToKeyValue" >> beam.Map(to_kv)
    )