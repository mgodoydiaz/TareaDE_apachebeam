"""Script de pipeline de Apache Beam para procesar datos de ejemplo.
Recibe como parámetros argparse un directorio de entrada y uno de salida.
Lee los archivos json del directorio de entrada, también los csv."""

import argparse
import os
import apache_beam as beam  
from apache_beam.options.pipeline_options import PipelineOptions

# Se importa el módulo de tooling en la carpeta tools
import load_tools as lt

# Se cargan los archivos JSON y CSV usando las funciones del módulo de tooling