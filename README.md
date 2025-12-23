# Pipeline en Apache Beam de Procesamiento de Datos

Este proyecto consiste en el desarrollo de un pipeline de datos utilizando Apache Beam para procesar y enriquecer informacion de la Liga de Carreras de Helicopteros (HRL). El objetivo es estandarizar registros de interaccion de usuarios y complementarlos con datos demograficos de paises.

## Ejecucion del Proyecto

### 1. Clonar el Repositorio

```bash
git clone https://github.com/mgodoydiaz/TareaDE_apachebeam.git
cd TareaDE_apachebeam

```

### 2. Instalacion del Entorno

**Opcion 1: Conda (Local)**

```bash
conda env create -f beam_environment.yml
conda activate beam_env

```

**Opcion 2: Pip (Nube o Google Colab)**

```bash
pip install -r requirements.txt

```

### 3. Ejecucion del Pipeline

Para procesar los datos, ejecute el script principal indicando las rutas de entrada y salida:

```bash
python scripts/pipeline.py --input_path data/desafio1 --output_path output

```

---

## Estructura de Archivos

```text
TAREADE_BEAM/
├── data/
│   ├── desafio1/
│   │   ├── country_data_v2.csv
│   │   ├── cup25_fan_engagement-000-of-001.json
│   │   ├── league04_fan_engagement-000-of-001.json
│   │   └── race11_fan_engagement-000-of-001.json
│   └── desafio3/
├── output/
│   └── processed_data.json
├── scripts/
│   ├── etl_tools.py
│   └── pipeline.py
├── beam_environment.yml
├── requirements.txt
├── .gitignore
└── README.md

```

---

## Pasos del Pipeline

El proceso de transformacion se divide en las siguientes etapas:

1. **Carga de datos:** Lectura de multiples archivos JSON y archivos CSV utilizando un modulo de herramientas personalizado.
2. **Estandarizacion:** Limpieza del campo RaceID para eliminar espacios, guiones y otros caracteres especiales, convirtiendo el texto a minusculas y dejando un formato de <string><integer>
3. **Filtrado:** Eliminacion de registros donde el tipo de dispositivo sea igual a "Other".
4. **Enriquecimiento:** Cruce de informacion entre el JSON y el CSV mediante Side Inputs, agregando una estructura anidada con datos del pais de origen.
5. **Escritura:** Generacion de un archivo de salida en formato JSON Lines serializado.

---