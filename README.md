# Pipeline Apache Beam

## Clonar repositorio
```bash
git clone https://github.com/mgodoydiaz/TareaDE_apachebeam.git
cd TareaDE_apachebeam
```

## Instalacion en local

### Opcion 1: conda (beam_environment.yml)
```bash
conda env create -f beam_environment.yml
conda activate beam_env
```

### Opcion 2: pip (Google Colab)
```bash
!pip install -r requirements.txt
```

## Ejecutar el pipeline
Ejemplo con argumentos por consola:
```bash
python scripts/pipeline.py --input_path data --output_path output
```
