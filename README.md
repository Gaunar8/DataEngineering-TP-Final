# Trabajo Integrador Final Rodrigo Gauna

## Instalación
```bash
python3 -m venv venv
source venv/bin/activate
pip install deltalake pandas requests pyarrow
```

## Ejecución
```bash
python RodrigoGauna_TP1.py   # Extrae datos y guarda en Delta Lake raw
python RodrigoGauna_TP2.py   # Procesa y guarda en Delta Lake processed
```

## API utilizada
Open-Meteo (https://open-meteo.com/) — gratuita, sin API key requerida.

## Endpoints
- `/v1/forecast` — datos climáticos horarios (extracción incremental)
- `/v1/search`   — metadatos de ciudades (extracción full)

## Ciudades
Buenos Aires, Córdoba, Rosario, Viedma, Mendoza