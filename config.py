# Configuración central del proyecto - Data Engineering UTN BA
# Todas las rutas, URLs y parámetros se gestionan desde este archivo
# para mantener las credenciales y configuración fuera del código principal.

# Ciudades argentinas a consultar
CIUDADES = [
    {"nombre": "Buenos Aires", "latitud": -34.6037, "longitud": -58.3816},
    {"nombre": "Córdoba",      "latitud": -31.4201, "longitud": -64.1888},
    {"nombre": "Rosario",      "latitud": -32.9442, "longitud": -60.6505},
    {"nombre": "Viedma",       "latitud": -40.8135, "longitud": -62.9967},
    {"nombre": "Mendoza",      "latitud": -32.8908, "longitud": -68.8272},
]

# Endpoints de Open-Meteo API (gratuita, sin API key)
URL_FORECAST  = "https://api.open-meteo.com/v1/forecast"    # Datos climáticos horarios (temporal)
URL_GEOCODING = "https://geocoding-api.open-meteo.com/v1/search"  # Metadatos de ciudades (estático)

# Variables climáticas a extraer del endpoint de forecast
VARIABLES_HORARIAS = [
    "temperature_2m",        # Temperatura a 2 metros del suelo (°C)
    "relative_humidity_2m",  # Humedad relativa a 2 metros (%)
    "wind_speed_10m",        # Velocidad del viento a 10 metros (km/h)
    "precipitation",         # Precipitación acumulada (mm)
    "weather_code",          # Código WMO de condición meteorológica
]

DIAS_INCREMENTALES = 1  # Días hacia atrás para extracción incremental

# Rutas del data lake
RUTA_RAW_CLIMA    = "data_lake/raw/clima_horario"      # Datos crudos temporales (incremental)
RUTA_RAW_CIUDADES = "data_lake/raw/ciudades_metadata"  # Datos crudos estáticos (full)

RUTA_PROCESSED_CLIMA   = "data_lake/processed/clima_horario"  # Clima transformado y enriquecido
RUTA_PROCESSED_RESUMEN = "data_lake/processed/resumen_diario" # Resumen agregado por ciudad/día
