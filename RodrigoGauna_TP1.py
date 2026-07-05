# =============================================================================
# RodrigoGauna_TP1.py - Módulo 1: Extracción y almacenamiento de datos
# Data Engineering - UTN BA / Centro de e-Learning
# =============================================================================
# FUENTE: Open-Meteo API (https://open-meteo.com/) - gratuita, sin API key.
#
# ENDPOINTS:
#   /v1/forecast  → datos climáticos horarios (temporal → extracción INCREMENTAL)
#   /v1/search    → metadatos de ciudades     (estático → extracción FULL)
#
# ALMACENAMIENTO: Delta Lake
#   raw/clima_horario/     → modo append, particionado por fecha y hora
#   raw/ciudades_metadata/ → modo overwrite (datos estáticos, se reemplazan)
# =============================================================================

import os
import requests
import pandas as pd
from datetime import datetime
from deltalake import write_deltalake

from config import (
    CIUDADES,
    URL_FORECAST,
    URL_GEOCODING,
    VARIABLES_HORARIAS,
    RUTA_RAW_CLIMA,
    RUTA_RAW_CIUDADES,
)


def crear_directorio_si_no_existe(ruta: str) -> None:
    """Crea el directorio indicado si aún no existe."""
    os.makedirs(ruta, exist_ok=True)
    print(f"  [DIR] Directorio listo: {ruta}")


def verificar_respuesta_api(respuesta: requests.Response, contexto: str) -> dict:
    """Valida que la respuesta HTTP sea exitosa y retorna el JSON parseado."""
    if respuesta.status_code != 200:
        raise ConnectionError(
            f"Error en {contexto}: HTTP {respuesta.status_code} - {respuesta.text[:200]}"
        )
    datos = respuesta.json()
    print(f"  [OK] Respuesta recibida de {contexto} (status 200)")
    return datos


# --- ENDPOINT 1: Datos temporales (extracción INCREMENTAL) ---

def extraer_clima_horario(ciudad: dict) -> pd.DataFrame:
    """
    Extrae datos climáticos horarios del día actual para una ciudad.
    Extracción INCREMENTAL: solo trae el período reciente para agregar
    nuevas filas sin repetir el historial completo.
    """
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")

    params = {
        "latitude":        ciudad["latitud"],
        "longitude":       ciudad["longitud"],
        "hourly":          ",".join(VARIABLES_HORARIAS),
        "start_date":      fecha_hoy,
        "end_date":        fecha_hoy,  # Solo el día actual (incremental)
        "timezone":        "America/Argentina/Buenos_Aires",
        "wind_speed_unit": "kmh",
    }

    print(f"\n  Extrayendo clima horario para: {ciudad['nombre']} ({fecha_hoy})")
    respuesta = requests.get(URL_FORECAST, params=params, timeout=15)
    datos = verificar_respuesta_api(respuesta, f"forecast/{ciudad['nombre']}")

    df = pd.DataFrame(datos["hourly"])

    # Agregar columnas de contexto para identificar el origen del registro
    df["ciudad"]           = ciudad["nombre"]
    df["latitud"]          = ciudad["latitud"]
    df["longitud"]         = ciudad["longitud"]
    df["fecha_extraccion"] = datetime.now().isoformat()

    # Columnas de partición derivadas del timestamp
    df["fecha"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%d")
    df["hora"]  = pd.to_datetime(df["time"]).dt.strftime("%H")

    print(f"  [DATOS] {len(df)} registros horarios obtenidos para {ciudad['nombre']}")
    return df


def extraer_clima_todas_ciudades() -> pd.DataFrame:
    """Itera sobre todas las ciudades y consolida los datos climáticos en un único DataFrame."""
    print("\n" + "="*60)
    print("EXTRACCIÓN INCREMENTAL - Clima horario (Endpoint 1)")
    print("="*60)

    lista_dfs = []
    for ciudad in CIUDADES:
        df_ciudad = extraer_clima_horario(ciudad)
        lista_dfs.append(df_ciudad)

    df_total = pd.concat(lista_dfs, ignore_index=True)
    print(f"\n  [TOTAL] {len(df_total)} registros combinados de {len(CIUDADES)} ciudades")
    return df_total


def guardar_clima_delta(df: pd.DataFrame) -> None:
    """
    Guarda el DataFrame de clima en Delta Lake con modo 'append'.
    Particiona por fecha y hora: cada ejecución agrega una nueva partición
    sin pisar los datos históricos ya almacenados.
    """
    crear_directorio_si_no_existe(RUTA_RAW_CLIMA)
    print(f"\n  Guardando en Delta Lake: {RUTA_RAW_CLIMA}")
    print(f"  Modo: append | Particiones: fecha / hora")

    write_deltalake(
        RUTA_RAW_CLIMA,
        df,
        mode="append",
        partition_by=["fecha", "hora"],  # Particionamiento para eficiencia de lectura
    )
    print(f"  [OK] Datos de clima guardados correctamente en Delta Lake")


# --- ENDPOINT 2: Datos estáticos (extracción FULL) ---

def extraer_metadata_ciudad(ciudad: dict):
    """
    Extrae metadatos de una ciudad desde la API de geocodificación.
    Devuelve la primera coincidencia (más relevante) o None si no hay resultados.
    """
    params = {
        "name":     ciudad["nombre"],
        "count":    1,        # Solo el resultado más relevante
        "language": "es",
        "format":   "json",
    }

    print(f"\n  Extrayendo metadatos para: {ciudad['nombre']}")
    respuesta = requests.get(URL_GEOCODING, params=params, timeout=15)
    datos = verificar_respuesta_api(respuesta, f"geocoding/{ciudad['nombre']}")

    if "results" not in datos or len(datos["results"]) == 0:
        print(f"  [AVISO] No se encontraron metadatos para {ciudad['nombre']}")
        return None

    resultado = datos["results"][0]

    # Normalizar a DataFrame seleccionando solo los campos relevantes
    df = pd.DataFrame([{
        "ciudad_nombre":    ciudad["nombre"],
        "geo_id":           resultado.get("id"),
        "geo_nombre":       resultado.get("name"),
        "latitud":          resultado.get("latitude"),
        "longitud":         resultado.get("longitude"),
        "elevacion_m":      resultado.get("elevation"),
        "pais":             resultado.get("country"),
        "codigo_pais":      resultado.get("country_code"),
        "provincia":        resultado.get("admin1"),
        "zona_horaria":     resultado.get("timezone"),
        "poblacion":        resultado.get("population"),
        "fecha_extraccion": datetime.now().isoformat(),
    }])

    print(f"  [DATOS] Metadatos obtenidos: {resultado.get('name')}, {resultado.get('country')}")
    return df


def extraer_metadata_todas_ciudades() -> pd.DataFrame:
    """
    Itera sobre todas las ciudades y consolida sus metadatos.
    Extracción FULL: se reemplaza el conjunto completo en cada ejecución.
    """
    print("\n" + "="*60)
    print("EXTRACCIÓN FULL - Metadatos de ciudades (Endpoint 2)")
    print("="*60)

    lista_dfs = []
    for ciudad in CIUDADES:
        df_ciudad = extraer_metadata_ciudad(ciudad)
        if df_ciudad is not None:
            lista_dfs.append(df_ciudad)

    df_total = pd.concat(lista_dfs, ignore_index=True)
    print(f"\n  [TOTAL] Metadatos obtenidos para {len(df_total)} ciudades")
    return df_total


def guardar_metadata_delta(df: pd.DataFrame) -> None:
    """
    Guarda los metadatos en Delta Lake con modo 'overwrite'.
    Al ser datos estáticos, se reemplaza el contenido completo en cada
    ejecución (extracción full), sin necesidad de particionamiento.
    """
    crear_directorio_si_no_existe(RUTA_RAW_CIUDADES)
    print(f"\n  Guardando en Delta Lake: {RUTA_RAW_CIUDADES}")
    print(f"  Modo: overwrite (datos estáticos, extracción full)")

    write_deltalake(
        RUTA_RAW_CIUDADES,
        df,
        mode="overwrite",
    )
    print(f"  [OK] Metadatos de ciudades guardados correctamente en Delta Lake")


def main():
    print("\n" + "="*60)
    print("  TP1 - EXTRACCIÓN Y ALMACENAMIENTO DE DATOS")
    print("  Fuente: Open-Meteo API")
    print(f"  Ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    # Extracción INCREMENTAL: datos temporales (clima horario)
    df_clima = extraer_clima_todas_ciudades()
    guardar_clima_delta(df_clima)

    # Extracción FULL: datos estáticos (metadatos de ciudades)
    df_ciudades = extraer_metadata_todas_ciudades()
    guardar_metadata_delta(df_ciudades)

    print("\n" + "="*60)
    print("  PROCESO TP1 FINALIZADO EXITOSAMENTE")
    print(f"  Registros de clima guardados:    {len(df_clima)}")
    print(f"  Ciudades con metadatos:          {len(df_ciudades)}")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
