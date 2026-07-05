# =============================================================================
# RodrigoGauna_TP2.py - Módulo 2: Procesamiento de datos
# Data Engineering - UTN BA / Centro de e-Learning
# =============================================================================
# Lee los datos crudos almacenados por TP1 y aplica 6 transformaciones:
#   T1 - Eliminación de duplicados
#   T2 - Conversión de tipos de datos
#   T3 - Renombrado de columnas al español
#   T4 - Creación de columnas derivadas (categoría, alerta viento, sensación térmica)
#   T5 - JOIN con metadatos de ciudades (enriquecimiento)
#   T6 - Agregación diaria por ciudad (GROUP BY con MAX, MIN, AVG, SUM)
#
# Resultados guardados en Delta Lake capa 'processed'.
# =============================================================================

import os
import pandas as pd
from datetime import datetime
from deltalake import DeltaTable, write_deltalake

from config import (
    RUTA_RAW_CLIMA,
    RUTA_RAW_CIUDADES,
    RUTA_PROCESSED_CLIMA,
    RUTA_PROCESSED_RESUMEN,
)


def leer_delta(ruta: str, nombre: str) -> pd.DataFrame:
    """Lee una tabla Delta Lake y la retorna como DataFrame de Pandas."""
    print(f"\n  Leyendo {nombre} desde: {ruta}")
    tabla = DeltaTable(ruta)
    df = tabla.to_pandas()
    print(f"  [OK] {len(df)} filas | {len(df.columns)} columnas")
    return df


# --- T1: Eliminación de duplicados ---

def t1_eliminar_duplicados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina filas duplicadas usando ciudad + timestamp como clave única.
    Puede haber solapamientos si el TP1 se ejecuta más de una vez en el mismo período.
    """
    total_antes = len(df)
    df = df.drop_duplicates(subset=["city", "time_utc"], keep="last")
    eliminados = total_antes - len(df)
    print(f"  T1 [Duplicados] Eliminados: {eliminados} | Restantes: {len(df)}")
    return df


# --- T2: Conversión de tipos de datos ---

def t2_convertir_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte columnas al tipo correcto para análisis:
      - time_utc     → datetime64 (necesario para ordenamiento y agrupación por fecha)
      - weather_code → int        (viene como float por posibles nulos en la API)
      - humidity_pct → int        (porcentaje, no tiene sentido como decimal)
    """
    df["time_utc"]     = pd.to_datetime(df["time_utc"])
    df["weather_code"] = df["weather_code"].fillna(0).astype(int)
    df["humidity_pct"] = df["humidity_pct"].fillna(0).astype(int)
    print(f"  T2 [Tipos] time_utc→datetime, weather_code→int, humidity_pct→int")
    return df


# --- T3: Renombrado de columnas ---

def t3_renombrar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas de inglés técnico a español descriptivo.
    Mejora la legibilidad y estandariza la nomenclatura del data lake.
    """
    mapa_columnas = {
        "time":                 "time_utc",
        "temperature_2m":       "temperatura_c",
        "relative_humidity_2m": "humidity_pct",
        "wind_speed_10m":       "viento_kmh",
        "precipitation":        "precipitacion_mm",
        "weather_code":         "weather_code",
        "ciudad":               "city",
        "latitud":              "latitud",
        "longitud":             "longitud",
        "fecha_extraccion":     "fecha_extraccion",
        "fecha":                "fecha",
        "hora":                 "hora",
    }
    # Solo renombrar columnas que existan en el DataFrame (evita errores)
    mapa_valido = {k: v for k, v in mapa_columnas.items() if k in df.columns}
    df = df.rename(columns=mapa_valido)
    print(f"  T3 [Rename] Columnas renombradas: {list(mapa_valido.values())}")
    return df


# --- T4: Creación de columnas derivadas ---

def t4_crear_columnas_derivadas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea nuevas columnas a partir de lógica de negocio:
      - temperatura_categoria: clasifica cada hora como Frío / Templado / Cálido
      - alerta_viento:         True si el viento supera 50 km/h
      - sensacion_termica_c:   fórmula wind chill (Environment Canada),
                               válida solo para T ≤ 10°C y viento > 4.8 km/h
    """
    def categorizar_temperatura(t):
        if t < 10:
            return "Frío"
        elif t < 20:
            return "Templado"
        else:
            return "Cálido"

    df["temperatura_categoria"] = df["temperatura_c"].apply(categorizar_temperatura)

    UMBRAL_VIENTO_KMH = 50
    df["alerta_viento"] = df["viento_kmh"] > UMBRAL_VIENTO_KMH

    def calcular_sensacion_termica(row):
        t = row["temperatura_c"]
        v = row["viento_kmh"]
        if t <= 10 and v > 4.8:
            wc = (13.12 + 0.6215 * t
                  - 11.37 * (v ** 0.16)
                  + 0.3965 * t * (v ** 0.16))
            return round(wc, 1)
        return t  # Si no aplica wind chill, se usa la temperatura real

    df["sensacion_termica_c"] = df.apply(calcular_sensacion_termica, axis=1)
    print(f"  T4 [Derivadas] Columnas creadas: temperatura_categoria, alerta_viento, sensacion_termica_c")
    return df


# --- T5: JOIN con metadatos de ciudades ---

def t5_join_con_metadata(df_clima: pd.DataFrame, df_meta: pd.DataFrame) -> pd.DataFrame:
    """
    Cruza el DataFrame de clima con los metadatos de ciudades (LEFT JOIN).
    Enriquece cada registro horario con: elevación, provincia, zona horaria y población.
    LEFT JOIN garantiza que no se pierdan registros de clima aunque falte algún metadato.
    """
    columnas_meta = ["ciudad_nombre", "elevacion_m", "provincia", "zona_horaria", "poblacion"]
    df_meta_reducido = df_meta[columnas_meta].copy()

    df_enriquecido = pd.merge(
        df_clima,
        df_meta_reducido,
        left_on="city",
        right_on="ciudad_nombre",
        how="left"
    ).drop(columns=["ciudad_nombre"])  # Eliminar columna duplicada generada por el join

    print(f"  T5 [JOIN] Filas antes: {len(df_clima)} | Después: {len(df_enriquecido)}")
    print(f"       Columnas agregadas: elevacion_m, provincia, zona_horaria, poblacion")
    return df_enriquecido


# --- T6: Agregación diaria (GROUP BY) ---

def t6_resumen_diario(df: pd.DataFrame) -> pd.DataFrame:
    """
    Genera un resumen estadístico diario agrupando por ciudad y fecha.
    Aplica MAX, MIN, AVG sobre temperatura y viento; SUM sobre precipitación;
    y conteo de horas con alerta de viento.
    """
    resumen = (
        df.groupby(["city", "fecha"])
        .agg(
            temp_max_c          = ("temperatura_c",       "max"),
            temp_min_c          = ("temperatura_c",       "min"),
            temp_promedio_c     = ("temperatura_c",       "mean"),
            sensacion_max_c     = ("sensacion_termica_c", "max"),
            sensacion_min_c     = ("sensacion_termica_c", "min"),
            humedad_promedio    = ("humidity_pct",        "mean"),
            viento_max_kmh      = ("viento_kmh",          "max"),
            viento_promedio     = ("viento_kmh",          "mean"),
            precipitacion_total = ("precipitacion_mm",    "sum"),
            horas_con_alerta    = ("alerta_viento",       "sum"),  # True cuenta como 1
            registros_horarios  = ("temperatura_c",       "count"),
        )
        .reset_index()
    )

    cols_redondear = ["temp_promedio_c", "sensacion_max_c", "sensacion_min_c",
                      "humedad_promedio", "viento_promedio", "precipitacion_total"]
    resumen[cols_redondear] = resumen[cols_redondear].round(2)

    print(f"  T6 [GROUP BY] Resumen diario: {len(resumen)} filas (ciudad × fecha)")
    return resumen


# --- Guardado en Delta Lake (capa processed) ---

def guardar_processed_clima(df: pd.DataFrame) -> None:
    """
    Guarda el clima procesado en la capa processed con modo 'overwrite'.
    Se recalcula íntegramente en cada ejecución para incluir todas las transformaciones.
    """
    os.makedirs(RUTA_PROCESSED_CLIMA, exist_ok=True)
    print(f"\n  Guardando clima procesado en: {RUTA_PROCESSED_CLIMA}")

    df_guardar = df.copy()
    # Convertir datetime a string para compatibilidad con el escritor de Delta Lake
    if pd.api.types.is_datetime64_any_dtype(df_guardar["time_utc"]):
        df_guardar["time_utc"] = df_guardar["time_utc"].astype(str)

    write_deltalake(
        RUTA_PROCESSED_CLIMA,
        df_guardar,
        mode="overwrite",
        schema_mode="overwrite",
    )
    print(f"  [OK] {len(df_guardar)} filas guardadas | Modo: overwrite")


def guardar_resumen_diario(df: pd.DataFrame) -> None:
    """Guarda el resumen diario agregado en la capa processed con modo 'overwrite'."""
    os.makedirs(RUTA_PROCESSED_RESUMEN, exist_ok=True)
    print(f"\n  Guardando resumen diario en: {RUTA_PROCESSED_RESUMEN}")

    write_deltalake(
        RUTA_PROCESSED_RESUMEN,
        df,
        mode="overwrite",
        schema_mode="overwrite",
    )
    print(f"  [OK] {len(df)} filas guardadas | Modo: overwrite")


def main():
    print("\n" + "="*60)
    print("  TP2 - PROCESAMIENTO Y TRANSFORMACIÓN DE DATOS")
    print(f"  Ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    # Paso 1: leer datos crudos desde Delta Lake raw
    print("\n[1] LECTURA DE DATOS CRUDOS (Delta Lake raw)")
    df_clima = leer_delta(RUTA_RAW_CLIMA,    "clima horario")
    df_meta  = leer_delta(RUTA_RAW_CIUDADES, "metadatos ciudades")

    # Paso 2: aplicar pipeline de transformaciones en orden
    print("\n[2] APLICANDO TRANSFORMACIONES")
    df = t3_renombrar_columnas(df_clima)  # T3 primero: los demás usan los nombres nuevos
    df = t1_eliminar_duplicados(df)
    df = t2_convertir_tipos(df)
    df = t4_crear_columnas_derivadas(df)
    df_enriquecido = t5_join_con_metadata(df, df_meta)
    df_resumen     = t6_resumen_diario(df_enriquecido)

    # Paso 3: guardar resultados en capa processed
    print("\n[3] GUARDANDO RESULTADOS EN DELTA LAKE (processed)")
    guardar_processed_clima(df_enriquecido)
    guardar_resumen_diario(df_resumen)

    print("\n" + "="*60)
    print("  PROCESO TP2 FINALIZADO EXITOSAMENTE")
    print(f"  Registros procesados:           {len(df_enriquecido)}")
    print(f"  Filas en resumen diario:        {len(df_resumen)}")
    print(f"  Transformaciones aplicadas:     6")
    print(f"  Destino processed/clima:        {RUTA_PROCESSED_CLIMA}")
    print(f"  Destino processed/resumen:      {RUTA_PROCESSED_RESUMEN}")
    print("="*60 + "\n")

    print("VISTA PREVIA - Resumen diario por ciudad:")
    print(df_resumen[["city", "fecha", "temp_max_c", "temp_min_c",
                       "temp_promedio_c", "precipitacion_total",
                       "horas_con_alerta"]].to_string(index=False))


if __name__ == "__main__":
    main()
