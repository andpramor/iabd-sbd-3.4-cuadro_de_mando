import os

import polars as pl
from django.conf import settings

from .db_reader import get_polars_df_from_last_fetch

"""
# Utilizando Polars:
# PASO 2: Limpieza y Estructuración
# - Eliminar filas con nulos: en clean_nulls eliminamos filas con valores nulos en columnas clave (ID, timestamp_captura).
# - Eliminamos filas con datos horarios nulos.
# - get_hourly_weather_dataframe: limpiamos nulos en la precipitación (asumimos 0.0 y "none" si es nulo) y en la temperatura (forward fill). También filtramos temperaturas extremas.
# - get_stats_dataframe: eliminamos filas con temperaturas nulas y contamos los nulos en precipitación como 0 para el total diario.
# - get_current_weather_dataframe: filtramos solo filas donde 'current' no es nulo, y limpiamos nulos en precipitación y viento con valores por defecto (0 o "unknown").
"""


def clean_nulls(df):
    """
    Paso intermedio para limpiar el dataframe base de nulos antes de ramificar.
    """
    return df.drop_nulls(subset=["id", "timestamp_captura"])


"""
# PASO 3: Generación de Dataframes a partir de los datos limpios ---
# - Crear nuevas columnas calculadas: en df_stats, calculamos temperatura máxima, mínima y promedio.
# - Crear columnas agrupadas para segmentar la información por provincias, años o sectores: en df_stats agrupamos por ID.
# - Capa de plata (silver layer): exportar a CSV los dataframes limpios.
"""


def export_to_csv(df, filename):
    """Exportar un DataFrame de Polars a CSV."""
    output_dir = (
        f"{settings.BASE_DIR}/data_silver_layer"  # Directorio de salida para los CSVs
    )
    os.makedirs(output_dir, exist_ok=True)  # Crear el directorio si no existe

    full_path = f"{output_dir}/{filename}.csv"

    df.write_csv(full_path)
    print(f"Archivo exportado correctamente: {full_path}")


def get_hourly_weather_dataframe(df):
    """
    Datos del tiempo extraídos por franja horaria.
    Se corrige el error de parseo de fecha adaptándose al formato ISO T.
    """
    # 1. Selección y desanidado
    df_hourly = (
        df.select(["lat", "lon", "hourly"])
        .drop_nulls("hourly")
        .unnest("hourly")
        .explode("data")
        .unnest("data")
    )

    # 2. TRANSFORMACIÓN
    df_hourly = df_hourly.with_columns(
        [
            # Usamos strict=False para evitar que el programa se interrumpa si una fecha falla
            # y no forzamos la zona horaria aquí para que acepte el formato "T"
            pl.col("date").str.to_datetime(strict=False),
            # Aplanamos el struct de precipitación
            pl.col("precipitation").struct.field("total").alias("precip_mm"),
            pl.col("precipitation").struct.field("type").alias("precip_tipo"),
            # Aseguramos tipos numéricos
            pl.col("temperature").cast(pl.Float64, strict=False),
        ]
    )

    # 3. FILTRO Y EXPORTACIÓN
    df_hourly = df_hourly.drop_nulls("date")

    df_hourly = df_hourly.filter(pl.col("temperature").is_between(-60, 60))

    # Exportamos el CSV
    df_final_csv = df_hourly.drop("precipitation")
    export_to_csv(df_final_csv, "Tiempo_por_horas")

    return df_hourly


def get_current_weather_dataframe(df):
    """Datos del tiempo actual."""
    df = clean_nulls(df)
    df_current = (
        df.sort("id", descending=True)
        .filter(
            pl.col("current").is_not_null()
        )  # LIMPIEZA: Filtrar solo filas donde 'current' no es nulo.
        .limit(1)
        .drop_nulls("hourly")  # LIMPIEZA: Eliminar filas donde 'hourly' es nulo.
        .select(["timestamp_captura", "current"])
        .unnest("current")
        .with_columns(  # Aplanamos el último nivel (precipitación y viento) para que el CSV no de error
            [
                pl.col("precipitation")
                .struct.field("total")
                .fill_null(0)
                .alias("precip_total"),
                pl.col("precipitation")
                .struct.field("type")
                .fill_null("unknown")
                .alias("precip_type"),
                pl.col("wind").struct.field("speed").fill_null(0).alias("wind_speed"),
                pl.col("wind").struct.field("angle").fill_null(0).alias("wind_angle"),
                pl.col("wind")
                .struct.field("dir")
                .fill_null("unknown")
                .alias("wind_dir"),
            ]
        )
        .drop(["precipitation", "wind"])
    )
    # El sort de arriba es para obtener el registro más reciente, ya que ID es incremental.
    export_to_csv(df_current, "Tiempo_actual")
    return df_current


def get_stats_dataframe(df):
    """
    Estadísticas por día (máximo, mínimo, promedio de temperatura y total de precipitación diaria). Aquí se crean columnas nuevas,
    agrupando por día para obtener estadísticas diarias.
    """
    df = clean_nulls(df)
    df_stats = (
        df.select(["hourly"])
        .drop_nulls("hourly")  # LIMPIEZA: Eliminar filas donde 'hourly' es nulo.
        .unnest("hourly")
        .explode("data")
        .unnest("data")
        # 1. Convertimos el string 'date' a datetime y luego extraemos solo la fecha (Date)
        .with_columns(
            pl.col("date")
            .str.to_datetime(time_unit="ms", time_zone="UTC")
            .dt.date()
            .alias("date_no_time")
        )
        # LIMPIEZA: Antes de agrupar, eliminamos filas con temperaturas nulas.
        .filter(pl.col("temperature").is_not_null())
        .group_by("date_no_time")
        .agg(
            [
                pl.col("temperature")
                .max()
                .alias("temp_max"),  # Columna calculada para la temperatura máxima.
                pl.col("temperature")
                .min()
                .alias("temp_min"),  # Columna calculada para la temperatura mínima.
                pl.col("temperature")
                .mean()
                .round(2)
                .alias("temp_avg"),  # Columna calculada para la temperatura promedio.
                # LIMPIEZA: sum() maneja nulls como 0 si usamos fill_null antes
                pl.col("precipitation")
                .struct.field("total")
                .fill_null(0)
                .sum()
                .alias("precip_total_diaria"),
            ]
        )
        .sort("date_no_time")
    )

    export_to_csv(df_stats, "Estadísticas_diarias")

    return df_stats


if __name__ == "__main__":
    df = get_polars_df_from_last_fetch()

    if df is not None:
        df_hourly = get_hourly_weather_dataframe(df)
        df_current = get_current_weather_dataframe(df)
        df_stats = get_stats_dataframe(df)

        print("--- VISTA DEL PRONÓSTICO POR HORAS ---")
        print(df_hourly.head(10))

        print("--- VISTA DEL CLIMA ACTUAL ---")
        print(df_current)

        print("--- VISTA DE ESTADÍSTICAS POR ID ---")
        print(df_stats.head(10))
        print(df_stats.head(10))
