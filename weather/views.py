import os
from datetime import datetime

import polars as pl
from django.conf import settings
from django.core.paginator import Paginator
from django.shortcuts import render

from .services.graficos import get_weather_dashboard
from .services.predictions import prediccion_clima

traduccion = {
    "Sunny": "Soleado",
    "sunny": "Soleado",
    "Partly sunny": "Parcialmente soleado",
    "partly sunny": "Parcialmente soleado",
    "partly_sunny": "Parcialmente soleado",
    "Overcast": "Nublado",
    "overcast": "Nublado",
    "Fog": "Niebla",
    "fog": "Niebla",
    "Rain": "Lluvia",
    "rain": "Lluvia",
    "Snow": "Nieve",
    "snow": "Nieve",
    "Rain showers": "Chubascos",
    "rain showers": "Chubascos",
    "rain_showers": "Chubascos",
    "Thunderstorm": "Tormenta",
    "thunderstorm": "Tormenta",
    "Cloudy": "Muy nublado",
    "cloudy": "Muy nublado",
    "none": "Ninguno",
}

iconos = {
    "Sunny": "sunny",
    "Partly sunny": "partly_sunny",
    "Overcast": "overcast",
    "Cloudy": "cloudy",
    "Fog": "fog",
    "Rain": "rain",
    "Rain showers": "showers",
    "Snow": "snow",
    "Thunderstorm": "thunder",
}


def index(request):
    return render(request, "tiempo/index.html")


def graficos(request, provincia):
    graficos_html = get_weather_dashboard(provincia)

    context = {
        "provincia": provincia,
        "graficos": graficos_html,
    }
    return render(request, "tiempo/graficos.html", context)


def get_datos_hourly(provincia):
    df_hourly = prediccion_clima(provincia)

    if df_hourly is not None and not df_hourly.is_empty():
        # Aplicamos la traducción y el formato de fecha directamente en el DataFrame
        df_hourly = df_hourly.with_columns(
            [
                (
                    pl.lit("icons/")
                    + pl.col("summary").replace(iconos, default="favicon")
                    + pl.lit(".svg")
                ).alias("icon"),
                # Formatear fecha ('date' ya es tipo string ISO)
                pl.col("date").map_elements(
                    lambda x: datetime.fromisoformat(str(x)).strftime(
                        "%d/%m/%Y - %H:%M"
                    ),
                    return_dtype=pl.String,
                ),
                # Traducir 'Estado' (Summary) y 'Predicción del modelo' (prediccion_modelo)
                pl.col("summary").replace(traduccion, default=pl.col("summary")),
                pl.col("prediccion_modelo").replace(
                    traduccion, default=pl.col("prediccion_modelo")
                ),
            ]
        )

        # Convertimos a lista de diccionarios para que el Paginator lo entienda
        datos_clima = df_hourly.to_dicts()
    else:
        datos_clima = []

    return datos_clima


def get_datos_current(provincia):
    CSV_PATH = os.path.join(
        settings.BASE_DIR, f"data_silver_layer/data_current/{provincia}.csv"
    )
    df = pl.read_csv(CSV_PATH)

    if df is not None and not df.is_empty():
        df = df.with_columns(
            [
                (
                    pl.lit("icons/")
                    + pl.col("summary").replace(iconos, default="favicon")
                    + pl.lit(".svg")
                ).alias("icon"),
                pl.col("summary")
                .replace(traduccion, default="Desconocido")
                .alias("estado"),
                pl.col("temperature").alias("temperatura"),
                pl.col("cloud_cover").alias("nubes"),
                pl.col("precip_total").alias("precipitacion_mm"),
                pl.col("precip_type")
                .replace(traduccion, default="Desconocido")
                .alias("tipo_precipitacion"),
                pl.col("wind_speed").alias("velocidad_viento"),
                pl.col("wind_dir").alias("direccion_viento"),
            ]
        )
        datos_current = df.to_dicts()
    else:
        datos_current = []

    return datos_current


def datos(request, provincia):
    # Datos por horas
    datos_hourly = get_datos_hourly(provincia)
    # Paginación de datos_hourly con 24 elementos por página (1 día completo)
    paginator = Paginator(datos_hourly, 24)
    numero_pagina = request.GET.get("page")
    page_obj = paginator.get_page(numero_pagina)

    # Datos actuales (current de la API)
    datos_current = get_datos_current(provincia)

    context = {
        "datos_hourly": page_obj,
        "datos_current": datos_current,
        "provincia": provincia,
    }

    return render(request, "tiempo/datos.html", context)
