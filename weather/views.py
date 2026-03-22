from datetime import datetime

import polars as pl
from django.core.paginator import Paginator
from django.shortcuts import render

from .services.predictions import prediccion_clima


def index(request):
    return render(request, "tiempo/index.html")


def graficos(request):
    return render(request, "tiempo/graficos.html")


def datos_hourly(request, provincia):
    df = prediccion_clima(provincia)

    traduccion = {
        "Sunny": "Soleado",
        "sunny": "Soleado",
        "Partly sunny": "Parcialmente soleado",
        "partly sunny": "Parcialmente soleado",
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
        "Thunderstorm": "Tormenta",
        "thunderstorm": "Tormenta",
        "Cloudy": "Muy nublado",
        "cloudy": "Muy nublado",
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

    if df is not None and not df.is_empty():
        # Aplicamos la traducción y el formato de fecha directamente en el DataFrame
        df = df.with_columns(
            [
                (
                    pl.lit("icons/")
                    + pl.col("summary").replace(iconos, default="favicon")
                    + pl.lit(".svg")
                ).alias("icon"),
                # Formatear fecha (asumiendo que 'date' ya es tipo datetime o string ISO)
                pl.col("date").map_elements(
                    lambda x: datetime.fromisoformat(str(x)).strftime(
                        "%d/%m/%Y - %H:%M"
                    ),
                    return_dtype=pl.String,
                ),
                # Traducir el resumen
                pl.col("summary").replace(traduccion, default=pl.col("summary")),
                pl.col("prediccion_modelo").replace(
                    traduccion, default=pl.col("prediccion_modelo")
                ),
            ]
        )

        # 3. Convertimos a lista de diccionarios para que el Paginator lo entienda
        datos_clima = df.to_dicts()
    else:
        datos_clima = []

    # 4. Paginación (se mantiene igual)
    paginator = Paginator(datos_clima, 24)
    numero_pagina = request.GET.get("page")
    page_obj = paginator.get_page(numero_pagina)

    context = {"datos": page_obj, "provincia": provincia}

    return render(request, "tiempo/datos.html", context)
