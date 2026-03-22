import csv
import os
from datetime import datetime

from django.conf import settings
from django.core.paginator import Paginator
from django.shortcuts import render


def index(request):
    return render(request, "tiempo/index.html")


def graficos(request):
    return render(request, "tiempo/graficos.html")


def datos_hourly(request, provincia):
    datos_clima = []
    nombre_archivo = f"{provincia}.csv"
    ruta_archivo = os.path.join(
        settings.BASE_DIR, "data_silver_layer/data_hourly", nombre_archivo
    )

    traduccion = {
        "Sunny": "Soleado",
        "Partly sunny": "Parcialmente soleado",
        "Overcast": "Nublado",
        "Fog": "Niebla",
        "Rain": "Lluvia",
        "Snow": "Nieve",
        "Rain showers": "Chubascos",
        "Thunderstorm": "Tormenta",
        "Cloudy": "Nublado",
    }

    if os.path.exists(ruta_archivo):
        with open(ruta_archivo, mode="r", encoding="utf-8") as f:
            lector = csv.DictReader(f)
            for fila in lector:
                try:
                    fila["date"] = datetime.fromisoformat(fila["date"]).strftime(
                        "%d/%m/%Y - %H:%M"
                    )  # Antes no hacía el .strftime().
                    fila["summary"] = traduccion.get(fila["summary"], fila["summary"])
                except ValueError:
                    pass
                datos_clima.append(fila)

    # Mostramos 24 registros por página (1 día cada hora)
    paginator = Paginator(datos_clima, 24)

    # Obtenemos el número de página de la URL (ej: /datos/sevilla/?page=2)
    numero_pagina = request.GET.get("page")
    page_obj = paginator.get_page(numero_pagina)

    context = {"datos": page_obj, "provincia": provincia}

    return render(request, "tiempo/datos.html", context)
