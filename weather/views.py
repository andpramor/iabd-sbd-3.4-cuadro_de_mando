import csv
import os
from datetime import datetime

from django.conf import settings
from django.shortcuts import render


def index(request):
    return render(request, "tiempo/index.html")


def graficos(request):
    return render(request, "tiempo/graficos.html")


def datos(request):
    datos_clima = []
    ruta_archivo = os.path.join(
        settings.BASE_DIR, "data_silver_layer/Tiempo_por_horas.csv"
    )

    traduccion = {
        "Sunny": "Soleado",
        "Partly Sunny": "Parcialmente soleado",
        "Overcast": "Nublado",
        "Cloudy": "Nublado",
        "Rain": "Lluvia",
    }

    with open(ruta_archivo, mode="r", encoding="utf-8") as f:
        lector = csv.DictReader(f)
        for fila in lector:
            try:
                fila["date"] = datetime.fromisoformat(fila["date"])
                fila["summary"] = traduccion.get(fila["summary"], fila["summary"])
            except ValueError:
                pass
            datos_clima.append(fila)

    return render(request, "tiempo/datos.html", {"datos": datos_clima})
