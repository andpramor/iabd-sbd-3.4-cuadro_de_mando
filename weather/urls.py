from django.urls import path

from .views import *

urlpatterns = [
    path("", index, name="index"),
    path("datos/", datos, name="datos"),
    path("graficos/", graficos, name="graficos"),
]
