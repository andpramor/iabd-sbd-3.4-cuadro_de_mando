from django.urls import path

from .views import *

urlpatterns = [
    path("", index, name="index"),
    path("datos/<str:provincia>/", datos, name="datos"),
    path("graficos/", graficos, name="graficos"),
]
