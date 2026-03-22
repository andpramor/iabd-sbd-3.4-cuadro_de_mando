from django.urls import path

from .views import datos_hourly, graficos, index

urlpatterns = [
    path("", index, name="index"),
    path("datos/<str:provincia>/", datos_hourly, name="datos"),
    path("graficos/", graficos, name="graficos"),
]
