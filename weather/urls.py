from django.urls import path

from .views import datos, graficos, index, informes

urlpatterns = [
    path("", index, name="index"),
    path("datos/<str:provincia>/", datos, name="datos"),
    path("graficos/<str:provincia>/", graficos, name="graficos"),
    path("informes/", informes, name="informes"),
]
