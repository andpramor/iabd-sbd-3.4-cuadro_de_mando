from django.shortcuts import render


def index(request):
    return render(request, "tiempo/index.html")


def datos(request):
    return render(request, "tiempo/datos.html")


def graficos(request):
    return render(request, "tiempo/graficos.html")
