from django.db import models


class WeatherData(models.Model):
    timestamp_captura = models.DateTimeField(auto_now_add=True)
    payload = models.JSONField()  # Almacena el JSON de OpenMeteo directamente
    # TODO: cambiar esto por un modelo de verdad, que luego queremos paginar y cosas así, en vez de almacenar el JSON entero, almacenar solo los campos que nos interesan.

    class Meta:
        db_table = "openmeteo"
