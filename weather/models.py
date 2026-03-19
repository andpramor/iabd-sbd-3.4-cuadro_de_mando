from django.db import models


class WeatherData(models.Model):
    timestamp_captura = models.DateTimeField(auto_now_add=True)
    payload = models.JSONField()  # Almacena el JSON de OpenMeteo directamente

    class Meta:
        db_table = "openmeteo"
