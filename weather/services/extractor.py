from datetime import datetime

import requests

from weather.models import WeatherData

LAT = "37.3886"
LON = "-5.9823"
URL = "https://api.open-meteo.com/v1/forecast"
COLLECTION_NAME = "openmeteo"


def get_wind_dir(degrees):
    """Convierte grados a dirección cardinal (N, NNE, etc.)"""
    dirs = [
        "N",
        "NNE",
        "NE",
        "ENE",
        "E",
        "ESE",
        "SE",
        "SSE",
        "S",
        "SSW",
        "SW",
        "WSW",
        "W",
        "WNW",
        "NW",
        "NNW",
    ]
    ix = int((degrees + 11.25) / 22.5)
    return dirs[ix % 16]


def get_weather_translation(wmo_code):

    # Traduce el código WMO de Open-Meteo al estilo de texto de Meteosource.
    # Mapeo simplificado para traducir la respuesta.
    if wmo_code == 0:
        return "sunny", "Sunny"
    if wmo_code in [1, 2]:
        return "partly_sunny", "Partly sunny"
    if wmo_code == 3:
        return "overcast", "Overcast"
    if 45 <= wmo_code <= 48:
        return "fog", "Fog"
    if 51 <= wmo_code <= 67:
        return "rain", "Rain"
    if 71 <= wmo_code <= 77:
        return "snow", "Snow"
    if 80 <= wmo_code <= 82:
        return "rain_shower", "Rain showers"
    if 95 <= wmo_code <= 99:
        return "thunderstorm", "Thunderstorm"
    return "cloudy", "Cloudy"


def get_precip_type(rain_val, snow_val, showers_val):
    """Determina el tipo de precipitación basado en los valores"""
    total = rain_val + snow_val + showers_val
    if total == 0:
        return "none"
    if snow_val > 0:
        return "snow"
    return "rain"


def get_open_meteo():
    """
    Captura datos de Open-Meteo, procesa campos de current y hourly,
    y guarda la estructura JSON final en la base de datos.
    """
    timestamp_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Definimos los parámetros exactos que queremos de la API
    params = {
        "latitude": LAT,
        "longitude": LON,
        "current": "temperature_2m,weather_code,wind_speed_10m,wind_direction_10m,precipitation,cloud_cover",
        "hourly": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,precipitation_probability,weather_code",
        "timezone": "auto",
        "forecast_days": 7,
    }

    try:
        response = requests.get(URL, params=params)
        response.raise_for_status()
        data = response.json()

        # --- 1. PROCESAR CURRENT ---
        curr_raw = data["current"]
        w_slug, w_summary = get_weather_translation(curr_raw["weather_code"])

        # Lógica de tipo de precipitación para 'current'
        curr_precip_type = "none"
        if curr_raw["precipitation"] > 0:
            # Si el código WMO es de nieve (71-77), marcamos snow, si no rain
            curr_precip_type = (
                "snow" if 71 <= curr_raw["weather_code"] <= 77 else "rain"
            )

        current_data = {
            "temperature": float(curr_raw["temperature_2m"]),
            "summary": w_summary,
            "icon": w_slug,
            "wind": {
                "speed": float(curr_raw["wind_speed_10m"]),
                "angle": int(curr_raw["wind_direction_10m"]),
                "dir": get_wind_dir(curr_raw["wind_direction_10m"]),
            },
            "precipitation": {
                "total": float(curr_raw["precipitation"]),
                "type": curr_precip_type,
            },
            "cloud_cover": int(curr_raw["cloud_cover"]),
        }

        # --- 2. PROCESAR HOURLY (Capturando todos los campos críticos) ---
        hourly_raw = data["hourly"]
        hourly_list = []

        # Recorremos la lista de 'time' para reconstruir los objetos por hora
        for i in range(len(hourly_raw["time"])):
            w_code = hourly_raw["weather_code"][i]
            p_total = float(hourly_raw["precipitation"][i])
            h_slug, h_summary = get_weather_translation(w_code)

            # Lógica de tipo de precipitación para la franja horaria
            p_type = "none"
            if p_total > 0:
                p_type = "snow" if 71 <= w_code <= 77 else "rain"

            # Construimos el objeto de la hora con TODOS los datos solicitados
            hourly_list.append(
                {
                    "date": str(hourly_raw["time"][i]),
                    "weather": h_slug,
                    "temperature": float(hourly_raw["temperature_2m"][i]),
                    "humidity": int(hourly_raw["relative_humidity_2m"][i]),
                    "apparent_temp": float(hourly_raw["apparent_temperature"][i]),
                    "precipitation": {"total": p_total, "type": p_type},
                    "precip_prob": int(hourly_raw["precipitation_probability"][i]),
                    "summary": h_summary,
                }
            )

        # --- 3. CONSTRUCCIÓN DEL JSON FINAL PARA SQLITE ---
        # Usamos 'lat' y 'lon' para coincidir con tu estructura de DB
        datos_finales = {
            "lat": str(LAT),
            "lon": str(LON),
            "timestamp_captura": timestamp_actual,
            "current": current_data,
            "hourly": {
                "data": hourly_list  # Aquí metemos la lista de objetos procesados
            },
        }
        # Guardar en la base de datos
        WeatherData.objects.create(payload=datos_finales)
        print(f"✅ [OK] Datos de meteorología guardados: {timestamp_actual}")

    except Exception as e:
        print(f"❌ [ERROR] Fallo en get_open_meteo: {e}")


if __name__ == "__main__":
    get_open_meteo()
