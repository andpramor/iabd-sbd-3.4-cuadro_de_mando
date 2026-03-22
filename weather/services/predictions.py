import os

import joblib
import polars as pl
from django.conf import settings


def prediccion_clima(provincia):
    # 1. Cargar el "paquete" del modelo
    model_path = os.path.join(settings.BASE_DIR, "weather/ml_models/modelo_clima.pkl")
    loaded_model_data = joblib.load(model_path)
    model = loaded_model_data["model"]
    scaler = loaded_model_data["scaler"]
    le = loaded_model_data["label_encoder"]
    features = loaded_model_data["features"]

    # 2. Cargar el csv
    CSV_PATH = os.path.join(
        settings.BASE_DIR, f"data_silver_layer/data_hourly/{provincia}.csv"
    )
    df = pl.read_csv(CSV_PATH)

    df = df.with_columns([pl.col("date").str.to_datetime().alias("date_dt")])

    df = df.with_columns(
        [
            pl.col("date_dt").dt.hour().alias("hour"),
            pl.col("date_dt")
            .dt.weekday()
            .alias("day_of_week"),  # Lunes=1, Domingo=7 en Polars
            pl.col("weather").replace({"rain_shower": "rain"}).alias("weather_clean"),
        ]
    )

    # 3. Transformar los datos
    X_new = df.select(features).to_numpy()
    X_new_scaled = scaler.transform(X_new)

    # 4. Predicción
    predicciones_numericas = model.predict(X_new_scaled)

    # 5. Convertir números de vuelta a nombres de clima (ej: 0 -> "rain")
    predicciones_clima = le.inverse_transform(predicciones_numericas)

    # Añadimos las predicciones como una nueva columna
    df_predicciones = df.with_columns(
        pl.Series(name="prediccion_modelo", values=predicciones_clima)
    )
    print(df_predicciones.head())

    return df_predicciones
