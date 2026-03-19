import os

import polars as pl
import plotly.express as px
from pathlib import Path
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scripts_1_7_weather_apis.db_connection import BASE_DIR

BASE_PATH = Path(BASE_DIR)

DIRS = {
    "DAILY_STATS": BASE_PATH
    / "data_output"
    / "silver_layer"
    / "Estadísticas_diarias.csv",
    "HOURLY_WEATHER": BASE_PATH
    / "data_output"
    / "silver_layer"
    / "Tiempo_por_horas.csv",
    "CURRENT_WEATHER": BASE_PATH / "data_output" / "silver_layer" / "Tiempo_actual.csv",
}

OUTPUT_PLOTS_DIR = f"{BASE_PATH}/docs"
os.makedirs(OUTPUT_PLOTS_DIR, exist_ok=True)


def plot_combined_dashboard():
    # Carga de datos
    df_h = pl.read_csv(DIRS["HOURLY_WEATHER"])
    df_d = pl.read_csv(DIRS["DAILY_STATS"])

    # Procesamiento de fechas
    df_h = df_h.with_columns(
        pl.col("date").str.to_datetime().dt.hour().alias("hora"),
        pl.col("date").str.to_datetime().dt.strftime("%d-%b").alias("dia"),
    )

    # --- CONFIGURACIÓN DE SUBPLOTS ---
    # Definimos 3 filas y 1 columna.
    # La fila 3 necesita 'secondary_y' para el gráfico dual.
    fig = make_subplots(
        rows=3,
        cols=1,
        subplot_titles=(
            "<b>Intensidad Térmica Horaria</b>",
            "<b>Impacto de Humedad en Sensación Térmica (Tamaño: intensidad de lluvia)</b>",
            "<b>Relación Lluvia vs. Temperatura Media</b>",
        ),
        vertical_spacing=0.1,
        specs=[
            [{"secondary_y": False}],
            [{"secondary_y": False}],
            [{"secondary_y": True}],
        ],
    )

    # 1. Heatmap
    fig_heat = px.density_heatmap(
        df_h,
        x="dia",
        y="hora",
        z="temperature",
        histfunc="avg",
        color_continuous_scale="RdYlBu_r",
    )
    for trace in fig_heat.data:
        fig.add_trace(trace, row=1, col=1)

    # 2. Scatter
    fig_scatter = px.scatter(
        df_h,
        x="humidity",
        y="apparent_temp",
        color="temperature",
        size="precip_mm",
        template="plotly_white",
    )
    for trace in fig_scatter.data:
        fig.add_trace(trace, row=2, col=1)

    # 3. Combinado (Gráfico Dual directamente en fig)
    # Barras: Lluvia
    fig.add_trace(
        go.Bar(
            x=df_d["date_no_time"],
            y=df_d["precip_total_diaria"],
            name="Lluvia (mm)",
            marker_color="rgba(52, 152, 219, 0.6)",
        ),
        row=3,
        col=1,
        secondary_y=False,
    )

    # Línea: Temperatura
    fig.add_trace(
        go.Scatter(
            x=df_d["date_no_time"],
            y=df_d["temp_avg"],
            name="Temp. Media (°C)",
            line=dict(color="#e74c3c", width=4),
        ),
        row=3,
        col=1,
        secondary_y=True,
    )

    # --- DISEÑO FINAL ---
    fig.update_layout(
        height=1200,  # Ajustamos altura total para que no se vean apretados
        title_text="<b>Dashboard Meteorológico Consolidado</b>",
        showlegend=True,
        template="plotly_white",
    )

    # Ajustes de ejes específicos
    fig.update_yaxes(title_text="Hora (24h)", row=1, col=1)
    fig.update_yaxes(title_text="Sensación Térmica (°C)", row=2, col=1)
    fig.update_yaxes(title_text="Precipitación (mm)", row=3, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Temp (°C)", row=3, col=1, secondary_y=True)

    fig.update_xaxes(title_text="Día del Mes", row=1, col=1)
    fig.update_xaxes(title_text="Humedad Relativa (%)", row=2, col=1)
    fig.update_xaxes(title_text="Calendario Semanal", row=3, col=1)

    # Exportar un solo archivo
    fig.write_html(OUTPUT_PLOTS_DIR + "/index.html")
    print(f"Reporte generado: {OUTPUT_PLOTS_DIR}/index.html")


if __name__ == "__main__":
    plot_combined_dashboard()
