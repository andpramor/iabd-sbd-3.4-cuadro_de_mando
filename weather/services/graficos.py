import os
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from django.conf import settings

def get_weather_dashboard(provincia):
    # Rutas a tus archivos
    ruta_h = os.path.join(settings.BASE_DIR, f"data_silver_layer/data_hourly/{provincia}.csv")
    ruta_s = os.path.join(settings.BASE_DIR, f"data_silver_layer/data_stats/{provincia}.csv")

    if not os.path.exists(ruta_h):
        return "<p class='text-danger'>Error: No se encontró el archivo de datos horarios.</p>"

    # 1. Carga de datos con Polars
    df_h = pl.read_csv(ruta_h)
    df_h = df_h.with_columns([
        pl.col("date").str.to_datetime().dt.hour().alias("hora"),
        pl.col("date").str.to_datetime().dt.strftime("%d-%b").alias("dia"),
    ])

    # 2. Datos diarios
    if os.path.exists(ruta_s):
        df_d = pl.read_csv(ruta_s)
        eje_x_diario = df_d["date_no_time"] if "date_no_time" in df_d.columns else df_d["date"]
    else:
        df_d = df_h.group_by("dia").agg([
            pl.col("temperature").mean().alias("temp_avg"),
            pl.col("precip_mm").sum().alias("precip_total_diaria")
        ]).sort("dia")
        eje_x_diario = df_d["dia"]

    # 3. Configuración del tablero (5 filas)
    fig = make_subplots(
        rows=5, cols=1,
        subplot_titles=(
            "<b>1. Intensidad Térmica Horaria</b>", 
            "<b>2. Humedad vs Sensación Térmica</b>", 
            "<b>3. Relación Lluvia vs Temperatura</b>",
            "<b>4. Variabilidad de Temperatura</b>",
            "<b>5. Distribución de Estados del Cielo</b>"
        ),
        vertical_spacing=0.05, 
        specs=[
            [{"secondary_y": False}], 
            [{"secondary_y": False}], 
            [{"secondary_y": True}],
            [{"secondary_y": False}],
            [{"type": "polar"}] 
        ]
    )

    # Gráfico 1: Heatmap
    fig_heat = px.density_heatmap(df_h, x="dia", y="hora", z="temperature", histfunc="avg", color_continuous_scale="RdYlBu_r")
    for trace in fig_heat.data:
        fig.add_trace(trace, row=1, col=1)

    # Gráfico 2: Scatter
    fig_scatter = px.scatter(df_h, x="humidity", y="apparent_temp", color="temperature", size="precip_mm")
    for trace in fig_scatter.data:
        fig.add_trace(trace, row=2, col=1)

    # Gráfico 3: Lluvia vs Temp
    fig.add_trace(go.Bar(x=eje_x_diario, y=df_d["precip_total_diaria"], name="Lluvia (mm)", marker_color="rgba(52, 152, 219, 0.6)"), row=3, col=1, secondary_y=False)
    fig.add_trace(go.Scatter(x=eje_x_diario, y=df_d["temp_avg"], name="Temp. Media", line=dict(color="#e74c3c", width=4)), row=3, col=1, secondary_y=True)

    # Gráfico 4: Box Plot
    fig_box = px.box(df_h, x="dia", y="temperature", color="dia")
    for trace in fig_box.data:
        trace.showlegend = False 
        fig.add_trace(trace, row=4, col=1)

    # Gráfico 5: Radar
    df_counts = df_h.group_by("summary").count()
    fig.add_trace(
        go.Scatterpolar(
            r=df_counts["count"],
            theta=df_counts["summary"],
            fill="toself",
            name="Frecuencia Clima",
            fillcolor="rgba(255, 127, 80, 0.5)", 
            line=dict(color="coral", width=2),
        ),
        row=5,
        col=1,
    )

    fig.update_layout(
        height=2200, 
        title_text=f"<b>ANÁLISIS METEOROLÓGICO: {provincia.upper()}</b>",
        template="plotly_white",
        margin=dict(t=100, b=100, l=80, r=80),
        showlegend=False
    )

    # Accedemos a la quinta anotación y le damos el margen
    fig.layout.annotations[4].update(yshift=10)

    fig.update_polars(
        domain=dict(y=[0.0, 0.14]), 
        bgcolor="rgba(245, 245, 245, 0.8)",
        radialaxis=dict(showticklabels=True, gridcolor="white"),
        angularaxis=dict(gridcolor="white")
    )

    return fig.to_html(full_html=False, include_plotlyjs='cdn')