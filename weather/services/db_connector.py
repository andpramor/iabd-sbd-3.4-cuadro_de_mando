import polars as pl
import sqlite3
from scripts_1_7_weather_apis.db_connection import DB_PATH


def get_weather_schema():
    precip_schema = pl.Struct(
        [pl.Field("total", pl.Float64), pl.Field("type", pl.String)]
    )

    return pl.Struct(
        [
            pl.Field("lat", pl.String),
            pl.Field("lon", pl.String),
            pl.Field("timestamp_captura", pl.String),
            pl.Field(
                "current",
                pl.Struct(
                    [
                        pl.Field("temperature", pl.Float64),
                        pl.Field("summary", pl.String),
                        pl.Field("icon", pl.String),
                        pl.Field("cloud_cover", pl.Int64),
                        pl.Field(
                            "wind",
                            pl.Struct(
                                [
                                    pl.Field("speed", pl.Float64),
                                    pl.Field("angle", pl.Int64),
                                    pl.Field("dir", pl.String),
                                ]
                            ),
                        ),
                        pl.Field("precipitation", precip_schema),
                    ]
                ),
            ),
            pl.Field(
                "hourly",
                pl.Struct(
                    [
                        pl.Field(
                            "data",
                            pl.List(
                                pl.Struct(
                                    [
                                        pl.Field("date", pl.String),
                                        pl.Field("weather", pl.String),
                                        pl.Field("temperature", pl.Float64),
                                        pl.Field("humidity", pl.Int64),  # Nombre exacto
                                        pl.Field(
                                            "apparent_temp", pl.Float64
                                        ),  # Nombre exacto (antes estaba temperature_2m)
                                        pl.Field("precipitation", precip_schema),
                                        pl.Field(
                                            "precip_prob", pl.Int64
                                        ),  # Nombre exacto
                                        pl.Field("summary", pl.String),
                                    ]
                                )
                            ),
                        )
                    ]
                ),
            ),
        ]
    )


def get_polars_df_from_last_fetch(table_name):
    """
    Obtener un DataFrame de Polars a partir de una tabla en SQLite, decodificando el JSON con un esquema manual.
    """
    query = f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT 1"
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pl.read_database(query=query, connection=conn)
        conn.close()

        if not df.is_empty():
            schema = get_weather_schema()
            # 1. Decodificar el JSON con el esquema manual
            df = df.with_columns(
                pl.col("payload").str.json_decode(dtype=schema)
            ).unnest("payload")
            return df
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None


if __name__ == "__main__":
    df = get_polars_df_from_last_fetch("openmeteo")

    if df is not None:
        print(df)
