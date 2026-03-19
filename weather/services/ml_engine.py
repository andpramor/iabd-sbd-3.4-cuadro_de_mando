import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.cluster import KMeans
from sklearn.metrics import confusion_matrix
from sklearn.metrics import classification_report, accuracy_score

# 1. Carga y Preparación con Polars
df = pl.read_csv("Tiempo_por_horas_3_3.csv")

# Procesamiento de fechas y limpieza usando expresiones (.with_columns)
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

# 2. Clasificación: queremos predecir el clima (weather_clean) a partir de las otras variables utilizando RandomForest.
features = ["temperature", "humidity", "precip_prob", "hour"]

# Para sklearn, convertimos el bloque seleccionado a un array de NumPy
X = df.select(features).to_numpy()
le = LabelEncoder()
# Convertimos la columna a una lista para el LabelEncoder
y = le.fit_transform(df.select("weather_clean").to_series().to_list())

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42, stratify=y
)

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Entrenamiento del modelo
model = RandomForestClassifier(n_estimators=150, max_depth=10, random_state=42)
model.fit(X_train_scaled, y_train)
y_pred = model.predict(X_test_scaled)

print("--- MÉTRICAS DE CLASIFICACIÓN (Random Forest) ---")
print(f"Precisión Global (Accuracy): {accuracy_score(y_test, y_pred):.4f}")
print("\nInforme detallado (Precision, Recall, F1):")
print(classification_report(y_test, y_pred, target_names=le.classes_, zero_division=0))

# 3. Clustering
X_cluster_raw = df.select(["temperature", "humidity", "precip_prob"]).to_numpy()
X_cluster_scaled = scaler.fit_transform(X_cluster_raw)
kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
clusters = kmeans.fit_predict(X_cluster_scaled)

# Añadimos los resultados de vuelta al DataFrame de Polars
df = df.with_columns([pl.Series(name="cluster_clima", values=clusters)])

# 4. Exportación de resultados
# Matriz de Confusión
plt.figure(figsize=(8, 6))
cm = confusion_matrix(y_test, y_pred)
sns.heatmap(
    cm,
    annot=True,
    fmt="d",
    cmap="Blues",
    xticklabels=le.classes_,
    yticklabels=le.classes_,
)
plt.title("Matriz de Confusión: Predicción del Clima")
plt.savefig("confusion_matrix.png")

# Visualización de Clústeres
clusters_names = {
    "0": "Frío y muy húmedo",
    "1": "Cálido y seco",
    "2": "Alta probabilidad de lluvia",
}
df = df.with_columns(
    cluster_descripcion=(df["cluster_clima"].cast(pl.String).replace(clusters_names))
)

plt.figure(figsize=(10, 6))
sns.scatterplot(
    data=df,
    x="temperature",
    y="humidity",
    hue="cluster_descripcion",
    palette="viridis",
)
plt.legend(title="Clústeres")
plt.title("Segmentación del Clima (Clustering K-Means)")
plt.savefig("weather_clusters.png")
