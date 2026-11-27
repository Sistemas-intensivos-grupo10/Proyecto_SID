# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC # 08 · ML · Detección de anomalías de velocidad
# MAGIC Por cada buque y día calcula características de su velocidad, entrena un Isolation Forest, registra el experimento en MLflow y deja el modelo en Unity Catalog. Al final marca los buques con comportamiento raro del día.
# MAGIC
# MAGIC Es una tarea del job diario, después de `gold`. No busca la mejor precisión: demuestra ML reproducible dentro del pipeline (semilla fija, versión de datos registrada, modelo versionado).

# COMMAND ----------

dbutils.widgets.text("fecha", "todas")      # "todas" = todo el histórico; el job pasa YYYY-MM-DD
fecha = dbutils.widgets.get("fecha")
print("fecha:", fecha)

SEMILLA = 42            # misma semilla = mismo resultado
CONTAMINACION = 0.01    # se espera que ~1 % de los buque-días sea raro
N_ARBOLES = 200
MIN_POSICIONES = 5      # con menos posiciones las características no son confiables

tbl_silver = T("silver", "ais_posiciones")
tbl_tray   = T("gold", "trayectorias_diarias")
tbl_feat   = T("ml", "features_anomalia_velocidad")
tbl_anom   = T("ml", "anomalias_velocidad")

# El modelo se registra en un esquema donde este equipo puede crear modelos.
# En el workspace del curso el esquema del equipo no lo permite, se usa uno propio en "proyecto".
esquema_modelo = f"proyecto.{schema}_ml" if schema else f"{catalog}.ml"
modelo_uc = f"{esquema_modelo}.modelo_anomalias_velocidad"

pred = "true" if fecha == "todas" else f"fecha = DATE'{fecha}'"
print("modelo:", modelo_uc)

# COMMAND ----------

# Esquema del modelo (y de las tablas de ML en el workspace propio)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {esquema_modelo}")
if not schema:
    try:
        spark.sql(f"GRANT USE SCHEMA, SELECT, EXECUTE ON SCHEMA {esquema_modelo} TO `cientificos_datos`")
    except Exception as e:
        print("permiso omitido (el grupo no existe en este workspace)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingeniería de características
# MAGIC Una fila por buque y día. Todas salen de la velocidad (`sog`) y de la trayectoria.

# COMMAND ----------

from pyspark.sql import functions as F, Window

s = (spark.table(tbl_silver)
          .filter(pred)
          .filter("es_buque AND sog IS NOT NULL"))

# Cambio de velocidad entre posiciones consecutivas del mismo buque
w = Window.partitionBy("mmsi", "fecha").orderBy("ts")
s = s.withColumn("delta_sog", F.abs(F.col("sog") - F.lag("sog").over(w)))

feat_sog = (s.groupBy("mmsi", "fecha").agg(
    F.count("*").alias("n_posiciones"),
    F.avg("sog").alias("sog_prom"),
    F.stddev("sog").alias("sog_std"),
    F.max("sog").alias("sog_max"),
    F.percentile_approx("sog", 0.95).alias("sog_p95"),
    F.max("delta_sog").alias("delta_sog_max"),
    F.avg((F.col("sog") <= 1).cast("double")).alias("frac_detenido")))

# De oro: velocidad implícita y distancia
tray = spark.table(tbl_tray).filter(pred).select(
    "mmsi", "fecha", "v_impl_max_kn", "distancia_km", "horas_activo", "saltos_anomalos")

feat = (feat_sog.join(tray, ["mmsi", "fecha"])
        .filter(F.col("n_posiciones") >= MIN_POSICIONES)
        .withColumn("v_media_kn", F.when(F.col("horas_activo") > 0,
                    F.col("distancia_km") / 1.852 / F.col("horas_activo")).otherwise(F.lit(0.0)))
        .na.fill(0.0, ["sog_std", "delta_sog_max", "v_impl_max_kn"]))

COLS_MODELO = ["n_posiciones", "sog_prom", "sog_std", "sog_max", "sog_p95",
               "delta_sog_max", "frac_detenido", "v_impl_max_kn", "v_media_kn"]

# Se guarda la tabla de características. Idempotente: solo reescribe las fechas procesadas
if not spark.catalog.tableExists(tbl_feat):
    feat.write.saveAsTable(tbl_feat)
else:
    feat.write.mode("overwrite").option("replaceWhere", pred).saveAsTable(tbl_feat)
print(tbl_feat, "→", spark.table(tbl_feat).filter(pred).count(), "filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Entrenamiento y registro en MLflow

# COMMAND ----------

import mlflow, numpy as np, pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

mlflow.set_registry_uri("databricks-uc")
usuario = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{usuario}/oceanwatch_anomalias_velocidad")

# El modelo se entrena con todo el histórico de características disponible
datos = spark.table(tbl_feat).toPandas()
X = datos[COLS_MODELO].astype("float64")
version_datos = spark.sql(f"DESCRIBE HISTORY {tbl_feat} LIMIT 1").first()["version"]
print(f"entrenamiento: {len(X):,} buque-días | versión Delta de las características: {version_datos}")

# COMMAND ----------

with mlflow.start_run(run_name=f"isolation_forest_{fecha}") as run:
    mlflow.log_params({
        "modelo": "IsolationForest", "n_arboles": N_ARBOLES, "contaminacion": CONTAMINACION,
        "semilla": SEMILLA, "min_posiciones": MIN_POSICIONES, "fecha_proceso": fecha,
        "columnas": ",".join(COLS_MODELO), "tabla_features": tbl_feat,
        "version_delta_features": version_datos})

    modelo = Pipeline([
        ("escala", StandardScaler()),
        ("bosque", IsolationForest(n_estimators=N_ARBOLES, contamination=CONTAMINACION,
                                   random_state=SEMILLA, n_jobs=-1))])
    modelo.fit(X)

    marca = modelo.predict(X) == -1                         # True = anómalo
    puntaje = -modelo.decision_function(X)                  # más alto = más raro

    # Chequeo de coherencia: ¿los buques con saltos detectados por regla salen marcados?
    regla = (datos["saltos_anomalos"] > 0).to_numpy()
    solape = float((marca & regla).sum() / max(regla.sum(), 1))
    mlflow.log_metrics({
        "n_entrenamiento": float(len(X)),
        "pct_anomalias": float(100 * marca.mean()),
        "puntaje_medio": float(puntaje.mean()),
        "cobertura_de_saltos_por_regla": solape})
    print(f"anomalías: {marca.sum():,} ({100 * marca.mean():.2f}%) | cobertura de saltos por regla: {100 * solape:.1f}%")

    firma = infer_signature(X, modelo.predict(X))
    info = mlflow.sklearn.log_model(modelo, name="modelo", signature=firma,
                                    input_example=X.head(5), registered_model_name=modelo_uc)
    run_id = run.info.run_id

# COMMAND ----------

# La versión recién registrada queda como "champion" (la que usa el pipeline)
cliente = MlflowClient()
version = max(int(v.version) for v in cliente.search_model_versions(f"name = '{modelo_uc}'"))
cliente.set_registered_model_alias(modelo_uc, "champion", version)
print(f"modelo {modelo_uc} · versión {version} · alias champion · run {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Anomalías del día
# MAGIC Se carga el modelo registrado (no el objeto en memoria) para comprobar que el registro sirve de punta a punta.

# COMMAND ----------

cargado = mlflow.sklearn.load_model(f"models:/{modelo_uc}@champion")
hoy = spark.table(tbl_feat).filter(pred).toPandas()
Xh = hoy[COLS_MODELO].astype("float64")
hoy["es_anomalia"] = cargado.predict(Xh) == -1
hoy["puntaje"] = -cargado.decision_function(Xh)
hoy["modelo_version"] = version

nombres = spark.table(tbl_tray).filter(pred).select("mmsi", "fecha", "vessel_name")
res = (spark.createDataFrame(hoy[["mmsi", "fecha", "es_anomalia", "puntaje", "modelo_version"] + COLS_MODELO])
       .join(nombres, ["mmsi", "fecha"], "left")
       .withColumn("ts_proceso", F.current_timestamp()))

if not spark.catalog.tableExists(tbl_anom):
    res.write.saveAsTable(tbl_anom)
else:
    res.write.mode("overwrite").option("replaceWhere", pred).saveAsTable(tbl_anom)

spark.sql(f"COMMENT ON TABLE {tbl_anom} IS 'Buques con comportamiento de velocidad atipico segun el modelo Isolation Forest. Una fila por buque y dia.'")
print(tbl_anom, "→", spark.table(tbl_anom).filter(pred).count(), "filas")

# COMMAND ----------

top = (spark.table(tbl_anom).filter(pred).filter("es_anomalia")
       .orderBy(F.desc("puntaje"))
       .select("mmsi", "vessel_name", "fecha", F.round("puntaje", 3).alias("puntaje"),
               "sog_max", "v_impl_max_kn", "delta_sog_max", "n_posiciones"))
print(top.limit(10).toPandas().to_string(index=False))

# COMMAND ----------
