# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC # 07 · Gobernanza
# MAGIC Permisos por rol, contrato de datos de la capa oro y linaje. Se corre una sola vez, cuando ya existen las tablas de oro. Se puede repetir sin problema.
# MAGIC
# MAGIC | Rol | bronze | silver | gold | ops |
# MAGIC |---|---|---|---|---|
# MAGIC | ingenieros_datos | todo | todo | todo | todo |
# MAGIC | cientificos_datos | - | lectura | lectura | - |
# MAGIC | analistas | - | - | lectura | - |
# MAGIC
# MAGIC En el workspace del curso no se pueden crear grupos, así que cada rol se asigna a un grupo que ya existe o a correos. Los permisos que dependan del dueño del esquema quedan listados al final.

# COMMAND ----------

# Quién tiene cada rol. Editar aquí si cambia el equipo.
if schema:
    # Workspace del curso (sin grupos propios): grupo del equipo y correos
    PRINCIPALES = {
        "ingenieros_datos":  ["mine-group10", "jd.ninom1@uniandes.edu.co"],
        "cientificos_datos": ["am.ochoat@uniandes.edu.co"],
        "analistas":         ["m.galarza@uniandes.edu.co"],
    }
else:
    # Workspace propio: grupos creados en la consola de cuenta
    PRINCIPALES = {
        "ingenieros_datos":  ["ingenieros_datos"],
        "cientificos_datos": ["cientificos_datos"],
        "analistas":         ["analistas"],
    }

# Qué capas ve cada rol y con qué permiso
MATRIZ = {
    "ingenieros_datos":  {"permiso": "ALL PRIVILEGES", "capas": ["bronze", "silver", "gold", "ops"]},
    "cientificos_datos": {"permiso": "SELECT",         "capas": ["silver", "gold"]},
    "analistas":         {"permiso": "SELECT",         "capas": ["gold"]},
}

def show(df, n=100):
    print(df.limit(n).toPandas().to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Permisos por rol

# COMMAND ----------

pendientes = []   # sentencias que no se pudieron aplicar con este usuario

def conceder(permiso, objeto, principal):
    sentencia = f"GRANT {permiso} ON {objeto} TO `{principal}`"
    try:
        spark.sql(sentencia)
        print("OK        ", sentencia)
    except Exception as e:
        print("PENDIENTE ", sentencia, "|", str(e).split("\n")[0][:90])
        pendientes.append(sentencia)

def tablas_de(capa):
    """Tablas de una capa. En el workspace del curso cada capa es un prefijo (silver_, gold_...)."""
    todas = [r.tableName for r in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()]
    return [f"{catalog}.{schema}.{n}" for n in todas if n.startswith(capa + "_")]

for rol, cfg in MATRIZ.items():
    for principal in PRINCIPALES[rol]:
        # Para ver algo, cada principal necesita entrar al catálogo y al esquema
        conceder("USE CATALOG", f"CATALOG {catalog}", principal)
        if schema:
            conceder("USE SCHEMA", f"SCHEMA {catalog}.{schema}", principal)
        for capa in cfg["capas"]:
            if schema:
                # Esquema compartido: el permiso se da tabla por tabla
                for tabla in tablas_de(capa):
                    conceder(cfg["permiso"], f"TABLE {tabla}", principal)
            else:
                conceder(f"USE SCHEMA, {cfg['permiso']}" if cfg["permiso"] == "SELECT" else cfg["permiso"],
                         f"SCHEMA {catalog}.{capa}", principal)

# COMMAND ----------

# Evidencia: permisos de las tablas de oro
oro = tablas_de("gold") if schema else [f"{catalog}.gold.{t}" for t in
      ["trafico_celda_hora", "trayectorias_diarias", "presencia_puertos", "dim_tipo_buque"]]
for t in oro:
    print("\n==", t)
    show(spark.sql(f"SHOW GRANTS ON TABLE {t}").select("Principal", "ActionType"))

# COMMAND ----------

# Lo que no se pudo aplicar depende del dueño del esquema. Se le pasa esta lista para que la corra.
if pendientes:
    print(f"{len(pendientes)} permisos pendientes. El dueño del esquema debe correr:\n")
    for s in pendientes:
        print(s + ";")
else:
    print("Todos los permisos quedaron aplicados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Contrato de datos de la capa oro
# MAGIC Descripción de cada tabla y columna dentro de Unity Catalog. El detalle completo está en `docs/contrato_datos_oro.md`.

# COMMAND ----------

def documentar(nombre, descripcion, granularidad, llave, columnas):
    t = T("gold", nombre)
    spark.sql(f"COMMENT ON TABLE {t} IS '{descripcion}'")
    spark.sql(f"""ALTER TABLE {t} SET TBLPROPERTIES (
        'contrato.dueno' = 'Ingenieria de datos OceanWatch',
        'contrato.granularidad' = '{granularidad}',
        'contrato.llave' = '{llave}',
        'contrato.frecuencia' = 'diaria',
        'contrato.origen' = '{T("silver", "ais_posiciones")}')""")
    existentes = set(spark.table(t).columns)
    for col, texto in columnas.items():
        if col in existentes:
            spark.sql(f"ALTER TABLE {t} ALTER COLUMN {col} COMMENT '{texto}'")
        else:
            print("  columna no encontrada, se omite:", col)
    print("documentada:", t)

# COMMAND ----------

documentar(
    "trafico_celda_hora",
    "Trafico maritimo por celda H3 y hora. Alimenta el mapa de intensidad del tablero.",
    "una fila por fecha, hora y celda", "fecha, hora, h3_r7",
    {"fecha": "Dia de las posiciones",
     "hora": "Hora (UTC), truncada a la hora en punto",
     "h3_r7": "Celda H3 resolucion 7 (unos 5 km2)",
     "posiciones": "Numero de posiciones registradas",
     "buques": "Buques distintos en la celda esa hora",
     "sog_promedio": "Velocidad promedio en nudos",
     "lat": "Latitud del centro de la celda",
     "lon": "Longitud del centro de la celda"})

documentar(
    "trayectorias_diarias",
    "Resumen del recorrido de cada buque por dia, con saltos anomalos de velocidad.",
    "una fila por buque y fecha", "mmsi, fecha",
    {"mmsi": "Identificador del buque (9 digitos)",
     "fecha": "Dia",
     "posiciones": "Posiciones del dia",
     "ts_inicio": "Primera posicion del dia",
     "ts_fin": "Ultima posicion del dia",
     "lat_inicio": "Latitud de la primera posicion",
     "lon_inicio": "Longitud de la primera posicion",
     "lat_fin": "Latitud de la ultima posicion",
     "lon_fin": "Longitud de la ultima posicion",
     "distancia_km": "Distancia recorrida, sin contar saltos anomalos",
     "sog_promedio": "Velocidad promedio en nudos",
     "sog_max": "Velocidad maxima reportada en nudos",
     "saltos_anomalos": "Veces que la velocidad entre dos posiciones supero 60 nudos",
     "v_impl_max_kn": "Mayor velocidad implicita entre dos posiciones, en nudos",
     "vessel_name": "Nombre del buque",
     "vessel_type": "Codigo AIS del tipo de buque",
     "length": "Eslora en metros",
     "horas_activo": "Horas entre la primera y la ultima posicion"})

documentar(
    "presencia_puertos",
    "Buques detenidos (velocidad de 1 nudo o menos) a menos de 5 km de un puerto del World Port Index.",
    "una fila por fecha, buque y puerto", "fecha, mmsi, puerto",
    {"fecha": "Dia",
     "mmsi": "Identificador del buque",
     "puerto": "Nombre del puerto",
     "pais": "Pais del puerto",
     "posiciones": "Posiciones con el buque detenido en la zona del puerto",
     "primera_posicion": "Primera posicion detenido",
     "ultima_posicion": "Ultima posicion detenido",
     "horas_en_puerto": "Horas estimadas en puerto",
     "vessel_name": "Nombre del buque",
     "vessel_type": "Codigo AIS del tipo de buque",
     "lat_puerto": "Latitud del puerto",
     "lon_puerto": "Longitud del puerto"})

documentar(
    "dim_tipo_buque",
    "Categoria del buque segun su codigo AIS. Se une con vessel_type BETWEEN codigo_min AND codigo_max.",
    "una fila por rango de codigos", "codigo_min, codigo_max",
    {"codigo_min": "Codigo AIS inicial del rango",
     "codigo_max": "Codigo AIS final del rango",
     "categoria": "Nombre de la categoria"})

# COMMAND ----------

# Evidencia: descripción y propiedades del contrato en una tabla
show(spark.sql(f"DESCRIBE TABLE EXTENDED {T('gold', 'trayectorias_diarias')}")
          .filter("col_name IN ('mmsi', 'saltos_anomalos', 'Comment', 'Table Properties')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Linaje
# MAGIC Unity Catalog registra solo de qué tablas viene cada tabla. Se ve como grafo en Catalog Explorer (tabla, pestaña Lineage) y se puede consultar aquí.

# COMMAND ----------

filtro = f"{catalog}.{schema}.%" if schema else f"{catalog}.%"
try:
    show(spark.sql(f"""
        SELECT DISTINCT source_table_full_name AS origen, target_table_full_name AS destino
        FROM system.access.table_lineage
        WHERE target_table_full_name LIKE '{filtro}'
          AND source_table_full_name IS NOT NULL
        ORDER BY destino, origen"""))
except Exception as e:
    print("Sin acceso a system.access: el linaje se ve en Catalog Explorer (tabla, pestaña Lineage).")

# COMMAND ----------
