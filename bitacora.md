# BITACORA - OceanWatch Analytics Entrega 2

Documento de seguimiento de la Entrega 2 del Proyecto Final de MINE4213 - Sistemas Intensivos en Datos

---

## Resumen Ejecutivo

La Entrega 2 convirtió el análisis de la Entrega 1 en un pipeline diario completo: ingesta en streaming, capas bronce, plata y oro con calidad como compuerta, orquestación con un job programado, tablero, gobernanza y un modelo de detección de anomalías con MLflow. Se procesaron 12 días de datos AIS (1 al 12 de junio de 2023) en el catálogo `clase10`, esquema `equipo10`.

---

## Diseño y arquitectura

### Qué se hizo
- Se definió el flujo: NOAA → landing → bronce → plata → compuerta de calidad → oro → tablero, con ML al final.
- Se separó cada capa en su propio notebook para poder orquestarlas como tareas independientes.
- Se creó `00_config`, que detecta el workspace y arma los nombres de tablas y volúmenes, para que los notebooks corran igual en el workspace del curso y en uno propio.

### Decisiones técnicas
- **Un solo esquema con prefijos de capa** (`bronze_`, `silver_`, `gold_`, `ops_`, `ml_`): el catálogo del curso es compartido, así que el equipo trabaja dentro de su esquema.
- **Delta Lake y Unity Catalog** para tener transacciones, historial y control de acceso.
- **Celdas H3 de resolución 7** (unos 5 km²) como unidad espacial común para la máscara de tierra, el tráfico y los puertos.

---

## Puntos 1 y 2 · Ingesta y capas

### Qué se hizo
- **Landing:** descarga del zip del día y CSV completo en el volume que vigila Auto Loader. Si el archivo ya existe, no se repite.
- **Bronce:** Auto Loader con esquema tomado del encabezado, todo como texto, columna de datos rescatados, archivo de origen y hora de ingesta. Checkpoint en un volume.
- **Plata:** lectura en streaming de bronce con `foreachBatch`. Cada lote se valida, los registros buenos entran con un `MERGE` que solo inserta, y los malos van a cuarentena con su causa.
- **Máscara de tierra:** las celdas H3 se clasifican como tierra con Natural Earth (tierra sin lagos ni ríos, a 5 km o más del agua, con pocos buques y pocas posiciones).
- **Compuerta de calidad:** `06_quality_gate` corta el pipeline si la cuarentena supera 1 %, si no hay filas de entrada o si plata queda vacía, y deja un registro de cada evaluación.
- **Oro:** tráfico por celda y hora, trayectorias diarias con detección de saltos de posición, presencia en puertos y una tabla de tipos de buque.

### Por qué se hizo
- Los datos AIS son crudos de sensores y traen problemas reales: identificadores mal formados, coordenadas imposibles, posiciones en tierra y velocidades inverosímiles. Separar el crudo (bronce) de lo validado (plata) permite reprocesar sin perder nada.
- La cuarentena con causa documentada hace que cada rechazo sea explicable y medible.
- La compuerta evita que un día de mala calidad contamine el modelo de negocio.

### Tolerancia a fallos
`02b_demo_falla` inyecta una falla al procesar un archivo, deja que el stream aborte sin guardar el lote, y luego relanza con el mismo checkpoint. La conciliación compara las filas de bronce contra las líneas de cada CSV para comprobar que no se perdió ni se duplicó nada.

### Resultados
- `04_silver` concilia al final que bronce = plata + cuarentena + duplicados.
- Entre 7,8 y 9,1 millones de posiciones de entrada por día.
- La cuarentena estuvo entre 0,033 % y 0,073 % de las filas por día, muy por debajo del umbral de 1 %, y la compuerta dio OK en los 12 días.
- Principales causas de cuarentena (día de ejemplo, 2023-06-10): posición en tierra, MMSI inválido y velocidad implausible.
- Ese mismo día: 21.560 buques activos y 7,8 millones de posiciones.

---

## Punto 3 · Orquestación

### Qué se hizo
Job `oceanwatch_pipeline_diario`, con seis tareas encadenadas: landing, bronce, plata, compuerta de calidad, oro y ML.
- Parámetro `fecha_feed`: `auto` toma el día siguiente al último cargado. La fecha resuelta en landing se pasa a las demás tareas.
- Reintentos por tarea, alertas por duración y tiempos máximos.
- Correo de aviso si el job falla.
- Ejecución programada todos los días a las 6:00 a. m. (hora de Bogotá) y disparo adicional por llegada de archivos al volume de entrada.

### Por qué se hizo
Un pipeline diario necesita correr solo, reintentar lo que falla de forma transitoria y avisar cuando algo no se resuelve.

### Decisiones
- La compuerta de calidad no tiene reintentos: si falla es por los datos, y reintentar no los arregla.
- La tarea de ML va al final, para no afectar a las tareas anteriores.
- Las tablas, el esquema y el modelo de ML pertenecen al grupo del equipo, de modo que cualquier integrante, o el propio job, puede usarlos.

### Resultados
Una ejecución completa procesó el día 2023-06-12 de punta a punta, con las seis tareas en verde: 8,8 millones de filas de entrada, 0,0717 % de cuarentena, compuerta en OK y 19.299 buque-días puntuados por el modelo.

---

## Punto 4 · Consola de visualización

### Qué se hizo
Tablero **OceanWatch — Tráfico marítimo**, publicado, alimentado por la capa oro y las métricas de calidad:
- Indicadores del día: buques activos, buques en puerto, posiciones y % de cuarentena.
- Mapa de intensidad por celda H3.
- Ranking de puertos (top 15 por buques) con filtro por categoría de buque.
- Buques en puerto.
- Anomalías del día (calidad y saltos de posición).
- Filtro por fecha.

### Resultados
Cada consulta del tablero se ejecutó con datos reales (fecha 2023-06-10) y devuelve resultados. El ranking muestra 15 puertos, desglosados por categoría de buque.

---

## Punto 5 · Gobernanza

### Qué se hizo
- **Permisos por rol:** tres roles (ingenieros, científicos de datos y analistas), con el mínimo acceso necesario: los analistas solo leen oro, y bronce y la cuarentena quedan solo para ingeniería.
- **Contrato de datos:** descripción de cada tabla de oro y de cada columna dentro de Unity Catalog, más propiedades de dueño, granularidad, llave, frecuencia y origen.
- **Linaje:** lo registra Unity Catalog de forma automática. Se ve el recorrido completo desde bronce hasta las tablas de ML y el tablero.
- Todo se aplica con el notebook `07_gobernanza`. Detalle en [docs/gobernanza.md](docs/gobernanza.md) y [docs/contrato_datos_oro.md](docs/contrato_datos_oro.md).

### Alcance de los permisos
Cada rol se asigna a un grupo existente o a correos, y los permisos se dan tabla por tabla. Como el equipo comparte el grupo del curso, que tiene acceso a todo el esquema, la separación entre roles aplica a usuarios externos al equipo; entre los integrantes del equipo todos pueden trabajar sobre todas las capas, que es lo esperado en un proyecto conjunto.

---

## Punto 6 · ML con MLflow

### Qué se hizo
Detección de anomalías de velocidad por buque y día, con Isolation Forest. Las características salen de plata y oro, el experimento queda registrado en MLflow (parámetros, métricas y versión de los datos) y el modelo se registra en Unity Catalog con el alias `champion`. El resultado se guarda en `ml_anomalias_velocidad` y la tarea corre al final del job.

### Por qué se hizo
- Se eligió detección de anomalías y no pronóstico de arribos porque se apoya directamente en la velocidad, que es una señal ya validada en plata, y funciona con pocos días de datos.
- El enunciado pide demostrar ML dentro de un pipeline reproducible, no maximizar la precisión: por eso la semilla es fija, los parámetros y la versión de los datos quedan registrados, y la tabla de salida se reescribe por fecha sin duplicar.

### Decisión técnica
El modelo se registra en `proyecto.equipo10_ml`, un esquema dedicado a ML y propiedad del grupo del equipo, para que cualquier integrante o el job puedan registrar nuevas versiones.

### Resultados
254.244 buque-días de 13 días, puntuados con la versión 5 del modelo, de los cuales se marca el 1 % como atípico (unos 2.540). El modelo coincide con el 16,7 % de los saltos que detecta la regla de oro, porque cada uno mira algo distinto: la regla detecta errores de posición y el modelo detecta comportamiento de velocidad. Los casos más extremos son lanchas rápidas de recreo y de fuerza pública, atípicas pero plausibles. Sin etiquetas no se puede decir cuáles son errores y cuáles son reales.

---

## Punto 7 · Repositorio y documentación

### Qué se hizo
- Notebooks, definición del job y tablero en `entrega2/`.
- `README.md` y esta bitácora para la Entrega 2.
- Documentación de gobernanza y contrato de datos con sus capturas en `docs/`.

---

## Lecciones Aprendidas

### Ingeniería de datos
1. Separar el dato crudo del validado permite reprocesar sin perder nada.
2. Un archivo de configuración por workspace evita duplicar notebooks.
3. La cuarentena con causa vuelve medible la calidad de la fuente.

### Orquestación y ML
1. Registrar la versión de los datos junto con el modelo hace reproducible el experimento.
2. Declarar el entorno de cada notebook (versión y librerías) lo hace reproducible en cualquier cómputo.
3. Comparar el modelo contra una regla conocida ayuda a interpretarlo, aunque no coincidan.
4. Los recursos que usa un job deben pertenecer al grupo del equipo y no a una persona.

### Gobernanza
1. Los permisos se suman: dar acceso a un grupo amplio anula las restricciones individuales.
2. Lo que no se pueda aplicar con el usuario actual debe quedar listado para quien sí puede, no fallar en silencio.
3. Documentar el alcance real de los permisos es mejor que prometer una separación que no se cumple.

---

Última actualización: 27 de Noviembre de 2026
