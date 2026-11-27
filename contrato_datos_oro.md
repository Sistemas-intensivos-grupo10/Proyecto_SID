# Contrato de datos · Capa oro (`oceanwatch_e2.gold`)

Este documento dice qué entrega la capa oro, con qué garantías y qué pasa si algo cambia. Es lo que pueden asumir quienes consumen estas tablas (dashboard, ML, análisis).

## Reglas generales

| Tema | Acuerdo |
|---|---|
| Dueño | Equipo de ingeniería de datos de OceanWatch |
| Origen | `silver.ais_posiciones` (solo registros que pasaron las compuertas de calidad) |
| Frecuencia | Una carga por día, la ejecuta el job `oceanwatch_pipeline_diario` |
| Actualización | Se reescribe solo la fecha procesada (`replaceWhere`). Correr dos veces el mismo día no duplica filas |
| Zona horaria | Todas las horas están en UTC |
| Cobertura | Datos AIS de NOAA, centrados en aguas de EE. UU. Las posiciones fuera de esa zona no se descartan, solo se marcan en plata (`fuera_extension`) |

## Fuentes de datos

| Fuente | Uso |
|---|---|
| AIS Data, Marine Cadastre (NOAA): https://hub.marinecadastre.gov/pages/vesseltraffic | Posiciones de buques, un zip con un CSV por día |
| World Port Index (capa pública `WPI_Ports2017` de ArcGIS, mismo catálogo de puertos del dataset) | Ubicación de puertos para `presencia_puertos` |
| Estándar AIS (ITU-R M.1371) | Significado de los códigos de `VesselType` y de los valores "sin dato" |

## Garantías de calidad (vienen de plata)

Un registro solo llega a oro si cumple todo lo siguiente:

- MMSI bien formado (9 dígitos).
- Fecha y hora válidas.
- Latitud entre -90 y 90, longitud entre -180 y 180.
- No está en tierra (según la máscara de superficie H3).
- Velocidad (`sog`) no mayor a 50 nudos.
- Sin duplicados por buque y timestamp.

Lo que no cumple va a `silver.ais_cuarentena` con su causa. Antes de cargar oro, `06_quality_gate` revisa que la cuarentena del día no pase de 1 %. Si pasa, oro no se actualiza.

## Tablas

### 1. `gold.trafico_celda_hora`
Tráfico por celda espacial y hora.

- **Granularidad:** una fila por `fecha` + `hora` + `h3_r7`.
- **Llave única:** (`fecha`, `hora`, `h3_r7`).

| Columna | Tipo | Significado |
|---|---|---|
| `fecha` | date | Día de las posiciones |
| `hora` | timestamp | Hora, truncada a la hora en punto |
| `h3_r7` | string | Celda H3 de resolución 7 (unos 5 km²) |
| `posiciones` | long | Número de posiciones registradas |
| `buques` | long | Buques distintos en la celda esa hora |
| `sog_promedio` | double | Velocidad promedio en nudos |
| `lat`, `lon` | double | Centro de la celda |

### 2. `gold.trayectorias_diarias`
Resumen del recorrido de cada buque en el día. Solo incluye buques (MMSI entre 200000000 y 799999999).

- **Granularidad:** una fila por `mmsi` + `fecha`.
- **Llave única:** (`mmsi`, `fecha`).

| Columna | Tipo | Significado |
|---|---|---|
| `mmsi` | string | Identificador del buque |
| `fecha` | date | Día |
| `posiciones` | long | Posiciones del día |
| `ts_inicio`, `ts_fin` | timestamp | Primera y última posición |
| `lat_inicio`, `lon_inicio` | double | Punto de partida del día |
| `lat_fin`, `lon_fin` | double | Punto final del día |
| `distancia_km` | double | Distancia recorrida, sin contar saltos anómalos |
| `sog_promedio`, `sog_max` | double | Velocidad promedio y máxima (nudos) |
| `saltos_anomalos` | long | Veces que la velocidad entre dos posiciones superó 60 nudos |
| `v_impl_max_kn` | double | Mayor velocidad implícita entre dos posiciones |
| `vessel_name`, `vessel_type`, `length` | string, int, double | Datos del buque |
| `horas_activo` | double | Horas entre la primera y la última posición |

### 3. `gold.presencia_puertos`
Buques detenidos (velocidad menor o igual a 1 nudo) dentro de 5 km de un puerto del World Port Index.

- **Granularidad:** una fila por `fecha` + `mmsi` + `puerto`.
- **Llave única:** (`fecha`, `mmsi`, `puerto`).

| Columna | Tipo | Significado |
|---|---|---|
| `fecha` | date | Día |
| `mmsi` | string | Buque |
| `puerto`, `pais` | string | Puerto y país |
| `posiciones` | long | Posiciones detenido en la zona del puerto |
| `primera_posicion`, `ultima_posicion` | timestamp | Rango de presencia |
| `horas_en_puerto` | double | Horas estimadas en puerto |
| `vessel_name`, `vessel_type` | string, int | Datos del buque |
| `lat_puerto`, `lon_puerto` | double | Ubicación del puerto |

### 4. `gold.dim_tipo_buque`
Tabla de apoyo con la categoría del buque según su código AIS.

- **Granularidad:** una fila por rango de códigos.

| Columna | Tipo | Significado |
|---|---|---|
| `codigo_min`, `codigo_max` | int | Rango de `vessel_type` |
| `categoria` | string | Nombre de la categoría (Carga, Tanquero, Pesca...) |

Se une con `vessel_type BETWEEN codigo_min AND codigo_max`.

## Qué puede ser nulo

- `sog_promedio` y `sog_max`: nulos si el buque solo reportó velocidad "sin dato".
- `vessel_name`, `vessel_type`, `length`: nulos si el buque nunca los transmitió.
- Las columnas de llave y de conteo nunca son nulas.

## Quién consume

| Consumidor | Tablas |
|---|---|
| Dashboard OceanWatch | las cuatro tablas |
| Modelo de ML | `trayectorias_diarias`, `presencia_puertos` |

## Cambios al esquema

- **Se puede hacer sin avisar:** agregar una columna nueva al final.
- **Hay que avisar a quienes consumen:** renombrar, borrar o cambiar el tipo de una columna, o cambiar la granularidad. Se anuncia antes y se actualiza este documento.

## Permisos

| Rol | Acceso a oro |
|---|---|
| `ingenieros_datos` | Lectura y escritura |
| `cientificos_datos` | Solo lectura |
| `analistas` | Solo lectura |

Los permisos se aplican con el notebook `07_gobernanza`, por tabla y con GRANT.

**Alcance:** el equipo trabaja en conjunto y comparte el grupo `mine-group10`, que tiene acceso completo al esquema. Por eso los tres roles están definidos y aplicados, pero la restricción entre ellos aplica a quien consuma los datos desde fuera del equipo. Separar también a los integrantes del equipo requeriría un grupo adicional, que solo puede crear un administrador de cuenta.
