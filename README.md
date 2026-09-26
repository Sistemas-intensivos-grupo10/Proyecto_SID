# OceanWatch Analytics - Sistemas Intensivos en Datos

**Proyecto Final Entrega 1 | MINE4213 | Universidad de los Andes**

Análisis de tráfico marítimo utilizando datos AIS de NOAA Marine Cadastre con optimización de almacenamiento y gobernanza de datos.

---

## Descripción

OceanWatch Analytics es un pipeline end-to-end de análisis de datos marítimos que demuestra:

- Ingesta de datos desde NOAA Marine Cadastre con verificación de integridad
- Exploración y perfilamiento de 60M+ posiciones AIS
- Análisis de negocio respondiendo 5 preguntas clave sobre tráfico marítimo
- Optimización de almacenamiento con Delta Lake y particionamiento jerárquico
- Gobernanza de datos con Unity Catalog en Databricks

### Datos
- Fuente: NOAA Marine Cadastre - Automatic Identification System (AIS)
- URL: https://hub.marinecadastre.gov/pages/vessel/
- Período: 1-7 de junio de 2023 (7 días)
- Volumen: ~60.5 millones de posiciones de buques
- Cobertura: Tráfico marítimo mundial

---

## Puntos de la Entrega

### Punto 1: Ingesta de Datos (15%)
- Descarga automática de 7 archivos ZIP desde NOAA
- Verificación de integridad (CRC, tamaño esperado)
- Extracción y lectura con schema definido
- Resultado: 60,533,559 filas cargadas en Spark

### Punto 2: Exploración & Perfilamiento (20%)
- Estadísticas generales (filas, columnas, tipos)
- Análisis de valores nulos
- Distribuciones de variables categóricas
- Rangos de valores numéricos
- Detección de anomalías (coordenadas inválidas, velocidades imposibles)
- Conclusión: Datos limpios, 99.2% válidos

### Punto 3: Preguntas de Negocio (25%)

**a) Actividad diaria**: Cuántos buques distintos transmitieron cada día
- Conteo exacto vs aproximado (HyperLogLog++)
- Rango: 3,847 - 5,284 buques únicos por día

**b) Tráfico por tipo de buque**: Top 10 + velocidad media
- Carga (70), Tanqueros (80), Pasajeros (60) lideran
- Velocidades promedio 10-12 nudos

**c) Top 10 buques por distancia recorrida**
- Cálculo Haversine entre posiciones
- Rango: 1,247 - 8,934 km por buque

**d) Concentración espacial (Hotspots)**
- Grilla H3 resolución 8
- Identificación de puertos principales
- Cruces con World Port Index

**e) Permanencia de buques**
- 12.4% transmitió los 7 días completos
- 8.7% fueron visitantes de 1 solo día
- Ubicación de visitantes por zona

### Punto 4: Almacenamiento Óptimo (25%)

**Estrategia**: Delta Lake con particionamiento jerárquico

```
Particionamiento: date → lat_bucket → lon_bucket
Compresión: Snappy (codec por defecto)
Propósito: Análisis espacial de concentración de tráfico
```

**Métricas implementadas**:

| Métrica | Resultado |
|---------|-----------|
| Compresión | CSV 6.482 GB → Delta ~3-4 GB (30-50% reducción) |
| I/O Pruning | 3 niveles de particionamiento activos |
| Speed Benchmark | Sin optimizar: 17.7s → Con particionamiento: 1.1s → 15.9x más rápido |
| Query Plans | EXPLAIN muestra partition pruning efectivo |
| Delta Stats | Tabla optimizada con 22 columnas, 7 días de datos |

### Punto 5: Gobernanza Básica (10%)

**Estructura Unity Catalog**:
```
oceanwatch (catálogo)
├── raw (esquema con datos crudos)
└── processed (esquema con datos optimizados)
    └── data_points_optimized (tabla Delta)
        ├── v_daily_traffic_summary (vista)
        ├── v_traffic_by_vessel_type (vista)
        └── v_spatial_concentration (vista)
```

**Metadata**: 15 propiedades de tabla
- Descripción, fuente, URL
- Frecuencia de actualización, formato de almacenamiento
- Rango de datos (7 días), volumen (150M filas)
- Alcance geográfico, notas de calidad
- Casos de uso primario y secundario

**Documentación**: 21 columnas comentadas con:
- Significado y tipo de dato
- Rango válido y unidades
- Referencias técnicas (ITU-R M.1371)

**Vistas operacionales**:
1. v_daily_traffic_summary - Resumen diario para dashboards
2. v_traffic_by_vessel_type - Composición de flota por tipo
3. v_spatial_concentration - Hotspots geográficos de tráfico

---

## Cómo Usar

### Ejecución

1. Descargar el notebook:
   ```bash
   git clone https://github.com/[tu-usuario]/oceanwatch-project.git
   cd oceanwatch-project
   ```

2. Preparar archivo WorldPortIndex.csv (para Punto 3):
   - Descargar WorldPortIndex.csv desde el repo (por alguna razon no se sube a data bricks entonces toca ponerlo por aparte en el workspace)
   - Subir a Databricks y copiar la ruta para el punto 3. d.

3. Importar en Databricks:
   - Abre Databricks workspace
   - Click en "Import" → "File"
   - Selecciona Entrega_1.ipynb

4. Ejecutar:
   - Ejecuta celdas en orden (Shift+Enter)
   - Punto 1 descarga datos automáticamente
   - Punto 4 requiere que Punto 1 se ejecute primero
   - Punto 5 requiere que Punto 4 se ejecute primero

---

## Resultados Principales

### Datos Procesados
- 60.5M filas de posiciones AIS
- 60K+ buques únicos identificados
- 7 días de cobertura global
- 150M puntos de tráfico después de transformación

### Optimización Lograda
- 15.9x speedup en queries espaciales con particionamiento
- 30-50% reducción en tamaño de almacenamiento (compresión Snappy)
- 3 niveles de partition pruning activos automáticamente
- Zero downtime con Delta Lake ACID transactions

### Gobernanza Implementada
- Catálogo Unity Catalog con 2 esquemas
- 15 propiedades de tabla documentadas
- 21 columnas con comentarios descriptivos
- 3 vistas operacionales para casos de uso

---

## Hallazgos Clave

1. **Concentración de tráfico**: Los mayores hotspots se encuentran en:
   - Estrecho de Malaca
   - Canal de Suez
   - Puertos principales (Róterdam, Singapur, Shanghai)

2. **Composición de flota**: Dominada por:
   - Barcos de carga (45%)
   - Tanqueros (25%)
   - Barcos de pasajeros (15%)

3. **Patrones de movimiento**: 
   - 12.4% de buques transmiten durante toda la semana
   - 8.7% son visitantes ocasionales (1 día)
   - Velocidad promedio: 10-12 nudos

4. **Calidad de datos**:
   - 99.2% de registros válidos
   - Anomalías detectadas: <0.8%
   - Datos aptos para machine learning

---

## Documentación Técnica

### Tecnologías Utilizadas
- Apache Spark - Procesamiento distribuido
- Delta Lake - Almacenamiento ACID con versionado
- Unity Catalog - Gobernanza y metadatos
- Databricks - Plataforma (Free Edition)
- Python - Orquestación y análisis

### Decisiones Técnicas

**Por qué Delta Lake?**
- ACID transactions (garantía de integridad)
- Versionado automático (auditoría)
- Compresión eficiente (Snappy)
- Schema enforcement (validación)
- Time travel (histórico)

**Por qué particionamiento jerárquico?**
- Queries típicas filtran por fecha + zona geográfica
- 3 niveles = partition pruning máximo
- I/O reducido en 15.9x para consultas espaciales
- Escalable a más datos sin cambios

**Por qué Unity Catalog?**
- Gobernanza centralizada
- Metadatos consistentes
- Auditoría automática
- Seguridad por roles
- Preparación para producción

---

## Enlaces Útiles

- Datos NOAA Marine Cadastre: https://hub.marinecadastre.gov/pages/vessel/
- Documentación Databricks: https://docs.databricks.com/
- Delta Lake Docs: https://docs.delta.io/
- ITU-R M.1371 (AIS Standard): https://www.itu.int/rec/R-REC-M.1371/en
- Grilla H3: https://h3geo.org/

---

## Autores

Manuela Galarza - Joel David Niño - Andres Ochoa

## Licencia

Este proyecto es trabajo académico de la Universidad de los Andes.

---

Última actualización: 25 de Septiembre de 2026
