# Gobernanza de datos

Resumen de cómo se manejan los permisos, la documentación de la capa oro y el linaje en OceanWatch. Todo se aplica con el notebook `07_gobernanza`.

## 1. Qué se hizo

- **Permisos por rol:** se definieron tres roles y se asignaron con GRANT en Unity Catalog.
- **Contrato de datos:** las cuatro tablas de oro tienen descripción, comentario en cada columna y propiedades (`contrato.dueno`, `contrato.granularidad`, `contrato.llave`, `contrato.frecuencia`, `contrato.origen`). El detalle está en [contrato_datos_oro.md](contrato_datos_oro.md).
- **Linaje:** lo registra Unity Catalog de forma automática. Se ve como grafo en Catalog Explorer (tabla, pestaña Lineage) y se puede consultar en `system.access.table_lineage`.

## 2. Los tres roles

| Rol | bronze | silver | gold | ops |
|---|---|---|---|---|
| ingenieros_datos | todo | todo | todo | todo |
| cientificos_datos | sin acceso | lectura | lectura | sin acceso |
| analistas | sin acceso | sin acceso | lectura | sin acceso |

La idea es dar a cada rol solo la capa que necesita (mínimo privilegio): los analistas ven únicamente oro, que es lo que alimenta el tablero, y nadie fuera de ingeniería ve el dato crudo ni la cuarentena.

## 3. Quién tiene cada rol

| Rol | Asignado a | Permiso aplicado |
|---|---|---|
| ingenieros_datos | grupo `mine-group10` y `jd.ninom1@uniandes.edu.co` | todos los privilegios sobre las tablas |
| cientificos_datos | `am.ochoat@uniandes.edu.co` | `SELECT` en las tablas de silver y gold |
| analistas | `m.galarza@uniandes.edu.co` | `SELECT` en las tablas de gold |

Los destinatarios están en una lista al inicio de `07_gobernanza` (`PRINCIPALES`), y qué ve cada rol está en `MATRIZ`. Si cambia el equipo, se edita ahí y se vuelve a correr.

## 4. Cómo se hizo

1. **Sin grupos propios.** En el workspace del curso no se pueden crear grupos (lo hace un administrador de cuenta), así que cada rol se asignó a un grupo que ya existe (`mine-group10`) o a correos.
2. **Permisos por tabla.** El esquema `equipo10` es compartido y tiene un dueño distinto, así que los permisos no se dan sobre el esquema completo sino tabla por tabla. Las tablas de cada capa se identifican por su prefijo (`bronze_`, `silver_`, `gold_`, `ops_`).
3. **Sentencias usadas.** Por ejemplo:
   ```sql
   GRANT SELECT ON TABLE clase10.equipo10.gold_trayectorias_diarias TO `m.galarza@uniandes.edu.co`;
   GRANT ALL PRIVILEGES ON TABLE clase10.equipo10.silver_ais_posiciones TO `mine-group10`;
   ```
4. **Lo que no se puede aplicar no rompe el proceso.** Si un permiso necesita al dueño del esquema, el notebook lo deja impreso en una lista de pendientes en vez de fallar.
5. **Contrato.** Con `COMMENT ON TABLE`, `ALTER COLUMN ... COMMENT` y `SET TBLPROPERTIES`.

## 5. Cómo se verifica

```sql
SHOW GRANTS ON TABLE clase10.equipo10.gold_trayectorias_diarias;
```

Resultado comprobado en las tablas de oro:

| Principal | Permiso |
|---|---|
| mine-group10 | ALL PRIVILEGES |
| jd.ninom1 | ALL PRIVILEGES |
| am.ochoat | SELECT |
| m.galarza | SELECT |

En silver, `am.ochoat` tiene `SELECT` y `m.galarza` no tiene permiso adicional. En bronze y ops, solo ingeniería.

## 6. Alcance y limitación

Este es un trabajo en equipo y los integrantes comparten el grupo `mine-group10`, que tiene `SELECT`, `MODIFY` y `CREATE_TABLE` sobre todo el esquema. En Unity Catalog los permisos se suman, así que quien está en ese grupo accede a todas las capas aunque su rol individual sea de lectura.

Esto significa que:

- **Los permisos por rol sí se aplicaron**, como se muestra en la sección 5.
- **Entre los integrantes del equipo no hay separación efectiva**, porque el acceso del grupo es completo. Es lo esperado: en un proyecto conjunto todos necesitan trabajar sobre todas las capas.
- **Para quien consuma los datos desde fuera del equipo, los roles sí limitan el acceso.**
- Separar también a los integrantes entre sí requeriría un grupo adicional, que solo puede crear un administrador de cuenta.

Además, para que alguien fuera del grupo llegue a las tablas necesita entrar al catálogo y al esquema (`USE CATALOG` y `USE SCHEMA`). Esos permisos los da el dueño de `clase10` y de `equipo10`.

## 7. Evidencia

Las capturas son de la tabla `clase10.equipo10.gold_trayectorias_diarias` en Catalog Explorer. Se guardan en `docs/img/` con estos nombres.

**Permisos por rol.** Pestaña Permissions de la tabla: se ven `mine-group10`, `jd.ninom1`, `am.ochoat` y `m.galarza` con sus permisos.

![Permisos de la tabla](img/permisos_gold.png)

**Contrato de datos.** Pestaña Overview de la tabla: descripción de la tabla y comentarios de cada columna.

![Contrato en la tabla](img/contrato_overview.png)

**Linaje.** Pestaña Lineage, grafo expandido: muestra de qué tablas viene la tabla de oro, desde bronce.

![Linaje de la tabla](img/linaje_gold.png)

La misma información en lista, con los notebooks, el job y el dashboard que escriben o leen la tabla:

![Linaje en lista](img/linaje_gold_lista.png)
