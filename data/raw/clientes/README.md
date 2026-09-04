# Datos fuente sintéticos — clientes

Mismos CSVs de la Sesión 09: reemplazan el landing zone que lee `bronze/clientes.py`
vía Auto Loader. Reproducibles con `data/raw/clientes/gen_clientes.py` si quieres
regenerarlos o ajustar volumen/errores.

## Lotes

| Archivo | Filas | Contenido |
|---|---|---|
| `lote_01_clientes_2024-01-15.csv` | 40 | Clientes nuevos `id_cliente` 1-39 + 1 fila con `id_cliente` vacío |
| `lote_02_clientes_2024-02-12.csv` | 29 | Clientes nuevos 40-59 + 1 fila con fecha no parseable + 8 actualizaciones a IDs del lote 1 |
| `lote_03_clientes_2024-03-10.csv` | 23 | Clientes nuevos 60-74 + 1 fila con fecha futura + 1 fila con fecha < 1900 + 6 actualizaciones (algunas sobre IDs ya actualizados en el lote 2) |

Cada lote representa un "drop" de archivos en el volumen. Súbelos **en orden** y deja
que el pipeline procese cada uno (Auto Loader es incremental) para ver:

- **Ingesta incremental** en `clientes_raw` (Bronze) lote a lote.
- **AUTO CDC tipo 1** en `clientes` (Silver): las actualizaciones de los lotes 2 y 3
  deben pisar el estado anterior del mismo `id_cliente` (mismo nombre, ciudad/email/fecha
  nuevos).
- Las 3 `expectations` (warning, no descartan filas) en `view_clientes`:
  - `id_cliente` nulo
  - email inválido/ausente
  - fecha inválida o fuera de rango (`< 1900-01-01` o `> hoy`)
- La capa **Gold** (`clientes_resumen_mensual`): conteo de clientes por ciudad y mes.

## Cómo subirlos al volumen

Los tres lotes se suben al mismo Volume ya usado en la Sesión 09
(`/Volumes/dbassociate/default/vol_landing/sesion_09/`), sin importar a qué target
(dev/test) se despliegue el pipeline: la ruta de landing es igual en ambos ambientes,
lo único que cambia entre targets es el catalog de destino.

```bash
databricks fs cp data/raw/clientes/lote_01_clientes_2024-01-15.csv \
  dbfs:/Volumes/dbassociate/default/vol_landing/sesion_09/lote_01_clientes_2024-01-15.csv \
  --profile <tu-perfil>
```

Repite para cada lote, uno a la vez, corriendo el pipeline entre cada subida.

**Nota:** a diferencia de la Sesión 09, el catalog de destino ya no está hardcodeado
en `transformations/`: `resources/project_sdp_etl.pipeline.yml` lo pasa como valor de
`configuration`, y cada archivo de `transformations/` lo lee con `spark.conf.get()`.
Correr `databricks bundle deploy -t dev` publica en `dbassociate.bronze/silver/gold`;
`databricks bundle deploy -t test` publica en `dbassociate_test.bronze/silver/gold` en
el otro workspace, sin tocar una línea de código.

## Gotcha esperado: la fila con `id_cliente` vacío

`lote_01` incluye a propósito una fila con `id_cliente` vacío para poder ver el warning
`warning_id_cliente_null`. Pero el schema de Silver declara `id_cliente BIGINT NOT NULL`
y la expectation es solo `expect_all` (warn, no descarta la fila), así que esa fila
**probablemente hará fallar el `AUTO CDC` merge** al intentar escribir un `id_cliente`
nulo en una columna `NOT NULL`, en vez de solo generar un warning.

Es un gap real del pipeline (expectation de solo warning sobre la clave de merge de una
tabla NOT NULL), útil como punto de discusión en el laboratorio, y se repite igual en
cada target al que promuevas el pipeline. Dos salidas:

- Cambiar esa constraint puntual a `@dp.expect_or_drop("warning_id_cliente_null", ...)`
  para descartar filas con `id_cliente` nulo antes del CDC.
- O quitar esa fila del CSV si prefieres una corrida sin fallas para la demo inicial.
