# Workshop 2: Pipeline ETL - Spotify & Grammys

**Estudiantes:** [Tu Nombre] y [Nombre de tu compañero]

Este proyecto implementa un pipeline de datos orquestado con Apache Airflow. El objetivo es extraer información de canciones de Spotify (CSV) y ganadores de premios Grammy (Base de Datos), validar su calidad, transformarlos, cruzarlos y cargarlos en una base de datos local SQLite para su posterior análisis.

## Arquitectura del Proyecto

El pipeline sigue un flujo de trabajo paralelo que luego se consolida:
1. **Rama CSV (Spotify):** Lectura del archivo, validación estricta de calidad de datos y limpieza.
2. **Rama DB (Grammys):** Extracción de datos de premiación y estandarización de nombres.
3. **Merge & Load:** Cruce de ambas fuentes y carga en `musica_destino.db` (SQLite).

## Estructura de Carpetas
* `dags/`: Contiene `dag_workshop2.py` con la orquestación en Airflow.
* `scripts/`: Scripts auxiliares de ETL y generación de reportes (`etl_funciones.py`, `02_reporte_dashboard.py`).
* `notebook/`: Contiene `notebook_final.ipynb` con el análisis exploratorio.
* `db/`: Bases de datos origen y destino.
* `data/`: Archivos CSV crudos y carpeta de reportes/output.
* `images/`: Capturas de evidencia de ejecución.

## Validación de calidad de datos (Pandera)

Se valida el dataset de Spotify con un esquema que define:
- **Tipos de dato** de las 20 columnas.
- **Valores faltantes:** `nullable=False`.
- **Rangos:** `popularity` 0–100; `danceability`, `energy`, `speechiness`, `acousticness`, `instrumentalness`, `liveness` y `valence` 0–1; `key` −1 a 11; `loudness` −60 a 5; `tempo` 0–300; `time_signature` 0–7; `duration_ms` > 0; `mode` en {0, 1}.
- **Unicidad:** `track_id`.

La validación se hace en dos puntos:
1. **Datos crudos (`validate_csv`):** Con `lazy=True` se revisan todas las reglas y se genera un reporte con PASA/FALLA por regla. Si falla una regla estructural (columna ausente o tipo incorrecto), la tarea queda en rojo y el pipeline se detiene. Las fallas de contenido (nulos, duplicados, rangos) terminan como `PASA CON ADVERTENCIAS`, porque `transform_csv` las corrige.
2. **Datos limpios (`transform_csv`):** El esquema "limpio" funciona como puerta de calidad: cualquier falla detiene el pipeline y los datos malos no llegan a la base.

## Transformación, Merge y Carga

- **Transformación:** En Spotify, se eliminan duplicados por `track_id`, se imputan nulos básicos y se filtran valores fuera de rango. En Grammys, se normalizan los nombres de los artistas (minúsculas, sin espacios extra) para facilitar el cruce.
- **Merge:** Se realiza un cruce (Left Join) usando el nombre del artista como llave (`artist_key`). Se agrega la bandera `has_grammy` (1 o 0) y el total de premios ganados.
- **Carga:** El dataframe consolidado se inserta en la tabla `spotify_grammys` dentro de la base de datos SQLite `musica_destino.db` usando `to_sql`, reemplazando la tabla en cada ejecución.

## Decisiones Técnicas
* **SQLite:** Elegido como motor de base de datos destino por su ligereza y portabilidad, ideal para un proyecto académico sin requerimientos de alta concurrencia.
* **Pandera sobre Pandas nativo:** Implementado para garantizar un control estricto de tipos y reglas de dominio (contratos de datos) antes de permitir que los datos fluyan.
* **Separación lógica en Airflow:** Mantener la extracción y transformación de DB y CSV en ramas paralelas optimiza el tiempo de ejecución.

## Limitaciones
* **Cruce por nombre exacto:** El cruce entre artistas se hace mediante coincidencia de texto normalizado. Diferencias ortográficas menores o colaboraciones (ej. "Artista A feat. Artista B" vs "Artista A") pueden quedar por fuera del cruce.
* **Escalabilidad:** SQLite no es apto para escrituras concurrentes masivas. En un entorno productivo real, el destino debería ser un Data Warehouse (ej. PostgreSQL, Snowflake).

## Cómo se Ejecuta

1. Clonar o descomprimir este proyecto.
2. Crear un entorno virtual e instalar las dependencias:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # En Windows: .venv\Scripts\activate
   pip install -r requirements-local.txt