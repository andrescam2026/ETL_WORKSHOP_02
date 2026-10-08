import os
import re
import sqlite3
import unicodedata
from contextlib import closing
from datetime import datetime

import pandas as pd


try:
    import pandera.pandas as pa
except ImportError:
    import pandera as pa

_RAIZ_PROYECTO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.environ.get("ETL_BASE_DIR", _RAIZ_PROYECTO)
DRIVE_DIR = os.environ.get("ETL_DRIVE_DIR", "")

RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
STAGING_DIR = os.path.join(BASE_DIR, "data", "staging")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "output")
DB_DIR = os.path.join(BASE_DIR, "db")

SPOTIFY_CSV = os.path.join(RAW_DIR, "spotify_dataset.csv")
GRAMMYS_CSV = os.path.join(RAW_DIR, "the_grammy_awards.csv")

DB_ORIGEN = os.path.join(DB_DIR, "grammys_origen.db")
DB_DESTINO = os.path.join(DB_DIR, "musica_destino.db")

TABLA_GRAMMYS = "grammys"
TABLA_FINAL = "spotify_grammys"
TABLA_CALIDAD = "data_quality_report"
CSV_FINAL = "spotify_grammys_merged.csv"

STG = {
    "spotify_raw": os.path.join(STAGING_DIR, "01_spotify_raw.csv"),
    "spotify_clean": os.path.join(STAGING_DIR, "02_spotify_clean.csv"),
    "grammys_raw": os.path.join(STAGING_DIR, "01_grammys_raw.csv"),
    "grammys_artist": os.path.join(STAGING_DIR, "02_grammys_by_artist.csv"),
    "merged": os.path.join(STAGING_DIR, "03_merged.csv"),
}


def crear_carpetas():
    for carpeta in (RAW_DIR, STAGING_DIR, OUTPUT_DIR, DB_DIR):
        os.makedirs(carpeta, exist_ok=True)


def leer_sql(ruta_db, consulta):
    with closing(sqlite3.connect(ruta_db)) as conn:
        return pd.read_sql(consulta, conn)


def escribir_sql(df, ruta_db, tabla, modo="replace"):
    with closing(sqlite3.connect(ruta_db)) as conn:
        df.to_sql(tabla, conn, if_exists=modo, index=False)
        conn.commit()
        filas = conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
    return int(filas)


COLUMNAS_TEXTO = [
    "track_id", "artists", "album_name", "track_name", "track_genre",
    "main_artist", "artist_key", "popularity_level",
    "title", "category", "nominee", "artist", "workers", "img",
    "published_at", "updated_at",
]


def leer_csv(ruta):
    return pd.read_csv(ruta, dtype={c: str for c in COLUMNAS_TEXTO})


def normalizar_texto(texto):
    if texto is None or pd.isna(texto):
        return None
    texto = unicodedata.normalize("NFKD", str(texto))
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = texto.lower()
    texto = re.sub(r"[^\w\s&]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto or None


SEPARADORES_ARTISTAS = re.compile(
    r"\s*(?:,|&|\+|\bfeaturing\b|\bfeat\b\.?|\bft\b\.?|\bwith\b|\band\b)\s*"
)

NO_SON_ARTISTAS = {"various artists", "various"}


def claves_artista(texto):
    completo = normalizar_texto(texto)
    if completo is None:
        return []
    partes = SEPARADORES_ARTISTAS.split(str(texto).lower())
    candidatas = [completo] + [normalizar_texto(p) for p in partes]
    claves = []
    for c in candidatas:
        if c and len(c) >= 2 and c not in NO_SON_ARTISTAS and c not in claves:
            claves.append(c)
    return claves


def read_csv():
    crear_carpetas()
    df = leer_csv(SPOTIFY_CSV)
    df.to_csv(STG["spotify_raw"], index=False)
    print(f"Spotify leído: {df.shape[0]} filas x {df.shape[1]} columnas")
    return {"filas": int(df.shape[0]), "columnas": int(df.shape[1])}


def read_db():
    crear_carpetas()
    df = leer_sql(DB_ORIGEN, f"SELECT * FROM {TABLA_GRAMMYS}")
    df.to_csv(STG["grammys_raw"], index=False)
    print(f"Grammys leídos desde la BD: {df.shape[0]} filas x {df.shape[1]} columnas")
    return {"filas": int(df.shape[0]), "columnas": int(df.shape[1])}


TIPOS_SPOTIFY = {
    "track_id": str, "artists": str, "album_name": str, "track_name": str,
    "popularity": int, "duration_ms": int, "explicit": bool,
    "danceability": float, "energy": float, "key": int, "loudness": float,
    "mode": int, "speechiness": float, "acousticness": float,
    "instrumentalness": float, "liveness": float, "valence": float,
    "tempo": float, "time_signature": int, "track_genre": str,
}


RANGOS_SPOTIFY = {
    "popularity": (0, 100),
    "danceability": (0.0, 1.0), "energy": (0.0, 1.0),
    "speechiness": (0.0, 1.0), "acousticness": (0.0, 1.0),
    "instrumentalness": (0.0, 1.0), "liveness": (0.0, 1.0),
    "valence": (0.0, 1.0),
    "key": (-1, 11),
    "loudness": (-60.0, 5.0),
    "tempo": (0.0, 300.0),
    "time_signature": (0, 7),
}


def esquema_spotify(etapa="crudo"):
    columnas = {}
    for col, tipo in TIPOS_SPOTIFY.items():
        reglas = []
        if col in RANGOS_SPOTIFY:
            minimo, maximo = RANGOS_SPOTIFY[col]
            reglas.append(pa.Check.in_range(minimo, maximo))
        if col == "duration_ms":
            reglas.append(pa.Check.gt(0))
        if col == "mode":
            reglas.append(pa.Check.isin([0, 1]))
        columnas[col] = pa.Column(
            tipo,
            checks=reglas,
            nullable=False,
            unique=(col == "track_id"),
        )
    if etapa == "limpio":
        columnas["duration_min"] = pa.Column(float, pa.Check.gt(0), nullable=False)
        columnas["main_artist"] = pa.Column(str, nullable=False)
        columnas["artist_key"] = pa.Column(str, nullable=False)
        columnas["n_artists"] = pa.Column(int, pa.Check.ge(1), nullable=False)
        columnas["popularity_level"] = pa.Column(
            str, pa.Check.isin(["Baja", "Media", "Alta"]), nullable=False
        )

    return pa.DataFrameSchema(columnas, strict=False, name=f"spotify_{etapa}")


def _categoria_regla(check):
    check = str(check)
    if check == "column_in_dataframe":
        return "existe_columna"
    if check.startswith("dtype("):
        return "tipo_dato"
    if check == "not_nullable":
        return "sin_nulos"
    if check == "field_uniqueness":
        return "valores_unicos"
    return check


def validar_con_reporte(df, esquema, nombre_dataset):
    try:
        esquema.validate(df, lazy=True)
        fallas = pd.DataFrame(columns=["column", "check", "failure_case", "index"])
    except pa.errors.SchemaErrors as error:
        fallas = error.failure_cases.copy()
    fallas["regla"] = fallas["check"].map(_categoria_regla)


    faltantes = fallas["regla"] == "existe_columna"
    fallas.loc[faltantes, "column"] = fallas.loc[faltantes, "failure_case"].astype(str)


    reglas = []
    for nombre, col in esquema.columns.items():
        reglas += [(nombre, "existe_columna"), (nombre, "tipo_dato")]
        if not col.nullable:
            reglas.append((nombre, "sin_nulos"))
        if col.unique:
            reglas.append((nombre, "valores_unicos"))
        reglas += [(nombre, _categoria_regla(chk.error)) for chk in col.checks]


    conteo = (
        fallas.groupby(["column", "regla"])["index"].nunique().to_dict()
        if len(fallas) else {}
    )
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    filas = []
    for columna, regla in reglas:
        n = int(conteo.get((columna, regla), 0))

        if n == 0 and ((fallas["column"] == columna) & (fallas["regla"] == regla)).any():
            n = 1
        filas.append({
            "fecha_ejecucion": fecha,
            "dataset": nombre_dataset,
            "columna": columna,
            "regla": regla,
            "filas_con_falla": n,
            "pct_filas": round(100 * n / max(len(df), 1), 3),
            "resultado": "PASA" if n == 0 else "FALLA",
        })
    return pd.DataFrame(filas), fallas


def guardar_reporte(reporte):
    escribir_sql(reporte, DB_DESTINO, TABLA_CALIDAD, modo="append")


def imprimir_reporte(reporte):
    total = len(reporte)
    fallidas = reporte[reporte["resultado"] == "FALLA"]
    print(f"Reglas evaluadas: {total} | Pasan: {total - len(fallidas)} | Fallan: {len(fallidas)}")
    if len(fallidas):
        print(fallidas[["columna", "regla", "filas_con_falla", "pct_filas"]].to_string(index=False))


def validate_csv():
    df = leer_csv(STG["spotify_raw"])
    reporte, fallas = validar_con_reporte(df, esquema_spotify("crudo"), "spotify_crudo")
    guardar_reporte(reporte)
    fallas.to_csv(os.path.join(STAGING_DIR, "validacion_fallas_spotify_crudo.csv"), index=False)
    imprimir_reporte(reporte)

    estructurales = reporte[
        (reporte["resultado"] == "FALLA")
        & reporte["regla"].isin(["existe_columna", "tipo_dato"])
    ]
    if len(estructurales):
        raise ValueError(
            "VALIDACIÓN FALLIDA: hay errores estructurales en el CSV de Spotify:\n"
            + estructurales.to_string(index=False)
        )
    reglas_fallidas = int((reporte["resultado"] == "FALLA").sum())
    estado = "PASA" if reglas_fallidas == 0 else "PASA CON ADVERTENCIAS"
    print(f"Estado de la validación de datos crudos: {estado}")
    return {"estado": estado, "reglas": int(len(reporte)), "reglas_fallidas": reglas_fallidas}


def transform_csv():
    df = leer_csv(STG["spotify_raw"])
    bitacora = {"filas_leidas": len(df)}


    df = df.drop(columns=[c for c in df.columns if c.startswith("Unnamed")])


    antes = len(df)
    df = df.dropna(subset=list(TIPOS_SPOTIFY))
    bitacora["quitadas_por_nulos"] = antes - len(df)


    antes = len(df)
    df = df.drop_duplicates(subset="track_id", keep="first")
    bitacora["quitadas_por_duplicado"] = antes - len(df)


    antes = len(df)
    valido = (df["duration_ms"] > 0) & df["mode"].isin([0, 1])
    for col, (minimo, maximo) in RANGOS_SPOTIFY.items():
        valido &= df[col].between(minimo, maximo)
    df = df.loc[valido].copy()
    bitacora["quitadas_fuera_de_rango"] = antes - len(df)


    for col in ["artists", "album_name", "track_name", "track_genre"]:
        df[col] = df[col].str.strip()


    df["duration_min"] = (df["duration_ms"] / 60000).round(2)


    lista_artistas = df["artists"].str.split(";")
    df["main_artist"] = lista_artistas.str[0].str.strip()
    df["n_artists"] = lista_artistas.str.len().astype(int)
    antes = len(df)
    df = df[df["main_artist"].str.len() > 0].copy()
    bitacora["quitadas_sin_artista"] = antes - len(df)


    df["artist_key"] = df["main_artist"].map(normalizar_texto)
    df["artist_key"] = df["artist_key"].fillna(df["main_artist"].str.lower())

    df["popularity_level"] = pd.cut(
        df["popularity"], bins=[-1, 33, 66, 100], labels=["Baja", "Media", "Alta"]
    ).astype(str)

    bitacora["filas_finales"] = len(df)
    print("Bitácora de transformación de Spotify:")
    for paso, valor in bitacora.items():
        print(f"  {paso:<28} {valor:>8}")


    reporte, fallas = validar_con_reporte(df, esquema_spotify("limpio"), "spotify_limpio")
    guardar_reporte(reporte)
    imprimir_reporte(reporte)
    if (reporte["resultado"] == "FALLA").any():
        raise ValueError("PUERTA DE CALIDAD: los datos limpios de Spotify no cumplen el esquema.")
    print("Puerta de calidad: PASA (los datos limpios cumplen todas las reglas)")

    df.to_csv(STG["spotify_clean"], index=False)
    return {k: int(v) for k, v in bitacora.items()}


def transform_db():
    df = leer_csv(STG["grammys_raw"])
    bitacora = {"filas_leidas": len(df)}


    df = df.drop(columns=["published_at", "updated_at", "img", "workers"], errors="ignore")


    antes = len(df)
    df = df.dropna(subset=["artist"])
    bitacora["quitadas_sin_artista"] = antes - len(df)


    df["winner"] = df["winner"].map(lambda v: str(v).strip().lower() in ("1", "1.0", "true"))
    df["year"] = df["year"].astype(int)
    df["category"] = df["category"].astype(str).str.strip()


    df = df.reset_index(drop=True)
    df["grammy_row_id"] = df.index


    df["artist_key"] = df["artist"].map(claves_artista)
    df = df.explode("artist_key").dropna(subset=["artist_key"])
    df = df.drop_duplicates(subset=["grammy_row_id", "artist_key"])
    bitacora["filas_artista_premio"] = len(df)


    resumen = (
        df.groupby("artist_key")
        .agg(
            grammy_wins=("winner", "sum"),
            grammy_categories=("category", "nunique"),
            first_grammy_year=("year", "min"),
            last_grammy_year=("year", "max"),
        )
        .reset_index()
    )
    resumen["grammy_wins"] = resumen["grammy_wins"].astype(int)
    bitacora["artistas_unicos"] = len(resumen)


    if resumen["artist_key"].duplicated().any():
        raise ValueError("artist_key repetida en el resumen de Grammys")

    print("Bitácora de transformación de Grammys:")
    for paso, valor in bitacora.items():
        print(f"  {paso:<28} {valor:>8}")
    resumen.to_csv(STG["grammys_artist"], index=False)
    return {k: int(v) for k, v in bitacora.items()}


def merge():
    spotify = leer_csv(STG["spotify_clean"])
    grammys = leer_csv(STG["grammys_artist"])


    df = spotify.merge(grammys, on="artist_key", how="left", validate="many_to_one")


    df["grammy_wins"] = df["grammy_wins"].fillna(0).astype(int)
    df["grammy_categories"] = df["grammy_categories"].fillna(0).astype(int)
    df["first_grammy_year"] = df["first_grammy_year"].astype("Int64")
    df["last_grammy_year"] = df["last_grammy_year"].astype("Int64")
    df["has_grammy"] = df["grammy_wins"] > 0


    if len(df) != len(spotify):
        raise ValueError(f"El merge cambió el número de filas: {len(spotify)} -> {len(df)}")

    resumen = {
        "filas": int(len(df)),
        "canciones_con_grammy": int(df["has_grammy"].sum()),
        "artistas_spotify": int(df["artist_key"].nunique()),
        "artistas_con_grammy": int(df.loc[df["has_grammy"], "artist_key"].nunique()),
    }
    print("Resultado del merge:")
    for k, v in resumen.items():
        print(f"  {k:<24} {v:>8}")
    df.to_csv(STG["merged"], index=False)
    return resumen


def load():
    df = leer_csv(STG["merged"])
    for col in ["first_grammy_year", "last_grammy_year"]:
        df[col] = df[col].astype("Int64")
    filas = escribir_sql(df, DB_DESTINO, TABLA_FINAL, modo="replace")


    with closing(sqlite3.connect(DB_DESTINO)) as conn:
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_artist_key ON {TABLA_FINAL}(artist_key)")
        conn.commit()

    if filas != len(df):
        raise ValueError(f"Se esperaban {len(df)} filas en la BD y hay {filas}")
    print(f"Tabla '{TABLA_FINAL}' cargada en {DB_DESTINO}: {filas} filas")
    return {"filas_cargadas": filas}


def store():
    crear_carpetas()
    df = leer_sql(DB_DESTINO, f"SELECT * FROM {TABLA_FINAL}")
    ruta_local = os.path.join(OUTPUT_DIR, CSV_FINAL)
    df.to_csv(ruta_local, index=False)
    destinos = [ruta_local]

    if DRIVE_DIR and os.path.isdir(os.path.dirname(DRIVE_DIR)):
        os.makedirs(DRIVE_DIR, exist_ok=True)
        ruta_drive = os.path.join(DRIVE_DIR, CSV_FINAL)
        df.to_csv(ruta_drive, index=False)
        destinos.append(ruta_drive)
    else:
        print("El CSV se guardó solo en local (no hay carpeta de Drive configurada).")

    for ruta in destinos:
        print(f"CSV guardado: {ruta} ({len(df)} filas)")
    return {"filas": int(len(df)), "destinos": destinos}
