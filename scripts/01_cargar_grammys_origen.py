import os
import sqlite3
import sys
from contextlib import closing

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dags"))
import etl_funciones as etl


def main():
    etl.crear_carpetas()
    if not os.path.exists(etl.GRAMMYS_CSV):
        sys.exit(f"No encuentro {etl.GRAMMYS_CSV}")

    grammys_csv = pd.read_csv(etl.GRAMMYS_CSV)
    with closing(sqlite3.connect(etl.DB_ORIGEN)) as conn:
        grammys_csv.to_sql(etl.TABLA_GRAMMYS, conn, if_exists="replace", index=False)
        conn.commit()
        filas = pd.read_sql(f"SELECT COUNT(*) AS filas FROM {etl.TABLA_GRAMMYS}", conn).filas[0]
    print(f"Base de origen creada: {etl.DB_ORIGEN}")
    print(f"Filas en la tabla: {int(filas)} | filas en el CSV: {len(grammys_csv)}")


if __name__ == "__main__":
    main()