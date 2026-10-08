import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dags"))
import etl_funciones as etl

consulta = """
SELECT fecha_ejecucion, dataset,
       COUNT(*) AS reglas,
       SUM(resultado = 'FALLA') AS reglas_fallidas
FROM data_quality_report
GROUP BY fecha_ejecucion, dataset
ORDER BY fecha_ejecucion DESC, dataset
"""

print(etl.leer_sql(etl.DB_DESTINO, consulta).to_string(index=False))