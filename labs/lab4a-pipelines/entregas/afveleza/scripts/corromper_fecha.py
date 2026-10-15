"""Parte 4 del Lab 4a: corrompe A PROPOSITO una fecha del CSV dejando
vacia la columna `total` en un porcentaje de sus filas.

Uso (desde labs/lab4a-pipelines/datos):
    python corromper_fecha.py              # 2026-01-08, 10% de filas
    python corromper_fecha.py 2026-01-08 0.10

Para deshacerlo: python gen_dataset.py  (regenera el CSV limpio).
"""
import csv
import sys

ARCHIVO = "ventas_colombia_lab4a.csv"
fecha = sys.argv[1] if len(sys.argv) > 1 else "2026-01-08"
fraccion = float(sys.argv[2]) if len(sys.argv) > 2 else 0.10

with open(ARCHIVO, newline="", encoding="utf-8") as f:
    lector = csv.DictReader(f)
    columnas = lector.fieldnames
    filas = list(lector)

del_dia = [fila for fila in filas if fila["fecha"] == fecha]
a_vaciar = max(1, round(len(del_dia) * fraccion))
for fila in del_dia[:a_vaciar]:
    fila["total"] = ""

with open(ARCHIVO, "w", newline="", encoding="utf-8") as f:
    escritor = csv.DictWriter(f, fieldnames=columnas)
    escritor.writeheader()
    escritor.writerows(filas)

print(f"{fecha}: vaciada la columna 'total' en {a_vaciar} de {len(del_dia)} filas "
      f"({a_vaciar / len(del_dia):.1%}).")
