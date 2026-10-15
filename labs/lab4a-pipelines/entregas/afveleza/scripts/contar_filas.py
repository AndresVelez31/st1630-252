"""Conteos de Bronze / Silver / Gold para evidenciar idempotencia (Lab 4a).

Uso (desde labs/lab4a-pipelines):
    docker compose exec airflow-scheduler python /opt/airflow/data/raw/contar_filas.py
"""
import os

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

OUT = "/opt/airflow/data/output"
BRONZE = f"{OUT}/bronze/ventas_partitioned"
SILVER = f"{OUT}/silver/ventas"
GOLD_REGION = f"{OUT}/gold/ventas_por_region"

builder = (
    SparkSession.builder.appName("lab4a-conteos")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n===== BRONZE (filas por particion) =====")
total_bronze = 0
for carpeta in sorted(os.listdir(BRONZE)):
    if carpeta.startswith("fecha="):
        n = spark.read.parquet(f"{BRONZE}/{carpeta}").count()
        total_bronze += n
        print(f"  {carpeta}: {n}")
print(f"  TOTAL BRONZE: {total_bronze}")

print("\n===== SILVER =====")
silver = spark.read.format("delta").load(SILVER)
print(f"  TOTAL SILVER: {silver.count()}")
print(f"  pedido_id distintos: {silver.select('pedido_id').distinct().count()}")
silver.groupBy("fecha").count().orderBy("fecha").show()

print("===== SILVER: historial Delta (versiones) =====")
DeltaTable.forPath(spark, SILVER).history().select("version", "timestamp", "operation").orderBy("version").show(truncate=False)

print("===== GOLD: ventas_por_region =====")
spark.read.format("delta").load(GOLD_REGION).orderBy("region").show()

spark.stop()
