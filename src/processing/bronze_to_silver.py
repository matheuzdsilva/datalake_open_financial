import sys, os
sys.path.insert(0, "/src")

from datetime import datetime
from pyspark.sql import functions as F
from utils.spark import get_spark
from processing.bronze_loaders import (
    load_bcb_bronze,
    load_ibge_bronze,
    load_eia_bronze,
)

BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"

USE_MINIO = os.getenv("USE_MINIO", "false").lower() == "true"

def get_target_path(base_dir: str, folder: str) -> str:

    if USE_MINIO:
        bucket = os.getenv("MINIO_BUCKET", "openmacrolake")
        return f"s3a://{bucket}/{base_dir}/{folder}"
    else:
        return f"file:/src/{base_dir}/{folder}"

def main():
    spark = get_spark("openmacrolake-bronze-silver")

    df_bcb = load_bcb_bronze(spark)
    df_ibge = load_ibge_bronze(spark)
    df_eia = load_eia_bronze(spark)

    if not USE_MINIO:
        for sub in ["bcb", "ibge", "eia"]:
            os.makedirs(os.path.join(BRONZE_DIR, sub), exist_ok=True)

    if df_bcb.count() > 0:
        df_bcb.write.mode("overwrite").parquet(get_target_path("data/bronze", "bcb"))
    if df_ibge.count() > 0:
        df_ibge.write.mode("overwrite").parquet(get_target_path("data/bronze", "ibge"))
    if df_eia.count() > 0:
        df_eia.write.mode("overwrite").parquet(get_target_path("data/bronze", "eia"))

    sel_bcb = df_bcb.select(
        F.col("data").alias("data_ref"),
        F.col("valor").alias("valor"),
        "indicador",
        "fonte"
    )
    sel_ibge = df_ibge.select(
        F.col("periodo").alias("data_ref"),
        F.col("valor").alias("valor"),
        "indicador",
        "fonte"
    )
    sel_eia = df_eia.select(
        F.col("data_ref").alias("data_ref"),
        F.col("valor").alias("valor"),
        "indicador",
        "fonte"
    )

    unified = (
        sel_bcb
        .unionByName(sel_ibge, allowMissingColumns=True)
        .unionByName(sel_eia, allowMissingColumns=True)
        .withColumn("ano", F.year("data_ref"))
        .withColumn("mes", F.month("data_ref"))
    )

    if not USE_MINIO:
        os.makedirs(os.path.join(SILVER_DIR, "macro_indicadores"), exist_ok=True)

    unified.write.mode("overwrite").partitionBy("ano", "mes").parquet(
        get_target_path("data/silver", "macro_indicadores")
    )

    print(f"[INFO] Bronze → Silver concluído com sucesso! "
          f"Destino: {'MinIO' if USE_MINIO else 'Local FS'}")

if __name__ == "__main__":
    main()
