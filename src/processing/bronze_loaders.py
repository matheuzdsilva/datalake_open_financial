import os
import json
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from utils.spark import get_spark

RAW_DIR = "data/raw"

def _list_json_files(path: str):
    if not os.path.exists(path):
        return []
    return [
        os.path.join(path, f)
        for f in os.listdir(path)
        if f.endswith(".json")
    ]

def load_bcb_bronze(spark) -> DataFrame:
    path = os.path.join(RAW_DIR, "bcb", "cambio")
    files = _list_json_files(path)
    if not files:
        return spark.createDataFrame([], schema="data string, valor string, indicador string, fonte string, data_ingestao string")
    rows = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            content = json.load(fh)
            for item in content:
                rows.append(Row(
                    data=item.get("data"),
                    valor=item.get("valor"),
                    indicador=item.get("indicador"),
                    fonte=item.get("fonte", "bcb"),
                    data_ingestao=item.get("data_ingestao")
                ))
    df = spark.createDataFrame(rows)
    
    df = (
        df
        .withColumn("data_ref",
                    F.to_date(F.col("data"), "dd/MM/yyyy"))
        .withColumn("valor_num", F.col("valor").cast("double"))
        .drop("data")
    )
    return df

def load_ibge_bronze(spark) -> DataFrame:
    path = os.path.join(RAW_DIR, "ibge", "ipca")
    files = _list_json_files(path)
    if not files:
        return spark.createDataFrame([], schema="ano int, mes int, periodo string, valor double, indicador string, fonte string, data_ingestao string")
    rows = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            content = json.load(fh)
            for item in content:
                rows.append(Row(
                    ano=item.get("ano"),
                    mes=item.get("mes"),
                    periodo=item.get("periodo"),
                    valor=item.get("valor"),
                    indicador=item.get("indicador", "ipca_mensal"),
                    fonte=item.get("fonte", "ibge"),
                    data_ingestao=item.get("data_ingestao")
                ))
    df = spark.createDataFrame(rows)

    df = (
        df
        .withColumn("data_ref",
                    F.to_date(F.concat_ws("-", F.col("ano"), F.col("mes"), F.lit(1))))
    )
    return df

def load_eia_bronze(spark) -> DataFrame:
    path = os.path.join(RAW_DIR, "eia", "petroleo")
    files = _list_json_files(path)
    if not files:
        return spark.createDataFrame([], schema="data_ref string, valor double, indicador string, fonte string, data_ingestao string")
    rows = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            content = json.load(fh)
            if isinstance(content, list):
                data_iter = content
            else:
                data_iter = content
            for item in data_iter:
                rows.append(Row(
                    data_ref=item.get("data_ref"),
                    valor=item.get("valor"),
                    indicador=item.get("indicador", "brent_spot_usd_bbl"),
                    fonte=item.get("fonte", "eia"),
                    data_ingestao=item.get("data_ingestao")
                ))
    df = spark.createDataFrame(rows)
    df = df.withColumn("data_ref", F.to_date("data_ref"))
    return df
