import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql import DataFrame


def add_ingestion_date(df: DataFrame) -> DataFrame:
    return df.withColumn("ingestion_date", F.cast(TimestampType(), F.current_timestamp()))


def add_surrogate_key(*fields, df: DataFrame, key_name: str) -> DataFrame:
    return df.withColumn(key_name, F.sha2(F.concat(*fields), numBits=256))


def overwrite_table(df: DataFrame, save_path: str, partition_fields: list[str]):
    if partition_fields:
        (df.write
        .format("delta")
        .partitionBy(*partition_fields)
        .option("path", save_path)
        .mode("overwrite")
        .save())
    else:
        (df.write
        .format("delta")
        .option("path", save_path)
        .mode("overwrite")
        .save())


def show_null_counts(df: DataFrame):
    display(df.select([F.count(F.when(F.isnotnull(c), F.col(c))).alias(c) for c in df.columns]))