import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_spark_session(gcp_project: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("user_main")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/etc/gcp/key.json")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("parentProject", gcp_project)
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
        .getOrCreate()
    )


def read_parquet(spark: SparkSession, gcs_path: str):
    logger.info(f"Reading parquet from {gcs_path}")
    df = spark.read.parquet(gcs_path)
    logger.info(f"Schema: {df.schema}")
    logger.info(f"Row count: {df.count()}")
    return df


def apply_transformations(df):
    logger.info("Applying transformations")

    df = (
        df
        .withColumn("ingested_at",   F.current_timestamp())
    )

    logger.info(f"Row count after transforms: {df.count()}")
    return df


def write_to_bq(df, gcp_project: str, bq_dataset: str, bq_table: str, gcs_temp_bucket: str):
    destination = f"{gcp_project}.{bq_dataset}.{bq_table}"
    logger.info(f"Writing to BigQuery: {destination}")

    (
        df.write
        .format("bigquery")
        .option("table",                destination)
        .option("temporaryGcsBucket",   gcs_temp_bucket)   # BQ connector stages data here first
        .option("writeMethod",          "indirect")         # streams via GCS → BQ
        .mode("overwrite")                                     
        .save()
    )
    logger.info("Write to BigQuery complete")


def main():
    # Args passed from SparkApplication via spec.arguments
    gcp_project     = sys.argv[1]
    gcs_input_path  = sys.argv[2]   # e.g. gs://my-bucket/data/input/
    bq_dataset      = sys.argv[3]
    bq_table        = sys.argv[4]
    gcs_temp_bucket = sys.argv[5]   # e.g. my-bucket-temp
#    date = sys.argv[7]

    try:
        spark = build_spark_session(gcp_project)
        df = read_parquet(spark, gcs_input_path)
        df_transformed = apply_transformations(df)
        write_to_bq(df_transformed, gcp_project, bq_dataset, bq_table, gcs_temp_bucket)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
