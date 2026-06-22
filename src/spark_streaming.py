import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_date, lit,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
)

from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    S3_BRONZE_BUCKET, AWS_REGION,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schema for the Kafka message value
MESSAGE_SCHEMA = StructType([
    StructField("nctId", StringType(), True),
    StructField("overallStatus", StringType(), True),
    StructField("briefTitle", StringType(), True),
    StructField("lastUpdatePostDate", StringType(), True),
])


def build_spark_session(app_name: str = "NIH-Trials-Streaming") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.s3a.region", AWS_REGION)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def run_streaming_job(spark: SparkSession, checkpoint_dir: str = "/tmp/spark-checkpoints") -> None:
    """
    Consume clinical-trials-updates topic and write Parquet to S3 bronze.
    Partitioned by ingest_date and overallStatus.
    """
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_timestamp")
        .withColumn("data", from_json(col("json_str"), MESSAGE_SCHEMA))
        .select(
            col("data.nctId").alias("nct_id"),
            col("data.overallStatus").alias("overall_status"),
            col("data.briefTitle").alias("brief_title"),
            col("data.lastUpdatePostDate").alias("last_update_post_date"),
            col("kafka_timestamp"),
            current_timestamp().alias("ingest_timestamp"),
            to_date(current_timestamp()).alias("ingest_date"),
        )
        .filter(col("nct_id").isNotNull())
    )

    bronze_path = f"s3a://{S3_BRONZE_BUCKET}/parquet/"

    query = (
        parsed.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", bronze_path)
        .option("checkpointLocation", checkpoint_dir)
        .partitionBy("ingest_date", "overall_status")
        .trigger(processingTime="60 seconds")
        .start()
    )

    logger.info(f"Streaming query started — writing to {bronze_path}")
    query.awaitTermination()


if __name__ == "__main__":
    spark = build_spark_session()
    run_streaming_job(spark)
