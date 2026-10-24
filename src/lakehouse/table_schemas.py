"""DDL table definitions and schema management for Apache Iceberg tables."""

import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger("LakehouseSchemas")

# PySpark Schema for parsing incoming Transaction JSON
TRANSACTION_PAYLOAD_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("card_token", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField(
            "location",
            StructType(
                [
                    StructField("city", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True),
                ]
            ),
            True,
        ),
        StructField("device_id", StringType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("is_fraud_injected", BooleanType(), True),
    ]
)


def initialize_iceberg_tables(spark: SparkSession) -> None:
    """Create namespace and Medallion Iceberg tables if they do not exist."""
    logger.info("Initializing Iceberg tables in catalog 'lakehouse'...")

    # 1. Namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.fraud_detection")

    # 2. Bronze Table (Append-only raw events with ingestion metadata)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.fraud_detection.bronze_transactions (
            kafka_key STRING,
            raw_payload STRING,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            ingestion_time TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ingestion_time))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    logger.info("  [+] Bronze table: lakehouse.fraud_detection.bronze_transactions initialized.")

    # 3. Silver Table (Cleaned, validated, strongly typed, deduplicated)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.fraud_detection.silver_transactions (
            transaction_id STRING,
            user_id STRING,
            card_token STRING,
            amount DOUBLE,
            currency STRING,
            merchant_id STRING,
            merchant_category STRING,
            city STRING,
            country STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            device_id STRING,
            event_time TIMESTAMP,
            is_fraud_injected BOOLEAN,
            processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    logger.info("  [+] Silver table: lakehouse.fraud_detection.silver_transactions initialized.")

    # 4. Gold Table (Enriched fraud incidents and compliance audit store)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.fraud_detection.gold_fraud_alerts (
            alert_id STRING,
            transaction_id STRING,
            user_id STRING,
            card_token STRING,
            amount DOUBLE,
            trigger_reason STRING,
            severity STRING,
            origin_city STRING,
            dest_city STRING,
            distance_km DOUBLE,
            time_delta_seconds DOUBLE,
            detected_at TIMESTAMP,
            event_time TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(detected_at))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    logger.info("  [+] Gold table: lakehouse.fraud_detection.gold_fraud_alerts initialized.")

    # 5. Dead-Letter Queue (DLQ) Table (Quarantined corrupted/malformed events)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.fraud_detection.quarantine_dlq (
            dlq_id STRING,
            raw_payload STRING,
            error_reason STRING,
            kafka_offset BIGINT,
            quarantined_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(quarantined_at))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    logger.info("  [+] Quarantine DLQ table: lakehouse.fraud_detection.quarantine_dlq initialized.")


if __name__ == "__main__":
    from lakehouse.spark_session import get_spark_session

    spark = get_spark_session("init-iceberg-tables")
    initialize_iceberg_tables(spark)
    tables = spark.sql("SHOW TABLES IN lakehouse.fraud_detection").collect()
    print("Tables in Iceberg catalog:")
    for t in tables:
        print(f" - {t.tableName}")
    spark.stop()
