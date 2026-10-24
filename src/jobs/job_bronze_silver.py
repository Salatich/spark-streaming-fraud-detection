"""Spark Streaming Job 1: Ingestion (Bronze), Cleansing (Silver), and DLQ (Kafka)."""

from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lit,
    struct,
    to_json,
)
from pyspark.sql.streaming import StreamingQueryListener

from lakehouse.spark_session import get_spark_session
from lakehouse.table_schemas import (
    TRANSACTION_PAYLOAD_SCHEMA,
    initialize_iceberg_tables,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("BronzeSilverJob")


class ObservabilityListener(StreamingQueryListener):
    """Custom listener capturing micro-batch telemetry for production observability."""

    def onQueryStarted(self, event):
        logger.info(f"[STREAM START] Query '{event.name}' [ID: {event.id}] started.")

    def onQueryProgress(self, event):
        progress = event.progress
        name = progress.name
        batch_id = progress.batchId
        input_rows = progress.numInputRows
        input_rate = progress.inputRowsPerSecond
        process_rate = progress.processedRowsPerSecond
        watermark = progress.eventTime.get("watermark", "N/A")

        if input_rows > 0:
            logger.info(
                f"[PROGRESS] Query='{name}' | Batch={batch_id} | Rows={input_rows} | "
                f"InputRate={input_rate:.1f} r/s | ProcessRate={process_rate:.1f} r/s | "
                f"Watermark={watermark}"
            )

    def onQueryTerminated(self, event):
        logger.warning(f"[STREAM END] Query '{event.id}' terminated. Exception: {event.exception}")


def build_bronze_stream(
    spark: SparkSession,
    kafka_servers: str,
    raw_topic: str,
    max_offsets: int = 500,
) -> DataFrame:
    """Read raw events from Kafka and structure for Bronze Iceberg table."""
    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
    raw_kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", raw_topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", max_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze_df = raw_kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_payload"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingestion_time"),
    )
    return bronze_df


def process_silver_and_dlq(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Parse JSON, filter malformed records to Kafka DLQ, and deduplicate with watermarking."""
    parsed_df = bronze_df.withColumn(
        "parsed",
        from_json(col("raw_payload"), TRANSACTION_PAYLOAD_SCHEMA),
    )

    # 1. Dead-Letter Queue (DLQ) condition: unparseable JSON, missing ID, or negative amount
    is_malformed = (
        col("parsed").isNull()
        | col("parsed.transaction_id").isNull()
        | (col("parsed.amount") <= 0.0)
    )

    # Format for Kafka sink (requires key and value columns)
    dlq_df = parsed_df.filter(is_malformed).select(
        col("kafka_key").alias("key"),
        to_json(
            struct(
                col("raw_payload"),
                lit("Validation failed or negative amount").alias("error_reason"),
                col("kafka_offset"),
                col("kafka_partition"),
                current_timestamp().alias("quarantined_at"),
            )
        ).alias("value"),
    )

    # 2. Valid Silver records
    valid_df = (
        parsed_df.filter(~is_malformed)
        .select(
            col("parsed.transaction_id").alias("transaction_id"),
            col("parsed.user_id").alias("user_id"),
            col("parsed.card_token").alias("card_token"),
            col("parsed.amount").alias("amount"),
            col("parsed.currency").alias("currency"),
            col("parsed.merchant_id").alias("merchant_id"),
            col("parsed.merchant_category").alias("merchant_category"),
            col("parsed.location.city").alias("city"),
            col("parsed.location.country").alias("country"),
            col("parsed.location.latitude").alias("latitude"),
            col("parsed.location.longitude").alias("longitude"),
            col("parsed.device_id").alias("device_id"),
            col("parsed.event_time").alias("event_time"),
            col("parsed.is_fraud_injected").alias("is_fraud_injected"),
            current_timestamp().alias("processed_at"),
        )
        # Apply event-time watermark: late data arriving >10 mins after watermark is dropped
        .withWatermark("event_time", "10 minutes")
        # Streaming stateful deduplication bounded by event_time watermark
        .dropDuplicates(["transaction_id", "event_time"])
    )

    return valid_df, dlq_df


def run_pipeline(once: bool = False):
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    raw_topic = os.getenv("KAFKA_RAW_TOPIC", "transactions.raw")
    dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", "transactions.dlq")

    logger.info(f"Initializing SparkSession (mode={'AvailableNow' if once else 'Continuous'})...")
    spark = get_spark_session("bronze-silver-streaming-job")

    # Attach observability listener
    spark.streams.addListener(ObservabilityListener())

    # Ensure tables exist
    initialize_iceberg_tables(spark)

    logger.info("Starting Bronze ingestion stream...")
    bronze_df = build_bronze_stream(spark, kafka_servers, raw_topic)

    # Configure trigger strategy
    trigger_args = {"availableNow": True} if once else {"processingTime": "5 seconds"}

    # Query 1: Write raw events to Bronze Iceberg table
    bronze_query = (
        bronze_df.writeStream.queryName("bronze_ingestion_stream")
        .format("iceberg")
        .outputMode("append")
        .trigger(**trigger_args)
        .option("checkpointLocation", "s3a://warehouse/checkpoints/bronze_transactions/")
        .toTable("lakehouse.fraud_detection.bronze_transactions")
    )

    # Query 2 & 3: Parse Silver and quarantine DLQ to Kafka
    silver_df, dlq_df = process_silver_and_dlq(bronze_df)

    logger.info("Starting Silver clean & deduplication stream...")
    silver_query = (
        silver_df.writeStream.queryName("silver_cleansing_stream")
        .format("iceberg")
        .outputMode("append")
        .trigger(**trigger_args)
        .option("checkpointLocation", "s3a://warehouse/checkpoints/silver_transactions/")
        .toTable("lakehouse.fraud_detection.silver_transactions")
    )

    logger.info(f"Starting DLQ stream -> Kafka topic '{dlq_topic}'...")
    dlq_query = (
        dlq_df.writeStream.queryName("dlq_kafka_stream")
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("topic", dlq_topic)
        .trigger(**trigger_args)
        .option("checkpointLocation", "s3a://warehouse/checkpoints/dlq_kafka/")
        .start()
    )

    active_queries = [bronze_query, silver_query, dlq_query]
    logger.info(f"Active queries started: {[q.name for q in active_queries]}")

    if once:
        logger.info("Trigger.AvailableNow set: Processing all pending micro-batches...")
        for q in active_queries:
            q.awaitTermination()
        logger.info("All pending batches processed successfully.")
    else:
        logger.info("Awaiting termination on all active streaming queries...")
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Bronze & Silver Streaming Ingestion")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Execute all available micro-batches using Trigger.AvailableNow and exit.",
    )
    args = parser.parse_args()
    run_pipeline(once=args.once)
