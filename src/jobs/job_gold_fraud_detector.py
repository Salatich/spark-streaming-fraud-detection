"""Spark Streaming Job 2: Stateful Fraud Detection Engine (Silver -> Gold & Kafka Alerts).

Streams cleaned transactions directly from Apache Iceberg 'silver_transactions', maintains
per-user state across streaming micro-batches, evaluates anomaly detection rules
(Impossible Geographic Travel & High-Frequency Velocity Bursts), and fans out results
to Kafka 'fraud.alerts' and Apache Iceberg 'gold_fraud_alerts'.
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    asin,
    col,
    concat,
    cos,
    current_timestamp,
    expr,
    lag,
    lit,
    max_by,
    pow,
    radians,
    round,
    sin,
    sqrt,
    struct,
    to_json,
    unix_timestamp,
    when,
)
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse.spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("GoldFraudDetector")

# Minimal schema for prior user state reference rows during anomaly detection
PRIOR_STATE_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("card_token", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("city", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)

GOLD_ALERT_SCHEMA = StructType(
    [
        StructField("alert_id", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("card_token", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("trigger_reason", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("origin_city", StringType(), True),
        StructField("dest_city", StringType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("time_delta_seconds", DoubleType(), True),
        StructField("detected_at", TimestampType(), False),
        StructField("event_time", TimestampType(), False),
    ]
)


class GoldObservabilityListener(StreamingQueryListener):
    """Custom streaming query listener tracking micro-batch progress and rate metrics."""

    def onQueryStarted(self, event):
        logger.info(f"[STREAM START] Query '{event.name}' [ID: {event.id}] started.")

    def onQueryProgress(self, event):
        progress = event.progress
        name = progress.name
        batch_id = progress.batchId
        input_rows = progress.numInputRows
        input_rate = progress.inputRowsPerSecond
        process_rate = progress.processedRowsPerSecond

        if input_rows > 0:
            logger.info(
                f"[PROGRESS] Query='{name}' | Batch={batch_id} | Rows={input_rows} | "
                f"InputRate={input_rate:.1f} r/s | ProcessRate={process_rate:.1f} r/s"
            )

    def onQueryTerminated(self, event):
        logger.warning(f"[STREAM END] Query '{event.id}' terminated. Exception: {event.exception}")


class FraudDetectionEngine:
    """Stateful stream processing engine detecting geo-velocity and burst fraud."""

    def __init__(
        self,
        kafka_servers: str,
        alert_topic: str = "fraud.alerts",
        max_speed_kmh: float = 800.0,
        min_impossible_distance_km: float = 500.0,
        burst_window_seconds: float = 60.0,
        burst_min_amount: float = 200.0,
    ):
        self.kafka_servers = kafka_servers
        self.alert_topic = alert_topic
        self.max_speed_kmh = max_speed_kmh
        self.min_impossible_distance_km = min_impossible_distance_km
        self.burst_window_seconds = burst_window_seconds
        self.burst_min_amount = burst_min_amount

        # In-memory per-user state cache: user_id -> dict of latest known transaction details
        self.user_state: dict[str, dict[str, Any]] = {}

    def warm_state_from_iceberg(self, spark: SparkSession) -> None:
        """Warm driver state cache from the latest transaction per user in silver_transactions."""
        try:
            logger.info(
                "Warming user state cache from 'lakehouse.fraud_detection.silver_transactions'..."
            )
            latest_df = (
                spark.table("lakehouse.fraud_detection.silver_transactions")
                .groupBy("user_id")
                .agg(
                    max_by(
                        struct(
                            "latitude",
                            "longitude",
                            "city",
                            "country",
                            "event_time",
                            "transaction_id",
                            "card_token",
                            "amount",
                        ),
                        "event_time",
                    ).alias("latest")
                )
            )
            rows = latest_df.collect()
            for r in rows:
                self.user_state[r.user_id] = {
                    "latitude": r.latest.latitude,
                    "longitude": r.latest.longitude,
                    "city": r.latest.city,
                    "country": r.latest.country,
                    "event_time": r.latest.event_time,
                    "transaction_id": r.latest.transaction_id,
                    "card_token": r.latest.card_token,
                    "amount": r.latest.amount,
                }
            logger.info(f"Successfully warmed state cache with {len(rows)} user profiles.")
        except Exception as e:
            logger.warning(f"State warming skipped (table might be empty or first launch): {e}")

    def detect_anomalies(self, batch_df: DataFrame) -> DataFrame:
        """Apply mathematical Haversine geo-velocity and spending burst rules."""
        spark = batch_df.sparkSession

        # Identify distinct users in current micro-batch
        batch_users = [row.user_id for row in batch_df.select("user_id").distinct().collect()]
        if not batch_users:
            return spark.createDataFrame([], schema=GOLD_ALERT_SCHEMA)

        # Build prior transaction rows for any users currently cached in state
        prior_rows = [
            (
                self.user_state[uid].get("transaction_id"),
                uid,
                self.user_state[uid].get("card_token"),
                self.user_state[uid].get("amount", 0.0),
                self.user_state[uid].get("city"),
                self.user_state[uid].get("latitude"),
                self.user_state[uid].get("longitude"),
                self.user_state[uid].get("event_time"),
            )
            for uid in batch_users
            if uid in self.user_state
        ]

        current_df = batch_df.select(
            "transaction_id",
            "user_id",
            "card_token",
            "amount",
            "city",
            "latitude",
            "longitude",
            "event_time",
        ).withColumn("is_prior", lit(False))

        if prior_rows:
            prior_df = spark.createDataFrame(prior_rows, schema=PRIOR_STATE_SCHEMA).withColumn(
                "is_prior", lit(True)
            )
            combined_df = prior_df.unionByName(current_df)
        else:
            combined_df = current_df

        # Window over each user ordered by event_time to obtain preceding transaction
        window_spec = Window.partitionBy("user_id").orderBy("event_time")
        enriched = (
            combined_df.withColumn("prev_lat", lag("latitude").over(window_spec))
            .withColumn("prev_lon", lag("longitude").over(window_spec))
            .withColumn("prev_city", lag("city").over(window_spec))
            .withColumn("prev_time", lag("event_time").over(window_spec))
            .filter(~col("is_prior"))  # Only evaluate new records in this batch
        )

        # Event-time delta in seconds
        time_delta_sec = unix_timestamp(col("event_time")) - unix_timestamp(col("prev_time"))

        # Haversine distance formula in PySpark SQL
        dlat = radians(col("latitude") - col("prev_lat"))
        dlon = radians(col("longitude") - col("prev_lon"))
        lat1 = radians(col("prev_lat"))
        lat2 = radians(col("latitude"))
        a = pow(sin(dlat / 2.0), 2) + cos(lat1) * cos(lat2) * pow(sin(dlon / 2.0), 2)
        c = 2.0 * asin(sqrt(a))
        distance_km = lit(6371.0) * c
        # Spark 4.1 ANSI-safe speed calculation avoiding divide-by-zero on concurrent/prior timestamps
        speed_kmh = when(
            col("prev_lat").isNotNull() & (time_delta_sec > 0),
            distance_km / (time_delta_sec / 3600.0),
        ).otherwise(lit(0.0))

        # Rule 1: Impossible Travel (speed > 800 km/h and distance > 500 km)
        is_impossible_travel = (
            col("prev_lat").isNotNull()
            & (time_delta_sec > 0)
            & (distance_km > self.min_impossible_distance_km)
            & (speed_kmh > self.max_speed_kmh)
        )

        # Rule 2: Velocity Burst (rapid transaction <= 60s with amount > $200)
        is_velocity_burst = (
            col("prev_lat").isNotNull()
            & (time_delta_sec > 0)
            & (time_delta_sec <= self.burst_window_seconds)
            & (col("amount") > self.burst_min_amount)
        )

        alerts_df = (
            enriched.filter(is_impossible_travel | is_velocity_burst)
            .withColumn("alert_id", expr("uuid()"))
            .withColumn("origin_city", col("prev_city"))
            .withColumn("dest_city", col("city"))
            .withColumn("distance_km", round(distance_km, 2))
            .withColumn("time_delta_seconds", round(time_delta_sec.cast("double"), 2))
            .withColumn("detected_at", current_timestamp())
            .withColumn(
                "trigger_reason",
                when(
                    is_impossible_travel,
                    concat(
                        lit("Impossible travel: "),
                        col("prev_city"),
                        lit(" -> "),
                        col("city"),
                        lit(" ("),
                        round(distance_km, 0),
                        lit(" km in "),
                        time_delta_sec,
                        lit("s)"),
                    ),
                ).otherwise(
                    concat(
                        lit("Velocity burst: $"),
                        round(col("amount"), 2),
                        lit(" within "),
                        time_delta_sec,
                        lit("s"),
                    )
                ),
            )
            .withColumn(
                "severity",
                when(is_impossible_travel, lit("CRITICAL")).otherwise(lit("HIGH")),
            )
        )

        return alerts_df

    def process_micro_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Micro-batch handler executing anomaly detection and multi-sink dispatch."""
        if batch_df.rdd.isEmpty():
            return

        batch_count = batch_df.count()
        logger.info(f"Processing micro-batch {batch_id}: {batch_count} transaction(s)...")

        # 1. Run anomaly detection
        alerts_df = self.detect_anomalies(batch_df)
        alert_count = alerts_df.count()

        if alert_count > 0:
            logger.warning(
                f"[ALERT] Detected {alert_count} suspicious transaction(s) in batch {batch_id}!"
            )

            # SINK 1: Fast Operational Path -> Kafka 'fraud.alerts'
            kafka_payload = alerts_df.select(
                col("card_token").alias("key"),
                to_json(
                    struct(
                        col("alert_id"),
                        col("transaction_id"),
                        col("user_id"),
                        col("card_token"),
                        col("amount"),
                        col("trigger_reason"),
                        col("severity"),
                        col("detected_at"),
                        struct(
                            col("origin_city"),
                            col("dest_city"),
                            col("distance_km"),
                            col("time_delta_seconds"),
                            col("event_time"),
                        ).alias("details"),
                    )
                ).alias("value"),
            )
            (
                kafka_payload.write.format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_servers)
                .option("topic", self.alert_topic)
                .save()
            )
            logger.info(
                f"  [+] Emitted {alert_count} alert(s) to Kafka topic '{self.alert_topic}'."
            )

            # SINK 2: Analytical Lakehouse Path -> Iceberg 'gold_fraud_alerts'
            gold_records = alerts_df.select(
                col("alert_id"),
                col("transaction_id"),
                col("user_id"),
                col("card_token"),
                col("amount"),
                col("trigger_reason"),
                col("severity"),
                col("origin_city"),
                col("dest_city"),
                col("distance_km"),
                col("time_delta_seconds"),
                col("detected_at"),
                col("event_time"),
            )
            (
                gold_records.write.format("iceberg")
                .mode("append")
                .save("lakehouse.fraud_detection.gold_fraud_alerts")
            )
            logger.info(f"  [+] Appended {alert_count} record(s) to Iceberg 'gold_fraud_alerts'.")
        else:
            logger.info(f"Micro-batch {batch_id}: All {batch_count} transactions verified clean.")

        # 2. Update state cache with the latest transaction per user in this batch
        latest_in_batch = (
            batch_df.groupBy("user_id")
            .agg(
                max_by(
                    struct(
                        "latitude",
                        "longitude",
                        "city",
                        "country",
                        "event_time",
                        "transaction_id",
                        "card_token",
                        "amount",
                    ),
                    "event_time",
                ).alias("latest")
            )
            .collect()
        )

        for row in latest_in_batch:
            self.user_state[row.user_id] = {
                "latitude": row.latest.latitude,
                "longitude": row.latest.longitude,
                "city": row.latest.city,
                "country": row.latest.country,
                "event_time": row.latest.event_time,
                "transaction_id": row.latest.transaction_id,
                "card_token": row.latest.card_token,
                "amount": row.latest.amount,
            }


def run_pipeline(once: bool = False, trigger_interval: str = "5 seconds"):
    """Initialize Spark streaming query reading from Silver Iceberg table."""
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    alert_topic = os.getenv("KAFKA_ALERTS_TOPIC", "fraud.alerts")

    logger.info(
        f"Initializing SparkSession for Job 2 (mode={'AvailableNow' if once else 'Continuous'})..."
    )
    spark = get_spark_session("gold-fraud-detector-job")
    spark.streams.addListener(GoldObservabilityListener())

    fraud_engine = FraudDetectionEngine(
        kafka_servers=kafka_servers,
        alert_topic=alert_topic,
    )
    fraud_engine.warm_state_from_iceberg(spark)

    logger.info(
        "Reading streaming DataFrame from Iceberg table: "
        "'lakehouse.fraud_detection.silver_transactions'..."
    )
    stream_df = spark.readStream.format("iceberg").table(
        "lakehouse.fraud_detection.silver_transactions"
    )

    trigger_args = {"availableNow": True} if once else {"processingTime": trigger_interval}

    logger.info("Starting Gold Fraud Detection Stream...")
    query = (
        stream_df.writeStream.queryName("gold_fraud_detector_stream")
        .foreachBatch(fraud_engine.process_micro_batch)
        .option("checkpointLocation", "s3a://warehouse/checkpoints/gold_fraud_detector/")
        .trigger(**trigger_args)
        .start()
    )

    if once:
        logger.info("Executing single micro-batch pass (availableNow=True)...")
        query.awaitTermination()
        logger.info("Job 2 batch pass completed successfully.")
        spark.stop()
    else:
        logger.info("Job 2 streaming continuously. Awaiting termination...")
        query.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Stateful Fraud Detection Job")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Execute available micro-batches and exit (AvailableNow mode)",
    )
    parser.add_argument(
        "--trigger-interval",
        default="5 seconds",
        help="Micro-batch trigger interval (e.g., '5 seconds')",
    )
    args = parser.parse_args()

    run_pipeline(once=args.once, trigger_interval=args.trigger_interval)
