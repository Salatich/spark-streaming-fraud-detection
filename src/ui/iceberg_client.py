"""PyIceberg client for querying Medallion Lakehouse tables and commit metadata."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from pyiceberg.catalog import Catalog, load_catalog

logger = logging.getLogger("UI_IcebergClient")


def get_catalog() -> Catalog:
    """Initialize PyIceberg catalog connecting to local REST Catalog and MinIO."""
    rest_uri = os.getenv("ICEBERG_REST_URI", "http://localhost:8181")
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password123")

    return load_catalog(
        "lakehouse",
        **{
            "type": "rest",
            "uri": rest_uri,
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": aws_access_key,
            "s3.secret-access-key": aws_secret_key,
            "s3.path-style-access": "true",
        },
    )


def load_table_df(table_name: str, limit: int = 500) -> pd.DataFrame:
    """Load records from an Iceberg table as a Pandas DataFrame."""
    try:
        cat = get_catalog()
        tbl = cat.load_table(("fraud_detection", table_name))
        arrow_tbl = tbl.scan(limit=limit).to_arrow()
        df = arrow_tbl.to_pandas()
        return df
    except Exception as e:
        logger.warning(f"Could not load table 'fraud_detection.{table_name}': {e}")
        return pd.DataFrame()


def get_table_snapshots(table_name: str) -> list[dict[str, Any]]:
    """Retrieve commit snapshot history for an Iceberg table."""
    try:
        cat = get_catalog()
        tbl = cat.load_table(("fraud_detection", table_name))
        snapshots = []
        for s in tbl.snapshots():
            ts_sec = s.timestamp_ms / 1000.0
            dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
            snapshots.append(
                {
                    "snapshot_id": str(s.snapshot_id),
                    "committed_at": dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "operation": str(s.summary.operation).replace("Operation.", ""),
                    "manifest_list": s.manifest_list,
                }
            )
        return list(reversed(snapshots))
    except Exception as e:
        logger.warning(f"Could not load snapshots for '{table_name}': {e}")
        return []


def get_medallion_counts() -> dict[str, int]:
    """Get row counts for Bronze, Silver, and Gold tables."""
    counts = {"bronze": 0, "silver": 0, "gold": 0}
    try:
        cat = get_catalog()
        for key, name in [
            ("bronze", "bronze_transactions"),
            ("silver", "silver_transactions"),
            ("gold", "gold_fraud_alerts"),
        ]:
            try:
                tbl = cat.load_table(("fraud_detection", name))
                arrow_tbl = tbl.scan().to_arrow()
                counts[key] = arrow_tbl.num_rows
            except Exception:
                counts[key] = 0
    except Exception as e:
        logger.warning(f"Could not fetch medallion counts: {e}")
    return counts
