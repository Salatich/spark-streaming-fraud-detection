"""Unit tests for Streamlit UI clients and metadata helpers."""

from ui.iceberg_client import get_medallion_counts, get_table_snapshots, load_table_df
from ui.kafka_client import get_consumer


def test_ui_medallion_counts_structure():
    """Verify get_medallion_counts returns expected keys and non-negative integers."""
    counts = get_medallion_counts()
    assert isinstance(counts, dict)
    assert "bronze" in counts
    assert "silver" in counts
    assert "gold" in counts
    assert counts["bronze"] >= 0
    assert counts["silver"] >= 0
    assert counts["gold"] >= 0


def test_ui_silver_df_load():
    """Verify loading silver table returns a pandas DataFrame."""
    df = load_table_df("silver_transactions", limit=10)
    assert hasattr(df, "columns")
    if not df.empty:
        assert "transaction_id" in df.columns
        assert "user_id" in df.columns


def test_ui_snapshots_fetch():
    """Verify retrieving snapshots returns a list of dictionaries with snapshot_id."""
    snapshots = get_table_snapshots("silver_transactions")
    assert isinstance(snapshots, list)
    if snapshots:
        first = snapshots[0]
        assert "snapshot_id" in first
        assert "committed_at" in first
        assert "operation" in first


def test_ui_kafka_consumer_init():
    """Verify temporary Kafka consumer configures cleanly without exceptions."""
    consumer = get_consumer("fraud.alerts")
    assert consumer is not None
    consumer.close()
