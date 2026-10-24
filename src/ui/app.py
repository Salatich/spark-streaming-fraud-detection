"""Streamlit Real-Time Fraud Operations and Medallion Lakehouse Dashboard."""

from __future__ import annotations

import sys
from pathlib import Path
src_path = str(Path(__file__).resolve().parent.parent)
if src_path not in sys.path:
    sys.path.insert(0, src_path)

import pandas as pd
import streamlit as st

from ui.iceberg_client import (
    get_medallion_counts,
    get_table_snapshots,
    load_table_df,
)
from ui.kafka_client import consume_recent_messages

st.set_page_config(
    page_title="Streaming Fraud Lakehouse",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for styling
st.markdown(
    """
    <style>
    .metric-card {
        background-color: #1E222A;
        border-radius: 8px;
        padding: 12px 18px;
        margin-bottom: 10px;
    }
    .badge-critical {
        background-color: #FF4B4B;
        color: white;
        padding: 3px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .badge-high {
        background-color: #FFA500;
        color: black;
        padding: 3px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def render_sidebar():
    st.sidebar.title("⚡ Control Center")
    st.sidebar.caption("Spark Structured Streaming + Apache Iceberg")

    st.sidebar.markdown("---")
    st.sidebar.subheader("Lakehouse Medallion Status")
    counts = get_medallion_counts()

    col1, col2 = st.sidebar.columns(2)
    col1.metric("Bronze Raw", f"{counts.get('bronze', 0):,}")
    col2.metric("Silver Clean", f"{counts.get('silver', 0):,}")
    st.sidebar.metric("Gold Fraud Alerts", f"{counts.get('gold', 0):,}")

    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Refresh Data", width="stretch"):
        st.rerun()

    st.sidebar.markdown("### Infrastructure Links")
    st.sidebar.markdown("- [Kafka UI (Port 8080)](http://localhost:8080)")
    st.sidebar.markdown("- [MinIO S3 Console (Port 9001)](http://localhost:9001)")
    st.sidebar.markdown("- [Iceberg REST (Port 8181)](http://localhost:8181)")


def render_tab_live_alerts():
    st.header("🚨 Real-Time Fraud Operations & Geo Velocity")
    st.caption("Consuming fast-path alerts live from Kafka topic: `fraud.alerts`")

    alerts = consume_recent_messages("fraud.alerts", max_messages=50)

    if not alerts:
        st.info("No active fraud alerts in Kafka topic 'fraud.alerts'.")
        return

    # Metrics
    total_alerts = len(alerts)
    critical_count = sum(1 for a in alerts if a.get("severity") == "CRITICAL")
    high_count = sum(1 for a in alerts if a.get("severity") == "HIGH")
    total_flagged_amount = sum(float(a.get("amount", 0.0)) for a in alerts)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Real-Time Alerts", total_alerts)
    m2.metric("Critical (Impossible Travel)", critical_count)
    m3.metric("High (Velocity Bursts)", high_count)
    m4.metric("Prevented Loss ($)", f"${total_flagged_amount:,.2f}")

    st.markdown("---")

    # Map visualization
    st.subheader("Global Anomaly Locations")
    map_points = []
    for a in alerts:
        details = a.get("details", {})
        dest_city = details.get("dest_city")
        # Approximate city coordinates for plotting
        city_coords = {
            "New York": (40.7128, -74.0060),
            "London": (51.5074, -0.1278),
            "Tokyo": (35.6762, 139.6503),
            "Paris": (48.8566, 2.3522),
            "Dubai": (25.2048, 55.2708),
            "Singapore": (1.3521, 103.8198),
            "Sydney": (-33.8688, 151.2093),
            "Berlin": (52.5200, 13.4050),
        }
        if dest_city in city_coords:
            lat, lon = city_coords[dest_city]
            map_points.append({"latitude": lat, "longitude": lon, "city": dest_city})

    if map_points:
        st.map(pd.DataFrame(map_points), zoom=1)

    # Alerts List Table
    st.subheader("Incoming Alert Stream")
    alert_rows = []
    for a in alerts:
        details = a.get("details", {})
        alert_rows.append(
            {
                "Alert ID": a.get("alert_id", "")[:8] + "...",
                "User ID": a.get("user_id"),
                "Amount ($)": f"${float(a.get('amount', 0)):,.2f}",
                "Severity": a.get("severity"),
                "Trigger Reason": a.get("trigger_reason"),
                "Detected At": str(a.get("detected_at", ""))[:19],
                "Distance (km)": details.get("distance_km", "N/A"),
                "Time Delta (s)": details.get("time_delta_seconds", "N/A"),
            }
        )
    st.dataframe(pd.DataFrame(alert_rows), width="stretch")


def render_tab_silver():
    st.header("💎 Clean Lakehouse Feed (Silver Layer)")
    st.caption("Cleaned, validated, and deduplicated records from `silver_transactions`")

    silver_df = load_table_df("silver_transactions", limit=200)

    if silver_df.empty:
        st.info("No records in `silver_transactions` yet.")
        return

    # Metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ingested Clean Records", f"{len(silver_df):,}")
    c2.metric("Total Clean Volume ($)", f"${silver_df['amount'].sum():,.2f}")
    c3.metric("Active Users", silver_df["user_id"].nunique())
    c4.metric("Active Merchants", silver_df["merchant_id"].nunique())

    st.markdown("---")

    # Filters
    f1, f2 = st.columns(2)
    categories = ["All"] + sorted(silver_df["merchant_category"].dropna().unique().tolist())
    selected_cat = f1.selectbox("Filter by Merchant Category", categories)

    search_user = f2.text_input("Search by User ID (e.g. usr_1001)")

    filtered_df = silver_df
    if selected_cat != "All":
        filtered_df = filtered_df[filtered_df["merchant_category"] == selected_cat]
    if search_user:
        filtered_df = filtered_df[
            filtered_df["user_id"].str.contains(search_user, case=False, na=False)
        ]

    st.dataframe(
        filtered_df[
            [
                "transaction_id",
                "user_id",
                "card_token",
                "amount",
                "currency",
                "merchant_category",
                "city",
                "country",
                "event_time",
            ]
        ],
        width="stretch",
    )


def render_tab_gold_audit():
    st.header("📊 Fraud Intelligence & Audit Trail (Gold Layer)")
    st.caption("Compliance audit store and historical anomalies from `gold_fraud_alerts`")

    gold_df = load_table_df("gold_fraud_alerts", limit=500)

    if gold_df.empty:
        st.info("No records in `gold_fraud_alerts` yet.")
        return

    # KPIs
    g1, g2, g3 = st.columns(3)
    g1.metric("Confirmed Fraud Incidents", len(gold_df))
    g2.metric("Total Prevented Loss", f"${gold_df['amount'].sum():,.2f}")
    avg_loss = gold_df["amount"].mean() if len(gold_df) > 0 else 0
    g3.metric("Average Fraud Amount", f"${avg_loss:,.2f}")

    st.markdown("---")

    col_chart1, col_chart2 = st.columns(2)
    with col_chart1:
        st.subheader("Anomalies by Origin City")
        if "origin_city" in gold_df.columns:
            city_counts = gold_df["origin_city"].value_counts()
            st.bar_chart(city_counts)

    with col_chart2:
        st.subheader("Severity Breakdown")
        if "severity" in gold_df.columns:
            sev_counts = gold_df["severity"].value_counts()
            st.bar_chart(sev_counts)

    st.subheader("Full Compliance Audit Log")
    st.dataframe(gold_df, width="stretch")


def render_tab_internals_and_dlq():
    st.header("⚙️ Streaming Telemetry, Iceberg Commits & DLQ")
    st.caption("Lakehouse ACID commit log and Kafka Dead-Letter Queue inspection")

    tab_iceberg, tab_dlq = st.tabs(["🧊 Iceberg Snapshot History", "💀 Dead-Letter Queue (DLQ)"])

    with tab_iceberg:
        st.subheader("ACID Commit History by Table")
        selected_tbl = st.selectbox(
            "Select Iceberg Table",
            ["silver_transactions", "bronze_transactions", "gold_fraud_alerts"],
        )
        snapshots = get_table_snapshots(selected_tbl)
        if snapshots:
            st.dataframe(pd.DataFrame(snapshots), width="stretch")
        else:
            st.info(f"No snapshots found for table '{selected_tbl}'.")

    with tab_dlq:
        st.subheader("Quarantined Poison Pills (`transactions.dlq`)")
        st.caption("Corrupted JSON, negative amounts, or malformed schemas filtered by Job 1")

        dlq_records = consume_recent_messages("transactions.dlq", max_messages=50)
        if dlq_records:
            st.warning(f"Found {len(dlq_records)} quarantined records in `transactions.dlq`.")
            st.dataframe(pd.DataFrame(dlq_records), width="stretch")
        else:
            st.success("No corrupted records found in DLQ.")


def main():
    render_sidebar()

    t1, t2, t3, t4 = st.tabs(
        [
            "🚨 Live Fraud Ops & Map",
            "💎 Clean Silver Feed",
            "📊 Fraud Intelligence & Audit",
            "⚙️ Lakehouse Commits & DLQ",
        ]
    )

    with t1:
        render_tab_live_alerts()
    with t2:
        render_tab_silver()
    with t3:
        render_tab_gold_audit()
    with t4:
        render_tab_internals_and_dlq()


if __name__ == "__main__":
    main()
