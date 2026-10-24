# Real-Time Fraud & Anomaly Detection with Spark Structured Streaming & Apache Iceberg

[![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-Structured%20Streaming-orange.svg)](https://spark.apache.org/)
[![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-Lakehouse-blue.svg)](https://iceberg.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-black.svg)](https://kafka.apache.org/)
[![uv](https://img.shields.io/badge/Package%20Manager-uv-purple.svg)](https://github.com/astral-sh/uv)

A production-grade streaming platform designed to demonstrate advanced **Spark Structured Streaming**, **Apache Iceberg**, and **Medallion Lakehouse Architecture**.

---

## 🎯 Architecture & Lead-Level Concepts

This repository addresses core streaming design challenges in modern enterprise data platforms:

1. **Dual-Path Streaming Pipeline**:
   - **Fast Operational Path**: Millisecond-level fraud alerts routed to Kafka topics (`fraud.alerts`) for automated reactions (e.g. card freeze, SMS notifications).
   - **Analytical Lakehouse Path**: ACID commits into Apache Iceberg (`bronze_transactions`, `silver_transactions`, `gold_fraud_alerts`) on local S3-compatible object storage (MinIO).
2. **Event-Time Processing & Watermarking**:
   - Watermark policies (`withWatermark`) to process out-of-order financial events and drop expired state safely.
3. **Streaming Deduplication**:
   - Idempotent processing and deduplication over network-retried transaction IDs within watermarked windows.
4. **Stateful Stream Processing**:
   - Arbitrary stateful operations to detect geo-velocity breaches (impossible travel) and velocity bursts across sliding windows.
5. **Data Quality & Dead-Letter Queue (DLQ)**:
   - Quarantine corrupted/malformed payloads to protect downstream tables from schema drift.
6. **Observability & Backpressure Control**:
   - Micro-batch rate limiting with `maxOffsetsPerTrigger`.
   - Streaming engine telemetry via `StreamingQueryListener` and interactive **Streamlit** dashboard.

---

## 🏗️ System Architecture

```
[ Chaos Transaction Generator ]
              │ (Configurable EPS, Out-of-order, Injected Fraud)
              ▼
    [ Kafka: transactions.raw ]
              │
    ┌─────────┴─────────────────────────────────────────┐
    │ Spark Streaming Job 1: Ingestion & Cleansing       │
    │  ├─ Bronze Ingest: Raw JSON -> Iceberg Bronze     │
    │  └─ Silver Cleanse: Watermark + Dedup -> Silver   │
    │     (Corrupted records -> Quarantine DLQ)         │
    └─────────┬─────────────────────────────────────────┘
              │
    ┌─────────┴─────────────────────────────────────────┐
    │ Spark Streaming Job 2: Stateful Fraud Detector    │
    │  ├─ Geo-Velocity Breach Detection                 │
    │  ├─ Rapid Spending Burst Detection                │
    │  ├─ Emit Alerts -> Kafka: fraud.alerts (Fast Path)│
    │  └─ Persist -> Iceberg: gold_fraud_alerts         │
    └─────────┬──────────────────────────────┬──────────┘
              ▼                              ▼
    [ Kafka: fraud.alerts ]        [ Iceberg Lakehouse ]
              │                              │
              └──────────────┬───────────────┘
                             ▼
              [ Streamlit Real-Time Dashboard ]
                • Tab 1: Live Fraud Alerts & Geo Map
                • Tab 2: Clean Silver Transactions Feed
                • Tab 3: Streaming Health & Snapshot Explorer
```

---

## 📦 Project Structure

```
├── .github/                 # CI/CD Workflows
├── docker/                  # Docker Compose (Kafka, Spark, Iceberg REST, MinIO)
├── src/
│   └── spark_streaming/
│       ├── generator/       # High-fidelity chaos transaction producer
│       ├── jobs/            # Spark Structured Streaming jobs (Bronze, Silver, Gold)
│       ├── lakehouse/       # Iceberg table schemas and maintenance utils
│       ├── models/          # Pydantic schemas and event definitions
│       └── ui/              # Streamlit live operational dashboard
├── tests/                   # Unit & Streaming integration tests
├── pyproject.toml           # Project dependencies managed by uv
├── uv.lock                  # Pinned deterministic lockfile
└── README.md
```

---

## 🚀 Quickstart

### Prerequisites
- [uv](https://github.com/astral-sh/uv) (Fast Python package manager)
- [Docker & Docker Compose](https://www.docker.com/)

### 1. Environment Setup
```bash
# Clone the repository
git clone <repo-url>
cd spark-streaming

# Install dependencies via uv
uv sync
```

### 2. Launch Local Streaming Infrastructure (Docker)
```bash
docker compose up -d
```

### 3. Run Producer & Streaming Jobs
```bash
# Start transaction generator
uv run python -m spark_streaming.generator.producer --rate 20

# Run Streamlit Live Dashboard
uv run streamlit run src/spark_streaming/ui/app.py
```
