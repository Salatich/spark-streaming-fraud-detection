"""One-click end-to-end sandbox runner for evaluation and live demos.

Steps:
1. Emits synthetic financial transactions with chaos (impossible travel, duplicates, poison pills).
2. Runs Job 1 (Bronze Ingestion, Silver Cleansing, and DLQ routing).
3. Runs Job 2 (Iceberg readStream, Stateful Fraud Detection, and Dual-Sink Egress).
4. Prints a summary of Kafka topics and Iceberg lakehouse tables.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("DemoRunner")


def run_command(cmd: list[str], description: str) -> bool:
    logger.info(f"==> {description}...")
    env = os.environ.copy()
    src_dir = os.path.abspath("src")
    existing_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{src_dir}{os.pathsep}{existing_pp}" if existing_pp else src_dir
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        logger.error(f"Failed: {description}\n{result.stderr or result.stdout}")
        return False
    logger.info(f"✓ {description} succeeded.")
    return True


def main():
    logger.info("=" * 70)
    logger.info("STARTING SPARK STREAMING & ICEBERG DEMO PIPELINE")
    logger.info("=" * 70)

    # 1. Emit Transactions with Chaos
    logger.info("\n[Stage 1] Generating Streaming Transactions with Chaos...")
    cmd_gen = [
        sys.executable,
        "src/generator/producer.py",
        "--rate",
        "15",
        "--duration",
        "4",
    ]
    if not run_command(cmd_gen, "Emit chaos transactions to Kafka 'transactions.raw'"):
        return

    time.sleep(1)

    # 2. Run Job 1 (Bronze, Silver, DLQ)
    logger.info("\n[Stage 2] Running Spark Job 1 (Ingestion & Cleansing)...")
    cmd_j1 = [
        "docker",
        "exec",
        "spark-streaming-engine",
        "python3",
        "src/jobs/job_bronze_silver.py",
        "--once",
    ]
    if not run_command(cmd_j1, "Spark Job 1: Ingest Bronze & Cleanse Silver"):
        return

    time.sleep(1)

    # 3. Run Job 2 (Gold Fraud Detection)
    logger.info("\n[Stage 3] Running Spark Job 2 (Stateful Fraud Detection)...")
    cmd_j2 = [
        "docker",
        "exec",
        "spark-streaming-engine",
        "python3",
        "src/jobs/job_gold_fraud_detector.py",
        "--once",
    ]
    if not run_command(cmd_j2, "Spark Job 2: Iceberg readStream & Geo-Velocity Engine"):
        return

    logger.info("\n" + "=" * 70)
    logger.info("DEMO PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info("=" * 70)
    logger.info("To launch the interactive 4-tab Streamlit dashboard:")
    logger.info("  uv run streamlit run src/ui/app.py\n")


if __name__ == "__main__":
    main()
