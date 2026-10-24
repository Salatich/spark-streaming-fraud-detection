"""Infrastructure connectivity and sanity check script."""

import json
import sys
import time
import urllib.request
from confluent_kafka.admin import AdminClient, NewTopic


def check_kafka(bootstrap_servers: str = "localhost:29092") -> bool:
    print(f"[*] Checking Kafka connectivity on {bootstrap_servers}...")
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin.list_topics(timeout=10.0)
        print(f"    [+] Kafka is accessible! Available topics: {list(metadata.topics.keys())}")

        # Ensure topics exist
        required_topics = ["transactions.raw", "fraud.alerts", "transactions.dlq"]
        existing = set(metadata.topics.keys())
        to_create = [
            NewTopic(topic, num_partitions=3, replication_factor=1)
            for topic in required_topics
            if topic not in existing
        ]

        if to_create:
            print(f"    [*] Creating missing topics: {[t.topic for t in to_create]}...")
            fs = admin.create_topics(to_create)
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"    [+] Created topic '{topic}' successfully.")
                except Exception as e:
                    print(f"    [!] Topic '{topic}' error: {e}")
        return True
    except Exception as e:
        print(f"    [x] Kafka check failed: {e}")
        return False


def check_minio(endpoint: str = "http://localhost:9000") -> bool:
    print(f"[*] Checking MinIO connectivity on {endpoint}...")
    try:
        req = urllib.request.Request(f"{endpoint}/minio/health/live")
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                print("    [+] MinIO is online and healthy!")
                return True
    except Exception as e:
        print(f"    [x] MinIO check failed: {e}")
    return False


def check_iceberg_rest(catalog_uri: str = "http://localhost:8181") -> bool:
    print(f"[*] Checking Iceberg REST Catalog on {catalog_uri}...")
    try:
        req = urllib.request.Request(f"{catalog_uri}/v1/config")
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())
                print(f"    [+] Iceberg REST catalog is online! Config: {data}")
                return True
    except Exception as e:
        print(f"    [x] Iceberg REST catalog check failed: {e}")
    return False


def main():
    print("=" * 60)
    print("      INFRASTRUCTURE SANITY & HEALTH CHECK")
    print("=" * 60)
    
    kafka_ok = check_kafka()
    minio_ok = check_minio()
    iceberg_ok = check_iceberg_rest()

    print("=" * 60)
    if kafka_ok and minio_ok and iceberg_ok:
        print("[SUCCESS] ALL INFRASTRUCTURE SERVICES ARE UP AND READY!")
        sys.exit(0)
    else:
        print("[ERROR] Some services failed health checks. Please check docker logs.")
        sys.exit(1)


if __name__ == "__main__":
    main()
