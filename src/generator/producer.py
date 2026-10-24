"""High-fidelity streaming transaction generator with configurable chaos injection."""

import argparse
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

from confluent_kafka import Producer
from faker import Faker

from generator.geo_utils import GLOBAL_CITIES, haversine_distance_km
from models.transaction import GeoLocation, TransactionEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TransactionProducer")

fake = Faker()

MERCHANT_CATEGORIES = [
    "groceries",
    "electronics",
    "travel",
    "restaurants",
    "luxury_goods",
    "entertainment",
    "utilities",
    "health_beauty",
]


class CustomerProfile:
    """Simulated legitimate customer persona."""

    def __init__(self, user_id: str, home_city: GeoLocation):
        self.user_id = user_id
        self.card_token = f"tok_{uuid.uuid4().hex[:16]}"
        self.home_city = home_city
        self.device_id = f"dev_{uuid.uuid4().hex[:12]}"
        self.base_avg_amount = round(random.uniform(20.0, 120.0), 2)


class ChaosTransactionGenerator:
    """Generates continuous streams of financial transactions with controllable chaos.

    Chaos Scenarios:
      1. Out-of-order events: Timestamps backdated by 3 to 15 minutes (tests watermarking).
      2. Duplicate events: Resending identical transaction_ids (tests streaming deduplication).
      3. Poison pills: Corrupted JSON or invalid schemas (tests Dead-Letter Queue).
      4. Impossible travel: High geographic displacement in short time (tests stateful velocity).
      5. Velocity bursts: Multiple rapid transactions within seconds (tests windowing).
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:29092",
        topic: str = "transactions.raw",
        late_prob: float = 0.05,
        dup_prob: float = 0.03,
        poison_prob: float = 0.02,
        fraud_prob: float = 0.05,
        num_customers: int = 50,
    ):
        self.topic = topic
        self.late_prob = late_prob
        self.dup_prob = dup_prob
        self.poison_prob = poison_prob
        self.fraud_prob = fraud_prob

        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "transaction-chaos-generator",
                "compression.type": "snappy",
                "linger.ms": 5,
                "batch.num.messages": 100,
            }
        )

        # Generate persistent customer personas
        self.customers: List[CustomerProfile] = [
            CustomerProfile(
                user_id=f"usr_{1000 + i}",
                home_city=random.choice(GLOBAL_CITIES),
            )
            for i in range(num_customers)
        ]

        # Recent transactions buffer for duplicate injection
        self.recent_tx_buffer: List[TransactionEvent] = []

    def _delivery_callback(self, err, msg):
        if err:
            logger.error(f"Message delivery failed: {err}")

    def generate_single_event(self) -> str:
        """Create a single transaction payload, occasionally injecting chaos or fraud."""
        rand = random.random()

        # 1. Chaos: Poison pill payload (malformed JSON / invalid schema)
        if rand < self.poison_prob:
            logger.warning("[CHAOS] Injecting POISON-PILL payload for DLQ testing")
            poison_type = random.choice(["bad_json", "negative_amount", "null_user"])
            if poison_type == "bad_json":
                return '{"corrupted_data": true, "raw_error": "' + uuid.uuid4().hex + '"}'
            elif poison_type == "negative_amount":
                return json.dumps(
                    {
                        "transaction_id": str(uuid.uuid4()),
                        "user_id": "usr_9999",
                        "amount": -500.00,  # Invalid negative
                        "event_time": datetime.now(timezone.utc).isoformat(),
                    }
                )
            else:
                return json.dumps(
                    {
                        "transaction_id": str(uuid.uuid4()),
                        "user_id": None,  # Missing user
                        "amount": 100.00,
                        "event_time": datetime.now(timezone.utc).isoformat(),
                    }
                )

        # 2. Chaos: Duplicate event injection
        if self.recent_tx_buffer and rand < (self.poison_prob + self.dup_prob):
            dup_event = random.choice(self.recent_tx_buffer)
            logger.warning(f"[CHAOS] Injecting DUPLICATE transaction: {dup_event.transaction_id}")
            return dup_event.to_json()

        customer = random.choice(self.customers)
        now_utc = datetime.now(timezone.utc)

        # 3. Chaos: Out-of-Order / Delayed event (Late Arrival)
        if rand < (self.poison_prob + self.dup_prob + self.late_prob):
            delay_minutes = random.randint(3, 15)
            event_time = now_utc - timedelta(minutes=delay_minutes)
            logger.info(f"[CHAOS] Injecting LATE transaction ({delay_minutes}m delay)")
        else:
            event_time = now_utc

        # 4. Targeted Fraud: Impossible Travel Breach
        is_fraud = False
        location = customer.home_city
        amount = round(random.gauss(customer.base_avg_amount, 15.0), 2)
        if amount <= 0:
            amount = 10.00

        if rand < (self.poison_prob + self.dup_prob + self.late_prob + self.fraud_prob):
            is_fraud = True
            # Pick a city far away from home
            distant_cities = [
                c
                for c in GLOBAL_CITIES
                if haversine_distance_km(
                    customer.home_city.latitude,
                    customer.home_city.longitude,
                    c.latitude,
                    c.longitude,
                )
                > 3000
            ]
            if distant_cities:
                location = random.choice(distant_cities)
                amount = round(amount * random.uniform(2.5, 6.0), 2)
                logger.warning(
                    f"[FRAUD] Injecting IMPOSSIBLE TRAVEL: {customer.user_id} "
                    f"from {customer.home_city.city} to {location.city}"
                )

        tx = TransactionEvent(
            transaction_id=str(uuid.uuid4()),
            user_id=customer.user_id,
            card_token=customer.card_token,
            amount=amount,
            currency="USD",
            merchant_id=f"merch_{random.randint(100, 999)}",
            merchant_category=random.choice(MERCHANT_CATEGORIES),
            location=location,
            device_id=customer.device_id,
            event_time=event_time,
            is_fraud_injected=is_fraud,
        )

        # Retain in buffer for possible duplicate replay
        if len(self.recent_tx_buffer) > 200:
            self.recent_tx_buffer.pop(0)
        self.recent_tx_buffer.append(tx)

        return tx.to_json()

    def run(self, events_per_second: float = 10.0, duration_seconds: int = 0):
        """Run transaction generation loop at specified rate limit."""
        interval = 1.0 / max(events_per_second, 0.1)
        duration_label = "Infinite" if duration_seconds == 0 else f"{duration_seconds}s"
        logger.info(
            f"Starting transaction generator: Rate={events_per_second} EPS, "
            f"Topic='{self.topic}', Duration={duration_label}"
        )

        start_time = time.time()
        sent_count = 0

        try:
            while True:
                payload = self.generate_single_event()
                self.producer.produce(
                    topic=self.topic,
                    value=payload.encode("utf-8"),
                    on_delivery=self._delivery_callback,
                )
                sent_count += 1

                # Poll Kafka producer events periodically
                if sent_count % 50 == 0:
                    self.producer.poll(0)
                    elapsed = time.time() - start_time
                    current_rate = sent_count / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Published {sent_count} events (Avg throughput: {current_rate:.1f} EPS)"
                    )

                # Check duration stop condition
                if duration_seconds > 0 and (time.time() - start_time) >= duration_seconds:
                    logger.info(f"Completed duration target ({duration_seconds}s). Exiting.")
                    break

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("Generator stopped by user.")
        finally:
            logger.info(f"Flushing Kafka producer ({sent_count} total messages sent)...")
            self.producer.flush(timeout=10)
            logger.info("Producer flushed cleanly.")


def main():
    parser = argparse.ArgumentParser(description="Chaos Transaction Stream Generator")
    parser.add_argument(
        "--rate",
        type=float,
        default=float(os.getenv("EVENTS_PER_SECOND", 10.0)),
        help="Events per second (EPS)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Duration in seconds (0 = infinite daemon)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=os.getenv("KAFKA_RAW_TOPIC", "transactions.raw"),
        help="Target Kafka topic",
    )

    args = parser.parse_args()

    generator = ChaosTransactionGenerator(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )
    generator.run(events_per_second=args.rate, duration_seconds=args.duration)


if __name__ == "__main__":
    main()
