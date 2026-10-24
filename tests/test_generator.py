"""Unit tests for geographic utilities and chaos generator logic."""

import json

from generator.geo_utils import haversine_distance_km
from generator.producer import ChaosTransactionGenerator


def test_haversine_distance():
    # NYC (40.7128, -74.0060) to London (51.5074, -0.1278) is approx ~5570 km
    dist = haversine_distance_km(40.7128, -74.0060, 51.5074, -0.1278)
    assert 5500 < dist < 5650

    # Distance to same point should be 0
    assert haversine_distance_km(40.7128, -74.0060, 40.7128, -74.0060) == 0.0


def test_generator_payload_structure():
    # Instantiate generator with 0 chaos probabilities to test clean path
    gen = ChaosTransactionGenerator(
        bootstrap_servers="localhost:29092",
        late_prob=0.0,
        dup_prob=0.0,
        poison_prob=0.0,
        fraud_prob=0.0,
    )
    raw_payload = gen.generate_single_event()
    data = json.loads(raw_payload)

    assert "transaction_id" in data
    assert "user_id" in data
    assert "amount" in data
    assert data["amount"] > 0
    assert "event_time" in data
    assert "location" in data
    assert "latitude" in data["location"]


def test_generator_poison_pill_injection():
    # Force 100% poison pill injection
    gen = ChaosTransactionGenerator(
        bootstrap_servers="localhost:29092",
        poison_prob=1.0,
    )
    raw_payload = gen.generate_single_event()
    data = json.loads(raw_payload)

    # Must either be corrupted or have invalid user/negative amount
    is_corrupted = (
        data.get("corrupted_data") is True
        or data.get("amount", 0) < 0
        or data.get("user_id") is None
    )
    assert is_corrupted is True
