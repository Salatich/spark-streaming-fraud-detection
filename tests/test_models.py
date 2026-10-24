"""Unit tests for transaction and alert models."""

import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from models.transaction import DLQRecord, FraudAlert, GeoLocation, TransactionEvent


def test_valid_transaction_event():
    location = GeoLocation(city="New York", country="USA", latitude=40.7128, longitude=-74.0060)
    tx = TransactionEvent(
        transaction_id="tx_123",
        user_id="usr_001",
        card_token="tok_abc123",
        amount=150.50,
        currency="USD",
        merchant_id="merch_55",
        merchant_category="electronics",
        location=location,
        device_id="dev_xyz",
        event_time=datetime.now(timezone.utc),
    )

    data = json.loads(tx.to_json())
    assert data["transaction_id"] == "tx_123"
    assert data["amount"] == 150.50
    assert data["location"]["city"] == "New York"
    assert data["is_fraud_injected"] is False


def test_invalid_negative_amount():
    location = GeoLocation(city="London", country="UK", latitude=51.5074, longitude=-0.1278)
    with pytest.raises(ValidationError):
        TransactionEvent(
            transaction_id="tx_invalid",
            user_id="usr_002",
            card_token="tok_def456",
            amount=-25.00,  # Negative amounts are strictly disallowed
            merchant_id="merch_10",
            merchant_category="travel",
            location=location,
            device_id="dev_123",
        )


def test_fraud_alert_model():
    alert = FraudAlert(
        alert_id="alt_999",
        transaction_id="tx_123",
        user_id="usr_001",
        card_token="tok_abc123",
        amount=5000.00,
        trigger_reason="Impossible travel detected (>5000 km in 5 mins)",
        severity="CRITICAL",
        details={"distance_km": 5570, "time_delta_seconds": 300},
    )

    data = json.loads(alert.to_json())
    assert data["severity"] == "CRITICAL"
    assert data["details"]["distance_km"] == 5570


def test_dlq_record_model():
    dlq = DLQRecord(
        raw_payload="MALFORMED_JSON_STRING",
        error_message="JSONDecodeError: Expecting value",
    )
    data = json.loads(dlq.model_dump_json())
    assert data["raw_payload"] == "MALFORMED_JSON_STRING"
    assert data["source_topic"] == "transactions.raw"
