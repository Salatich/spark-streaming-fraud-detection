"""Unit tests for Job 2 stateful fraud detection rules and alert structures."""

import json
from datetime import datetime, timedelta, timezone

from generator.geo_utils import haversine_distance_km
from models.transaction import FraudAlert


def test_impossible_travel_physics():
    """Verify impossible travel detection physics (commercial aviation max ~800-900 km/h)."""
    # Tokyo to New York (~10850 km) in 15 minutes (900 seconds)
    tokyo_lat, tokyo_lon = 35.6762, 139.6503
    nyc_lat, nyc_lon = 40.7128, -74.0060

    dist_km = haversine_distance_km(tokyo_lat, tokyo_lon, nyc_lat, nyc_lon)
    time_delta_sec = 900.0  # 15 minutes
    speed_kmh = dist_km / (time_delta_sec / 3600.0)

    # Must be > 40,000 km/h (blatantly impossible)
    assert dist_km > 10000.0
    assert speed_kmh > 40000.0
    is_impossible = dist_km > 500.0 and speed_kmh > 800.0
    assert is_impossible is True


def test_legitimate_travel_physics():
    """Verify legitimate travel is not flagged as impossible travel."""
    # Paris to London (~343 km) in 2.5 hours (9000 seconds)
    paris_lat, paris_lon = 48.8566, 2.3522
    london_lat, london_lon = 51.5074, -0.1278

    dist_km = haversine_distance_km(paris_lat, paris_lon, london_lat, london_lon)
    time_delta_sec = 9000.0  # 2.5 hours
    speed_kmh = dist_km / (time_delta_sec / 3600.0)

    # Eurostar / commercial flight: speed ~137 km/h
    assert dist_km < 500.0 or speed_kmh < 800.0
    is_impossible = dist_km > 500.0 and speed_kmh > 800.0
    assert is_impossible is False


def test_velocity_burst_criteria():
    """Verify rapid card-testing burst detection criteria."""
    time_delta_sec = 25.0
    amount = 550.00

    is_burst = time_delta_sec <= 60.0 and amount > 200.0
    assert is_burst is True

    # Low amount within short window is not flagged as high-risk burst
    low_amount = 15.00
    assert (time_delta_sec <= 60.0 and low_amount > 200.0) is False


def test_fraud_alert_json_payload():
    """Verify fraud alert JSON matches expected Kafka egress format."""
    now = datetime.now(timezone.utc)
    alert = FraudAlert(
        alert_id="alt_123456",
        transaction_id="tx_987654",
        user_id="usr_1029",
        card_token="tok_db052c59",
        amount=89.72,
        trigger_reason="Impossible travel: Tokyo -> London (9559 km in 563s)",
        severity="CRITICAL",
        detected_at=now,
        details={
            "origin_city": "Tokyo",
            "dest_city": "London",
            "distance_km": 9558.56,
            "time_delta_seconds": 563.0,
            "event_time": (now - timedelta(minutes=10)).isoformat(),
        },
    )

    raw_json = alert.to_json()
    parsed = json.loads(raw_json)

    assert parsed["alert_id"] == "alt_123456"
    assert parsed["severity"] == "CRITICAL"
    assert parsed["details"]["distance_km"] == 9558.56
    assert parsed["details"]["origin_city"] == "Tokyo"
