"""Pydantic data models for streaming transactions, fraud alerts, and DLQ records."""

from datetime import datetime, timezone

from pydantic import BaseModel, Field


class GeoLocation(BaseModel):
    """Geographic location coordinates for velocity calculations."""

    city: str
    country: str
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)


class TransactionEvent(BaseModel):
    """Core transaction event model representing raw payment operations.

    Attributes:
        transaction_id: Unique UUID identifier used for streaming deduplication.
        user_id: Customer identifier for stateful grouping and velocity profiling.
        card_token: Masked tokenized card identifier (PCI-DSS compliant).
        amount: Monetary transaction value.
        currency: ISO 4217 3-letter currency code (e.g., USD, EUR).
        merchant_id: Merchant identifier.
        merchant_category: Industry code (e.g., travel, electronics, grocery, luxury).
        location: GPS coordinates and city of origin (for impossible travel detection).
        device_id: Unique device identifier.
        event_time: UTC timestamp when transaction took place (used for Spark watermarking).
        is_fraud_injected: Metadata flag indicating synthetic fraud injection for testing.
    """

    transaction_id: str
    user_id: str
    card_token: str
    amount: float = Field(..., gt=0.0)
    currency: str = "USD"
    merchant_id: str
    merchant_category: str
    location: GeoLocation
    device_id: str
    event_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    is_fraud_injected: bool = False

    def to_json(self) -> str:
        """Serialize event to ISO-8601 UTC JSON string."""
        return self.model_dump_json()


class FraudAlert(BaseModel):
    """Real-time alert emitted to Kafka 'fraud.alerts' and Gold Iceberg table."""

    alert_id: str
    transaction_id: str
    user_id: str
    card_token: str
    amount: float
    trigger_reason: str
    severity: str = "HIGH"  # CRITICAL, HIGH, MEDIUM
    detected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    details: dict = Field(default_factory=dict)

    def to_json(self) -> str:
        return self.model_dump_json()


class DLQRecord(BaseModel):
    """Dead-Letter Queue record for quarantine of poisoned or corrupted payloads."""

    raw_payload: str
    error_message: str
    quarantined_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source_topic: str = "transactions.raw"
