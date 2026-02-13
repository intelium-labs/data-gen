"""Transaction model for financial domain."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from data_gen.models.financial.enums import (
    Direction,
    PixKeyType,
    TransactionStatus,
    TransactionType,
)


@dataclass
class Transaction:
    """Bank account transaction."""

    transaction_id: str
    account_id: str
    customer_id: str
    transaction_type: TransactionType
    amount: Decimal
    direction: Direction
    counterparty_key: str | None  # Pix key, account number, etc.
    counterparty_name: str | None
    description: str
    timestamp: datetime
    status: TransactionStatus

    # Pix-specific fields (optional)
    pix_e2e_id: str | None = None
    pix_key_type: PixKeyType | None = None

    # Location for fraud detection (optional)
    location_lat: float | None = None
    location_lon: float | None = None

    # Incremental processing fields
    created_at: datetime | None = None  # Record creation timestamp
    updated_at: datetime | None = None
    incremental_id: int = 0  # Sequential ID for incremental processing
