"""Transaction model for financial domain."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class Transaction:
    """Bank account transaction."""

    transaction_id: str
    account_id: str
    transaction_type: str  # PIX, TED, DOC, WITHDRAW, DEPOSIT, BOLETO
    amount: Decimal
    direction: str  # CREDIT, DEBIT
    counterparty_key: str | None  # Pix key, account number, etc.
    counterparty_name: str | None
    description: str
    timestamp: datetime
    status: str  # PENDING, COMPLETED, FAILED

    # Pix-specific fields (optional)
    pix_e2e_id: str | None = None
    pix_key_type: str | None = None  # CPF, CNPJ, EMAIL, PHONE, EVP

    # Location for fraud detection (optional)
    location_lat: float | None = None
    location_lon: float | None = None

    # Incremental processing fields
    created_at: datetime | None = None  # Record creation timestamp
    incremental_id: int = 0  # Sequential ID for incremental processing
