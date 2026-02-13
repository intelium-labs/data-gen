"""Credit card models for financial domain."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from data_gen.models.financial.enums import CardBrand, CardStatus, CardTransactionStatus


@dataclass
class CreditCard:
    """Credit card entity."""

    card_id: str
    customer_id: str
    card_number_masked: str  # ****-****-****-1234
    brand: CardBrand
    credit_limit: Decimal
    available_limit: Decimal
    due_day: int  # 1-28
    status: CardStatus
    created_at: datetime
    updated_at: datetime | None = None
    incremental_id: int = 0  # Sequential ID for incremental processing


@dataclass
class CardTransaction:
    """Credit card transaction."""

    transaction_id: str
    card_id: str
    customer_id: str
    merchant_name: str
    merchant_category: str  # MCC description
    mcc_code: str  # Merchant Category Code (4 digits)
    amount: Decimal
    installments: int  # 1 for a vista
    timestamp: datetime
    status: CardTransactionStatus
    location_city: str | None = None
    location_country: str = "BR"
    created_at: datetime | None = None  # Record creation timestamp
    updated_at: datetime | None = None
    incremental_id: int = 0  # Sequential ID for incremental processing
