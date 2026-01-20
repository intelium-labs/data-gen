"""Credit card models for financial domain."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class CreditCard:
    """Credit card entity."""

    card_id: str
    customer_id: str
    card_number_masked: str  # ****-****-****-1234
    brand: str  # VISA, MASTERCARD, ELO
    credit_limit: Decimal
    available_limit: Decimal
    due_day: int  # 1-28
    status: str  # ACTIVE, BLOCKED, CANCELLED
    created_at: datetime


@dataclass
class CardTransaction:
    """Credit card transaction."""

    transaction_id: str
    card_id: str
    merchant_name: str
    merchant_category: str  # MCC description
    mcc_code: str  # Merchant Category Code (4 digits)
    amount: Decimal
    installments: int  # 1 for à vista
    timestamp: datetime
    status: str  # PENDING, APPROVED, DECLINED
    location_city: str | None = None
    location_country: str = "BR"
