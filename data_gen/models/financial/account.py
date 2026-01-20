"""Account model for financial domain."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class Account:
    """Bank account entity.

    Brazilian account types:
    - CONTA_CORRENTE: Checking account (most common)
    - POUPANCA: Savings account with monthly yield
    - INVESTIMENTOS: Investment account for stocks, funds, etc.
    """

    account_id: str
    customer_id: str
    account_type: str  # CONTA_CORRENTE, POUPANCA, INVESTIMENTOS
    bank_code: str  # 001, 033, 341, 237
    branch: str  # 4 digits
    account_number: str  # 6-8 digits + check digit
    balance: Decimal
    status: str  # ACTIVE, BLOCKED, CLOSED
    created_at: datetime
