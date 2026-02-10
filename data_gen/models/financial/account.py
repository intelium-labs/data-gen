"""Account model for financial domain."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from data_gen.models.financial.enums import AccountStatus, AccountType


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
    account_type: AccountType
    bank_code: str  # 001, 033, 341, 237
    branch: str  # 4 digits
    account_number: str  # 6-8 digits + check digit
    balance: Decimal
    status: AccountStatus
    created_at: datetime
    updated_at: datetime | None = None
    incremental_id: int = 0  # Sequential ID for incremental processing
