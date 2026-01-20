"""Loan models for financial domain."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal


@dataclass
class Loan:
    """Loan contract entity."""

    loan_id: str
    customer_id: str
    loan_type: str  # PERSONAL, HOUSING, VEHICLE
    principal: Decimal  # Amount borrowed
    interest_rate: Decimal  # Monthly rate (e.g., 0.015 for 1.5%)
    term_months: int
    amortization_system: str  # SAC, PRICE
    status: str  # PENDING, APPROVED, REJECTED, ACTIVE, PAID_OFF, DEFAULT
    disbursement_date: date | None
    property_id: str | None  # For housing finance
    created_at: datetime


@dataclass
class Installment:
    """Loan installment (parcela)."""

    installment_id: str
    loan_id: str
    installment_number: int  # 1, 2, 3, ...
    due_date: date
    principal_amount: Decimal
    interest_amount: Decimal
    total_amount: Decimal
    paid_date: date | None
    paid_amount: Decimal | None
    status: str  # PENDING, PAID, LATE, DEFAULT
