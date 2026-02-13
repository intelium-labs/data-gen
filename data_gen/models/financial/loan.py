"""Loan models for financial domain."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from data_gen.models.financial.enums import (
    AmortizationSystem,
    InstallmentStatus,
    LoanStatus,
    LoanType,
)


@dataclass
class Loan:
    """Loan contract entity."""

    loan_id: str
    customer_id: str
    loan_type: LoanType
    principal: Decimal  # Amount borrowed
    interest_rate: Decimal  # Monthly rate (e.g., 0.015 for 1.5%)
    term_months: int
    amortization_system: AmortizationSystem
    status: LoanStatus
    disbursement_date: date | None
    property_id: str | None  # For housing finance
    created_at: datetime
    updated_at: datetime | None = None
    incremental_id: int = 0  # Sequential ID for incremental processing


@dataclass
class Installment:
    """Loan installment (parcela)."""

    installment_id: str
    loan_id: str
    customer_id: str
    installment_number: int  # 1, 2, 3, ...
    due_date: date
    principal_amount: Decimal
    interest_amount: Decimal
    total_amount: Decimal
    paid_date: date | None
    paid_amount: Decimal | None
    status: InstallmentStatus
    created_at: datetime | None = None  # Record creation timestamp
    updated_at: datetime | None = None
    incremental_id: int = 0  # Sequential ID for incremental processing
