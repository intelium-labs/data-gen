"""Customer model for financial domain."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from data_gen.models.base import Address
from data_gen.models.financial.enums import EmploymentStatus


@dataclass
class Customer:
    """Bank customer entity."""

    customer_id: str
    cpf: str
    name: str
    email: str
    phone: str
    address: Address
    monthly_income: Decimal
    employment_status: EmploymentStatus
    credit_score: int  # 300-850 (like Serasa)
    created_at: datetime
    updated_at: datetime | None = None
    incremental_id: int = 0  # Sequential ID for incremental processing
