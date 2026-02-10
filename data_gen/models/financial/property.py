"""Property model for housing finance."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from data_gen.models.base import Address
from data_gen.models.financial.enums import PropertyType


@dataclass
class Property:
    """Real estate property for housing finance."""

    property_id: str
    property_type: PropertyType
    address: Address
    appraised_value: Decimal
    area_sqm: float  # Square meters
    registration_number: str  # Matricula do imovel
    created_at: datetime | None = None  # Record creation timestamp
    updated_at: datetime | None = None
    incremental_id: int = 0  # Sequential ID for incremental processing
