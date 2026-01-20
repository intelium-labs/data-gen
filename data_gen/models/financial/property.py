"""Property model for housing finance."""

from dataclasses import dataclass
from decimal import Decimal

from data_gen.models.base import Address


@dataclass
class Property:
    """Real estate property for housing finance."""

    property_id: str
    property_type: str  # APARTMENT, HOUSE, LAND
    address: Address
    appraised_value: Decimal
    area_sqm: float  # Square meters
    registration_number: str  # Matrícula do imóvel
