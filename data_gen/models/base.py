"""Base models shared across domains."""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Address:
    """Physical address supporting multiple countries.

    Fields are general enough to represent addresses worldwide:
    - street/number: street address
    - neighborhood: bairro (BR), district, borough, etc.
    - state: state/province/county abbreviation or name
    - postal_code: ZIP/CEP/postcode in country-specific format
    - country: ISO 3166-1 alpha-2 code (default: ``"BR"``)
    """

    street: str
    number: str
    neighborhood: str
    city: str
    state: str
    postal_code: str
    complement: str = ""
    country: str = "BR"


@dataclass
class Event:
    """Standard event envelope for streaming."""

    event_id: str
    event_type: str  # entity.action (e.g., transaction.created)
    event_time: datetime
    source: str  # Service/system that generated
    subject: str  # Entity ID affected
    data: dict
    metadata: dict = field(default_factory=dict)
