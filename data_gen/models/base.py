"""Base models shared across domains."""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Address:
    """Brazilian address."""

    street: str
    number: str
    neighborhood: str
    city: str
    state: str  # 2-letter code (SP, RJ, etc.)
    postal_code: str  # CEP format: XXXXX-XXX
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
