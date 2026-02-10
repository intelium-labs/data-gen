"""Shared serialization utilities for sinks."""

from dataclasses import asdict, fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any


def to_dict(obj: Any) -> dict:
    """Convert object to dictionary."""
    if is_dataclass(obj):
        return dataclass_to_dict(obj)
    elif isinstance(obj, dict):
        return obj
    else:
        return {"value": str(obj)}


def dataclass_to_dict(obj: Any) -> dict:
    """Convert dataclass to dict with proper serialization."""
    result = {}
    for key, value in asdict(obj).items():
        result[key] = serialize_value(value)
    return result


def to_dict_fast(obj: Any) -> dict:
    """Convert dataclass without deep copy (2-3x faster than asdict).

    Uses ``dataclasses.fields()`` + ``getattr`` instead of ``asdict()``
    which does a recursive deep-copy of every value.  Best for flat
    dataclasses (no nested dataclass fields like Address).

    Parameters
    ----------
    obj : Any
        A dataclass instance.

    Returns
    -------
    dict
        Serialized dictionary.
    """
    return {f.name: serialize_value(getattr(obj, f.name)) for f in fields(obj)}


def serialize_value(value: Any) -> Any:
    """Serialize a value for JSON output."""
    if isinstance(value, Decimal):
        return str(value)
    elif isinstance(value, Enum):
        return value.value
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, dict):
        return {k: serialize_value(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [serialize_value(v) for v in value]
    return value
