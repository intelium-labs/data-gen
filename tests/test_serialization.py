"""Tests for shared serialization utilities."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum

from data_gen.sinks.serialization import dataclass_to_dict, serialize_value, to_dict, to_dict_fast


class _SampleEnum(str, Enum):
    VALUE_A = "VALUE_A"
    VALUE_B = "VALUE_B"


@dataclass
class _SampleData:
    name: str
    amount: Decimal
    created_at: datetime


class TestToDict:
    """Tests for to_dict function."""

    def test_dataclass(self) -> None:
        obj = _SampleData(name="test", amount=Decimal("100.50"), created_at=datetime(2024, 1, 1))
        result = to_dict(obj)
        assert result["name"] == "test"
        assert result["amount"] == "100.50"
        assert result["created_at"] == "2024-01-01T00:00:00"

    def test_dict_passthrough(self) -> None:
        d = {"key": "value"}
        assert to_dict(d) == d

    def test_other_type(self) -> None:
        result = to_dict(42)
        assert result == {"value": "42"}


class TestSerializeValue:
    """Tests for serialize_value function."""

    def test_decimal(self) -> None:
        assert serialize_value(Decimal("99.99")) == "99.99"

    def test_datetime(self) -> None:
        dt = datetime(2024, 6, 15, 10, 30, 0)
        assert serialize_value(dt) == "2024-06-15T10:30:00"

    def test_date(self) -> None:
        d = date(2024, 6, 15)
        assert serialize_value(d) == "2024-06-15"

    def test_nested_dict(self) -> None:
        data = {"amount": Decimal("50.00"), "info": {"date": datetime(2024, 1, 1)}}
        result = serialize_value(data)
        assert result["amount"] == "50.00"
        assert result["info"]["date"] == "2024-01-01T00:00:00"

    def test_list(self) -> None:
        data = [Decimal("10.00"), Decimal("20.00")]
        result = serialize_value(data)
        assert result == ["10.00", "20.00"]

    def test_string_passthrough(self) -> None:
        assert serialize_value("hello") == "hello"

    def test_int_passthrough(self) -> None:
        assert serialize_value(42) == 42

    def test_none_passthrough(self) -> None:
        assert serialize_value(None) is None

    def test_enum(self) -> None:
        assert serialize_value(_SampleEnum.VALUE_A) == "VALUE_A"


class TestDataclassToDict:
    """Tests for dataclass_to_dict function."""

    def test_converts_all_fields(self) -> None:
        obj = _SampleData(name="test", amount=Decimal("100.00"), created_at=datetime(2024, 1, 1))
        result = dataclass_to_dict(obj)
        assert set(result.keys()) == {"name", "amount", "created_at"}
        assert isinstance(result["amount"], str)
        assert isinstance(result["created_at"], str)


class TestToDictFast:
    """Tests for to_dict_fast function."""

    def test_flat_dataclass(self) -> None:
        """to_dict_fast serializes a flat dataclass correctly."""
        obj = _SampleData(name="test", amount=Decimal("100.50"), created_at=datetime(2024, 1, 1))
        result = to_dict_fast(obj)
        assert result["name"] == "test"
        assert result["amount"] == "100.50"
        assert result["created_at"] == "2024-01-01T00:00:00"

    def test_matches_to_dict(self) -> None:
        """to_dict_fast produces same output as to_dict for flat dataclasses."""
        obj = _SampleData(name="hello", amount=Decimal("42.00"), created_at=datetime(2024, 6, 15))
        assert to_dict_fast(obj) == to_dict(obj)

    def test_enum_field(self) -> None:
        """to_dict_fast serializes enum values."""

        @dataclass
        class _WithEnum:
            status: _SampleEnum
            value: int

        obj = _WithEnum(status=_SampleEnum.VALUE_A, value=10)
        result = to_dict_fast(obj)
        assert result["status"] == "VALUE_A"
        assert result["value"] == 10

    def test_date_field(self) -> None:
        """to_dict_fast serializes date fields."""

        @dataclass
        class _WithDate:
            due_date: date

        obj = _WithDate(due_date=date(2024, 3, 15))
        result = to_dict_fast(obj)
        assert result["due_date"] == "2024-03-15"
