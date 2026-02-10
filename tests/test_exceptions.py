"""Tests for custom exception hierarchy."""

from data_gen.exceptions import (
    ConfigurationError,
    DataGenError,
    EntityNotFoundError,
    InvalidEntityStateError,
    ReferentialIntegrityError,
    SinkError,
)


class TestExceptionHierarchy:
    """Test exception inheritance chain."""

    def test_data_gen_error_is_exception(self) -> None:
        assert isinstance(DataGenError("test"), Exception)

    def test_entity_not_found_is_data_gen_error(self) -> None:
        assert isinstance(EntityNotFoundError("test"), DataGenError)

    def test_referential_integrity_is_entity_not_found(self) -> None:
        err = ReferentialIntegrityError("test")
        assert isinstance(err, EntityNotFoundError)
        assert isinstance(err, DataGenError)

    def test_invalid_entity_state_is_data_gen_error(self) -> None:
        assert isinstance(InvalidEntityStateError("test"), DataGenError)

    def test_configuration_error_is_data_gen_error(self) -> None:
        assert isinstance(ConfigurationError("test"), DataGenError)

    def test_sink_error_is_data_gen_error(self) -> None:
        assert isinstance(SinkError("test"), DataGenError)

    def test_exception_message(self) -> None:
        err = ReferentialIntegrityError("Customer cust-001 not found")
        assert str(err) == "Customer cust-001 not found"
