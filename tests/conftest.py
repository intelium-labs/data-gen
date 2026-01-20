"""Pytest configuration and fixtures."""

import pytest


@pytest.fixture
def seed() -> int:
    """Fixed seed for reproducible tests."""
    return 42


@pytest.fixture
def sample_customer_id() -> str:
    """Sample customer ID."""
    return "cust-test-001"


@pytest.fixture
def sample_account_id() -> str:
    """Sample account ID."""
    return "acct-test-001"


@pytest.fixture
def sample_card_id() -> str:
    """Sample card ID."""
    return "card-test-001"


@pytest.fixture
def sample_loan_id() -> str:
    """Sample loan ID."""
    return "loan-test-001"
