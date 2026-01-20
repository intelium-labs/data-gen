"""Tests for data generators."""

from decimal import Decimal

import pytest

from data_gen.generators.financial import (
    AccountGenerator,
    CreditCardGenerator,
    CustomerGenerator,
    LoanGenerator,
    TransactionGenerator,
)
from data_gen.generators.financial.credit_card import CardTransactionGenerator
from data_gen.generators.financial.patterns import FraudPatternGenerator, PaymentBehavior


class TestCustomerGenerator:
    """Tests for CustomerGenerator."""

    def test_generate_customer(self, seed: int) -> None:
        """Test customer generation."""
        gen = CustomerGenerator(seed=seed)
        customer = gen.generate()

        assert customer.customer_id is not None
        assert len(customer.cpf) == 14  # XXX.XXX.XXX-XX
        assert customer.name is not None
        assert customer.email is not None
        assert customer.monthly_income > 0
        assert 300 <= customer.credit_score <= 850
        assert customer.employment_status in [
            "EMPLOYED",
            "SELF_EMPLOYED",
            "RETIRED",
            "UNEMPLOYED",
        ]

    def test_generate_multiple(self, seed: int) -> None:
        """Test generating multiple customers."""
        gen = CustomerGenerator(seed=seed)
        customers = list(gen.generate_batch(5))

        assert len(customers) == 5
        # All should have unique IDs
        ids = [c.customer_id for c in customers]
        assert len(set(ids)) == 5


class TestAccountGenerator:
    """Tests for AccountGenerator."""

    def test_generate_account(self, seed: int, sample_customer_id: str) -> None:
        """Test account generation."""
        gen = AccountGenerator(seed=seed)
        account = gen.generate(sample_customer_id)

        assert account.account_id is not None
        assert account.customer_id == sample_customer_id
        assert account.account_type in ["CONTA_CORRENTE", "POUPANCA", "INVESTIMENTOS"]
        assert account.balance >= 0
        assert account.status == "ACTIVE"


class TestTransactionGenerator:
    """Tests for TransactionGenerator."""

    def test_generate_transaction(self, seed: int, sample_account_id: str) -> None:
        """Test transaction generation."""
        gen = TransactionGenerator(seed=seed)
        transaction = gen.generate(sample_account_id)

        assert transaction.transaction_id is not None
        assert transaction.account_id == sample_account_id
        assert transaction.amount > 0
        assert transaction.direction in ["CREDIT", "DEBIT"]
        assert transaction.transaction_type in [
            "PIX",
            "TED",
            "DOC",
            "WITHDRAW",
            "DEPOSIT",
            "BOLETO",
        ]

    def test_generate_pix_transaction(self, seed: int, sample_account_id: str) -> None:
        """Test PIX transaction generation."""
        gen = TransactionGenerator(seed=seed)
        transaction = gen.generate_pix(sample_account_id)

        assert transaction.transaction_type == "PIX"
        assert transaction.pix_e2e_id is not None
        assert transaction.pix_key_type in ["CPF", "CNPJ", "EMAIL", "PHONE", "RANDOM"]


class TestCreditCardGenerator:
    """Tests for CreditCardGenerator."""

    def test_generate_credit_card(self, seed: int, sample_customer_id: str) -> None:
        """Test credit card generation."""
        gen = CreditCardGenerator(seed=seed)
        card = gen.generate(sample_customer_id)

        assert card.card_id is not None
        assert card.customer_id == sample_customer_id
        assert card.brand in ["VISA", "MASTERCARD", "ELO"]
        assert card.credit_limit >= 500
        assert card.available_limit <= card.credit_limit
        assert 1 <= card.due_day <= 28
        assert "****" in card.card_number_masked


class TestCardTransactionGenerator:
    """Tests for CardTransactionGenerator."""

    def test_generate_card_transaction(self, seed: int, sample_card_id: str) -> None:
        """Test card transaction generation."""
        gen = CardTransactionGenerator(seed=seed)
        transaction = gen.generate(sample_card_id)

        assert transaction.transaction_id is not None
        assert transaction.card_id == sample_card_id
        assert transaction.amount > 0
        assert transaction.installments >= 1
        assert transaction.mcc_code is not None


class TestLoanGenerator:
    """Tests for LoanGenerator."""

    def test_generate_personal_loan(self, seed: int, sample_customer_id: str) -> None:
        """Test personal loan generation."""
        gen = LoanGenerator(seed=seed)
        loan, installments = gen.generate_with_installments(
            customer_id=sample_customer_id,
            loan_type="PERSONAL",
        )

        assert loan.loan_id is not None
        assert loan.customer_id == sample_customer_id
        assert loan.loan_type == "PERSONAL"
        assert loan.principal > 0
        assert loan.interest_rate > 0
        assert loan.term_months > 0
        assert len(installments) == loan.term_months

    def test_generate_housing_loan(self, seed: int, sample_customer_id: str) -> None:
        """Test housing loan generation."""
        gen = LoanGenerator(seed=seed)
        loan, installments = gen.generate_with_installments(
            customer_id=sample_customer_id,
            loan_type="HOUSING",
            property_id="prop-001",
        )

        assert loan.loan_type == "HOUSING"
        assert loan.property_id == "prop-001"
        assert loan.term_months >= 120  # Housing loans are longer

    def test_installment_totals(self, seed: int, sample_customer_id: str) -> None:
        """Test that installment amounts are reasonable."""
        gen = LoanGenerator(seed=seed)
        loan, installments = gen.generate_with_installments(
            customer_id=sample_customer_id,
            loan_type="PERSONAL",
        )

        # All installments should have positive amounts
        for inst in installments:
            assert inst.total_amount > 0
            assert inst.principal_amount >= 0
            assert inst.interest_amount >= 0


class TestFraudPatternGenerator:
    """Tests for FraudPatternGenerator."""

    def test_velocity_pattern(self, seed: int, sample_account_id: str) -> None:
        """Test velocity fraud pattern."""
        tx_gen = TransactionGenerator(seed=seed)
        fraud_gen = FraudPatternGenerator(seed=seed)

        base_tx = tx_gen.generate(sample_account_id)
        frauds = fraud_gen.inject_velocity_pattern(base_tx, count=5)

        assert len(frauds) == 5
        # All transactions should be within a short time window
        timestamps = [f.timestamp for f in frauds]
        time_diff = max(timestamps) - min(timestamps)
        assert time_diff.total_seconds() <= 600  # 10 minutes

    def test_amount_anomaly(self, seed: int, sample_account_id: str) -> None:
        """Test amount anomaly fraud pattern."""
        tx_gen = TransactionGenerator(seed=seed)
        fraud_gen = FraudPatternGenerator(seed=seed)

        base_tx = tx_gen.generate(sample_account_id)
        fraud_tx = fraud_gen.inject_amount_anomaly(base_tx, multiplier=50)

        assert fraud_tx.amount > base_tx.amount * 40  # Allow some variance


class TestPaymentBehavior:
    """Tests for PaymentBehavior."""

    def test_apply_payment_behavior(self, seed: int, sample_customer_id: str) -> None:
        """Test payment behavior application."""
        loan_gen = LoanGenerator(seed=seed)
        behavior = PaymentBehavior(seed=seed)

        _, installments = loan_gen.generate_with_installments(
            customer_id=sample_customer_id,
            loan_type="PERSONAL",
        )

        modified = behavior.apply_payment_behavior(
            installments,
            on_time_rate=0.80,
            late_rate=0.15,
            default_rate=0.05,
        )

        # Check that statuses were assigned
        statuses = [i.status for i in modified]
        assert "PAID" in statuses or "PENDING" in statuses
