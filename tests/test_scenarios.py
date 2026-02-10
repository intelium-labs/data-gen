"""Tests for scenarios."""

import pytest

from data_gen.config import ScenarioConfig
from data_gen.scenarios.financial import (
    Customer360Scenario,
    FraudDetectionScenario,
    LoanPortfolioScenario,
)


class TestFraudDetectionScenario:
    """Tests for FraudDetectionScenario."""

    def test_generate_scenario(self, seed: int) -> None:
        """Test fraud detection scenario generation."""
        scenario = FraudDetectionScenario(
            num_customers=10,
            transactions_per_customer=5,
            fraud_rate=0.10,
            seed=seed,
        )
        store = scenario.generate()

        assert len(store.customers) == 10
        assert len(store.accounts) >= 10  # At least one per customer
        assert len(store.transactions) > 0

    def test_fraud_labels(self, seed: int) -> None:
        """Test that fraud labels are generated."""
        scenario = FraudDetectionScenario(
            num_customers=10,
            transactions_per_customer=10,
            fraud_rate=0.10,
            seed=seed,
        )
        scenario.generate()

        labels = scenario.get_labels()
        assert len(labels) > 0

        # Should have some fraud transactions
        fraud_count = sum(1 for is_fraud in labels.values() if is_fraud)
        assert fraud_count > 0


class TestLoanPortfolioScenario:
    """Tests for LoanPortfolioScenario."""

    def test_generate_scenario(self, seed: int) -> None:
        """Test loan portfolio scenario generation."""
        scenario = LoanPortfolioScenario(
            num_customers=20,
            loan_penetration=0.50,
            seed=seed,
        )
        store = scenario.generate()

        assert len(store.customers) == 20
        assert len(store.loans) >= 5  # ~50% of 20

    def test_portfolio_summary(self, seed: int) -> None:
        """Test portfolio summary statistics."""
        scenario = LoanPortfolioScenario(
            num_customers=20,
            loan_penetration=0.50,
            seed=seed,
        )
        scenario.generate()

        summary = scenario.get_portfolio_summary()

        assert summary["total_loans"] > 0
        assert summary["total_principal"] > 0
        assert "loan_status_distribution" in summary

    def test_update_loan_statuses_no_installments(self, seed: int) -> None:
        """Test _update_loan_statuses handles loans without installments (line 165)."""
        from datetime import datetime
        from decimal import Decimal

        from data_gen.models.financial import Loan

        scenario = LoanPortfolioScenario(num_customers=5, loan_penetration=0.0, seed=seed)
        scenario.generate()

        # Manually add a loan without installments
        loan = Loan(
            loan_id="test-loan-no-inst",
            customer_id=list(scenario.store.customers.keys())[0],
            loan_type="PERSONAL",
            principal=Decimal("10000.00"),
            interest_rate=Decimal("0.02"),
            term_months=12,
            amortization_system="PRICE",
            status="ACTIVE",
            disbursement_date=datetime.now().date(),
            property_id=None,
            created_at=datetime.now(),
        )
        scenario.store.loans[loan.loan_id] = loan
        # Note: No installments added

        # Should not crash - just skip this loan
        scenario._update_loan_statuses()

        # Loan status should remain unchanged (no installments to evaluate)
        assert scenario.store.loans[loan.loan_id].status == "ACTIVE"

    def test_update_loan_statuses_delinquent(self, seed: int) -> None:
        """Test _update_loan_statuses marks loan as DELINQUENT with 3+ late payments (line 182)."""
        from datetime import datetime, timedelta
        from decimal import Decimal

        from data_gen.models.financial import Installment, Loan

        scenario = LoanPortfolioScenario(num_customers=5, loan_penetration=0.0, seed=seed)
        scenario.generate()

        # Manually add a loan
        customer_id = list(scenario.store.customers.keys())[0]
        loan = Loan(
            loan_id="test-loan-delinquent",
            customer_id=customer_id,
            loan_type="PERSONAL",
            principal=Decimal("10000.00"),
            interest_rate=Decimal("0.02"),
            term_months=6,
            amortization_system="PRICE",
            status="ACTIVE",
            disbursement_date=(datetime.now() - timedelta(days=180)).date(),
            property_id=None,
            created_at=datetime.now() - timedelta(days=180),
        )
        scenario.store.loans[loan.loan_id] = loan
        scenario.store._loan_installments[loan.loan_id] = []

        # Add 3+ LATE installments (not DEFAULT)
        # Store indices into installments list, not installment IDs
        base_idx = len(scenario.store.installments)
        for i in range(4):
            inst = Installment(
                installment_id=f"inst-late-{i}",
                loan_id=loan.loan_id,
                installment_number=i + 1,
                due_date=(datetime.now() - timedelta(days=150 - i * 30)).date(),
                principal_amount=Decimal("1666.67"),
                interest_amount=Decimal("166.67"),
                total_amount=Decimal("1833.34"),
                paid_date=None,
                paid_amount=None,
                status="LATE",  # Late but not default
            )
            scenario.store.installments.append(inst)
            # Store index into installments list, not installment_id
            scenario.store._loan_installments[loan.loan_id].append(base_idx + i)

        # Update statuses
        scenario._update_loan_statuses()

        # Loan should be marked as DELINQUENT
        assert scenario.store.loans[loan.loan_id].status == "DELINQUENT"


class TestCustomer360Scenario:
    """Tests for Customer360Scenario."""

    def test_generate_scenario(self, seed: int) -> None:
        """Test customer 360 scenario generation."""
        scenario = Customer360Scenario(
            num_customers=5,
            card_penetration=0.80,
            loan_penetration=0.40,
            seed=seed,
        )
        store = scenario.generate()

        assert len(store.customers) == 5
        assert len(store.accounts) >= 5
        assert len(store.transactions) > 0

    def test_customer_view(self, seed: int) -> None:
        """Test getting complete customer view."""
        scenario = Customer360Scenario(
            num_customers=3,
            seed=seed,
        )
        store = scenario.generate()

        # Get first customer's view
        customer_id = list(store.customers.keys())[0]
        view = scenario.get_customer_view(customer_id)

        assert view is not None
        assert view["customer"].customer_id == customer_id
        assert "accounts" in view
        assert "risk_indicators" in view
        assert "credit_score" in view["risk_indicators"]

    def test_scenario_summary(self, seed: int) -> None:
        """Test scenario summary statistics."""
        scenario = Customer360Scenario(
            num_customers=5,
            seed=seed,
        )
        scenario.generate()

        summary = scenario.get_summary()

        assert summary["total_customers"] == 5
        assert summary["total_accounts"] >= 5
        assert "avg_credit_score" in summary


class TestScenarioConfigIntegration:
    """Tests for scenarios using ScenarioConfig."""

    def test_fraud_detection_with_config(self, seed: int) -> None:
        """Test FraudDetectionScenario with ScenarioConfig."""
        config = ScenarioConfig(
            name="fraud_detection",
            num_customers=8,
            transactions_per_customer=5,
            enable_fraud_patterns=True,
            fraud_rate=0.15,
        )
        scenario = FraudDetectionScenario(seed=seed, config=config)

        assert scenario.num_customers == 8
        assert scenario.transactions_per_customer == 5
        assert scenario.fraud_rate == 0.15
        assert scenario.config is config

        store = scenario.generate()
        assert len(store.customers) == 8

    def test_loan_portfolio_with_config(self, seed: int) -> None:
        """Test LoanPortfolioScenario with ScenarioConfig."""
        config = ScenarioConfig(
            name="loan_portfolio",
            num_customers=15,
        )
        scenario = LoanPortfolioScenario(
            loan_penetration=0.50, seed=seed, config=config
        )

        assert scenario.num_customers == 15
        assert scenario.config is config

        store = scenario.generate()
        assert len(store.customers) == 15

    def test_customer_360_with_config(self, seed: int) -> None:
        """Test Customer360Scenario with ScenarioConfig."""
        config = ScenarioConfig(
            name="customer_360",
            num_customers=6,
            transactions_per_customer=10,
        )
        scenario = Customer360Scenario(seed=seed, config=config)

        assert scenario.num_customers == 6
        assert scenario.transactions_per_account == 10
        assert scenario.config is config

        store = scenario.generate()
        assert len(store.customers) == 6

    def test_scenarios_without_config_unchanged(self, seed: int) -> None:
        """Test that scenarios work unchanged without config parameter."""
        scenario = FraudDetectionScenario(
            num_customers=5,
            transactions_per_customer=3,
            fraud_rate=0.05,
            seed=seed,
        )
        assert scenario.config is None
        assert scenario.num_customers == 5

        store = scenario.generate()
        assert len(store.customers) == 5
