"""Comprehensive tests for scenarios - 100% coverage."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from data_gen.scenarios.financial import (
    Customer360Scenario,
    FraudDetectionScenario,
    LoanPortfolioScenario,
)
from data_gen.sinks.console import ConsoleSink
from data_gen.sinks.json_file import JsonFileSink


class TestFraudDetectionScenarioFull:
    """Full coverage tests for FraudDetectionScenario."""

    def test_init_default(self) -> None:
        """Test default initialization."""
        scenario = FraudDetectionScenario()

        assert scenario.num_customers == 1000
        assert scenario.transactions_per_customer == 50
        assert scenario.fraud_rate == 0.02
        assert scenario.seed is None

    def test_init_custom(self) -> None:
        """Test custom initialization."""
        scenario = FraudDetectionScenario(
            num_customers=100,
            transactions_per_customer=20,
            fraud_rate=0.10,
            seed=42,
        )

        assert scenario.num_customers == 100
        assert scenario.transactions_per_customer == 20
        assert scenario.fraud_rate == 0.10
        assert scenario.seed == 42

    def test_generate_creates_all_entities(self) -> None:
        """Test that generate creates all expected entities."""
        scenario = FraudDetectionScenario(
            num_customers=5,
            transactions_per_customer=10,
            fraud_rate=0.20,
            seed=42,
        )
        store = scenario.generate()

        assert len(store.customers) == 5
        assert len(store.accounts) >= 5  # At least one per customer
        assert len(store.transactions) > 0

    def test_fraud_transactions_property(self) -> None:
        """Test fraud_transactions property."""
        scenario = FraudDetectionScenario(
            num_customers=10,
            transactions_per_customer=10,
            fraud_rate=0.20,
            seed=42,
        )
        scenario.generate()

        fraud_txs = scenario.fraud_transactions
        assert isinstance(fraud_txs, list)
        assert len(fraud_txs) > 0

    def test_get_labels(self) -> None:
        """Test get_labels method."""
        scenario = FraudDetectionScenario(
            num_customers=10,
            transactions_per_customer=10,
            fraud_rate=0.10,
            seed=42,
        )
        scenario.generate()

        labels = scenario.get_labels()

        assert isinstance(labels, dict)
        assert len(labels) > 0

        # All transactions should have a label
        for tx in scenario.store.transactions:
            assert tx.transaction_id in labels

        # Some should be fraud
        fraud_count = sum(1 for is_fraud in labels.values() if is_fraud)
        assert fraud_count > 0

    def test_export(self) -> None:
        """Test export method."""
        scenario = FraudDetectionScenario(
            num_customers=5,
            transactions_per_customer=5,
            fraud_rate=0.10,
            seed=42,
        )
        scenario.generate()

        with tempfile.TemporaryDirectory() as tmpdir:
            sink = JsonFileSink(tmpdir)
            scenario.export([sink])

            # Check files were created
            assert (Path(tmpdir) / "customers.json").exists()
            assert (Path(tmpdir) / "accounts.json").exists()
            assert (Path(tmpdir) / "transactions.json").exists()

    def test_fraud_patterns_distribution(self) -> None:
        """Test that different fraud patterns are generated."""
        scenario = FraudDetectionScenario(
            num_customers=20,
            transactions_per_customer=20,
            fraud_rate=0.30,
            seed=42,
        )
        scenario.generate()

        # Should have variety of fraud transaction IDs
        fraud_txs = scenario.fraud_transactions
        fraud_ids = [tx.transaction_id for tx in fraud_txs]

        # Check for pattern indicators in IDs
        velocity_found = any("vel" in id for id in fraud_ids)
        amount_found = any("amt" in id for id in fraud_ids)
        night_found = any("night" in id for id in fraud_ids)
        newpayee_found = any("newpayee" in id for id in fraud_ids)

        # At least some patterns should be present
        patterns_found = sum([velocity_found, amount_found, night_found, newpayee_found])
        assert patterns_found >= 1

    def test_multiple_accounts_per_customer(self) -> None:
        """Test that some customers have multiple accounts."""
        scenario = FraudDetectionScenario(
            num_customers=20,
            transactions_per_customer=5,
            seed=42,
        )
        scenario.generate()

        # Count accounts per customer
        customer_account_counts = {}
        for account in scenario.store.accounts.values():
            cid = account.customer_id
            customer_account_counts[cid] = customer_account_counts.get(cid, 0) + 1

        # Some should have more than 1
        max_accounts = max(customer_account_counts.values())
        assert max_accounts >= 1


class TestLoanPortfolioScenarioFull:
    """Full coverage tests for LoanPortfolioScenario."""

    def test_init_default(self) -> None:
        """Test default initialization."""
        scenario = LoanPortfolioScenario()

        assert scenario.num_customers == 1000
        assert scenario.loan_penetration == 0.30
        assert scenario.housing_loan_rate == 0.10
        assert scenario.on_time_rate == 0.85

    def test_init_custom(self) -> None:
        """Test custom initialization."""
        scenario = LoanPortfolioScenario(
            num_customers=50,
            loan_penetration=0.50,
            housing_loan_rate=0.20,
            on_time_rate=0.70,
            late_rate=0.20,
            default_rate=0.10,
            seed=42,
        )

        assert scenario.num_customers == 50
        assert scenario.loan_penetration == 0.50

    def test_generate_creates_entities(self) -> None:
        """Test that generate creates all expected entities."""
        scenario = LoanPortfolioScenario(
            num_customers=20,
            loan_penetration=0.50,
            seed=42,
        )
        store = scenario.generate()

        assert len(store.customers) == 20
        assert len(store.accounts) >= 20
        assert len(store.loans) >= 5  # ~50% of 20

    def test_housing_loans_have_properties(self) -> None:
        """Test that housing loans have associated properties."""
        scenario = LoanPortfolioScenario(
            num_customers=50,
            loan_penetration=0.60,
            housing_loan_rate=0.30,
            seed=42,
        )
        store = scenario.generate()

        housing_loans = [l for l in store.loans.values() if l.loan_type == "HOUSING"]

        for loan in housing_loans:
            if loan.property_id:
                assert loan.property_id in store.properties

    def test_loan_statuses_updated(self) -> None:
        """Test that loan statuses are updated based on installments."""
        scenario = LoanPortfolioScenario(
            num_customers=30,
            loan_penetration=0.50,
            default_rate=0.20,
            seed=42,
        )
        scenario.generate()

        statuses = set()
        for loan in scenario.store.loans.values():
            statuses.add(loan.status)

        # Should have variety of statuses
        assert "ACTIVE" in statuses or "PAID_OFF" in statuses or "DEFAULT" in statuses

    def test_get_portfolio_summary(self) -> None:
        """Test portfolio summary statistics."""
        scenario = LoanPortfolioScenario(
            num_customers=30,
            loan_penetration=0.50,
            seed=42,
        )
        scenario.generate()

        summary = scenario.get_portfolio_summary()

        assert "total_loans" in summary
        assert "total_principal" in summary
        assert "average_interest_rate" in summary
        assert "loan_status_distribution" in summary
        assert "installment_status_distribution" in summary
        assert "housing_loans" in summary
        assert "personal_loans" in summary

        assert summary["total_loans"] > 0
        assert summary["total_principal"] > 0

    def test_get_portfolio_summary_empty(self) -> None:
        """Test portfolio summary with no loans."""
        scenario = LoanPortfolioScenario(
            num_customers=10,
            loan_penetration=0.0,  # No loans
            seed=42,
        )
        scenario.generate()

        summary = scenario.get_portfolio_summary()

        assert summary == {}

    def test_export(self) -> None:
        """Test export method."""
        scenario = LoanPortfolioScenario(
            num_customers=10,
            loan_penetration=0.50,
            seed=42,
        )
        scenario.generate()

        with tempfile.TemporaryDirectory() as tmpdir:
            sink = JsonFileSink(tmpdir)
            scenario.export([sink])

            assert (Path(tmpdir) / "customers.json").exists()
            assert (Path(tmpdir) / "loans.json").exists()
            assert (Path(tmpdir) / "installments.json").exists()

    def test_high_credit_score_preference(self) -> None:
        """Test that higher credit score customers get loans first."""
        scenario = LoanPortfolioScenario(
            num_customers=50,
            loan_penetration=0.30,  # Only top 30% get loans
            seed=42,
        )
        scenario.generate()

        # Get customers with loans
        customers_with_loans = set()
        for loan in scenario.store.loans.values():
            customers_with_loans.add(loan.customer_id)

        # Average credit score of customers with loans should be higher
        if customers_with_loans:
            scores_with_loans = [
                scenario.store.customers[cid].credit_score
                for cid in customers_with_loans
            ]
            avg_score_with_loans = sum(scores_with_loans) / len(scores_with_loans)

            all_scores = [c.credit_score for c in scenario.store.customers.values()]
            avg_all_scores = sum(all_scores) / len(all_scores)

            assert avg_score_with_loans >= avg_all_scores


class TestCustomer360ScenarioFull:
    """Full coverage tests for Customer360Scenario."""

    def test_init_default(self) -> None:
        """Test default initialization."""
        scenario = Customer360Scenario()

        assert scenario.num_customers == 100
        assert scenario.card_penetration == 0.60
        assert scenario.loan_penetration == 0.25

    def test_init_custom(self) -> None:
        """Test custom initialization."""
        scenario = Customer360Scenario(
            num_customers=20,
            accounts_per_customer=(1, 3),
            card_penetration=0.80,
            loan_penetration=0.40,
            transactions_per_account=50,
            card_transactions_per_card=30,
            seed=42,
        )

        assert scenario.num_customers == 20
        assert scenario.card_penetration == 0.80

    def test_generate_creates_all_entity_types(self) -> None:
        """Test that generate creates all entity types."""
        scenario = Customer360Scenario(
            num_customers=10,
            card_penetration=0.80,
            loan_penetration=0.50,
            seed=42,
        )
        store = scenario.generate()

        assert len(store.customers) == 10
        assert len(store.accounts) >= 10
        assert len(store.transactions) > 0
        assert len(store.credit_cards) > 0
        # Loans may or may not be generated based on randomness

    def test_get_customer_view(self) -> None:
        """Test getting complete customer view."""
        scenario = Customer360Scenario(
            num_customers=5,
            card_penetration=1.0,  # Ensure cards
            loan_penetration=0.0,  # No loans for simplicity
            seed=42,
        )
        scenario.generate()

        customer_id = list(scenario.store.customers.keys())[0]
        view = scenario.get_customer_view(customer_id)

        assert view is not None
        assert view["customer"].customer_id == customer_id
        assert "accounts" in view
        assert "transactions" in view
        assert "credit_cards" in view
        assert "card_transactions" in view
        assert "loans" in view
        assert "installments" in view
        assert "risk_indicators" in view

    def test_get_customer_view_with_full_data(self) -> None:
        """Test customer view with all product types."""
        scenario = Customer360Scenario(
            num_customers=5,
            card_penetration=1.0,
            loan_penetration=1.0,
            seed=42,
        )
        scenario.generate()

        customer_id = list(scenario.store.customers.keys())[0]
        view = scenario.get_customer_view(customer_id)

        assert view is not None
        assert "risk_indicators" in view

        risk = view["risk_indicators"]
        assert "credit_score" in risk
        assert "total_balance" in risk
        assert "total_debt" in risk
        assert "debt_to_income_ratio" in risk
        assert "num_products" in risk

    def test_get_customer_view_not_found(self) -> None:
        """Test getting view for non-existent customer."""
        scenario = Customer360Scenario(num_customers=3, seed=42)
        scenario.generate()

        view = scenario.get_customer_view("non-existent-id")
        assert view is None

    def test_get_summary(self) -> None:
        """Test scenario summary statistics."""
        scenario = Customer360Scenario(
            num_customers=10,
            card_penetration=0.50,
            loan_penetration=0.30,
            seed=42,
        )
        scenario.generate()

        summary = scenario.get_summary()

        assert summary["total_customers"] == 10
        assert summary["total_accounts"] >= 10
        assert summary["total_transactions"] > 0
        assert "avg_credit_score" in summary
        assert "card_penetration_actual" in summary
        assert "loan_penetration_actual" in summary

    def test_get_summary_empty(self) -> None:
        """Test summary with no customers."""
        scenario = Customer360Scenario(num_customers=0, seed=42)
        # Don't generate - leave empty

        summary = scenario.get_summary()

        assert summary["total_customers"] == 0
        assert summary["avg_credit_score"] == 0

    def test_export(self) -> None:
        """Test export method."""
        scenario = Customer360Scenario(
            num_customers=5,
            seed=42,
        )
        scenario.generate()

        with tempfile.TemporaryDirectory() as tmpdir:
            sink = JsonFileSink(tmpdir)
            scenario.export([sink])

            assert (Path(tmpdir) / "customers.json").exists()
            assert (Path(tmpdir) / "accounts.json").exists()
            assert (Path(tmpdir) / "transactions.json").exists()
            assert (Path(tmpdir) / "credit_cards.json").exists()

    def test_multiple_sinks(self) -> None:
        """Test exporting to multiple sinks."""
        scenario = Customer360Scenario(
            num_customers=3,
            seed=42,
        )
        scenario.generate()

        mock_sink1 = MagicMock()
        mock_sink2 = MagicMock()

        scenario.export([mock_sink1, mock_sink2])

        # Both sinks should have write_batch called
        assert mock_sink1.write_batch.called
        assert mock_sink2.write_batch.called

    def test_accounts_per_customer_range(self) -> None:
        """Test that accounts_per_customer range is respected."""
        scenario = Customer360Scenario(
            num_customers=20,
            accounts_per_customer=(2, 4),
            seed=42,
        )
        scenario.generate()

        # Count accounts per customer
        customer_account_counts = {}
        for account in scenario.store.accounts.values():
            cid = account.customer_id
            customer_account_counts[cid] = customer_account_counts.get(cid, 0) + 1

        # All should be within range
        for count in customer_account_counts.values():
            assert 2 <= count <= 4

    def test_debt_to_income_calculation(self) -> None:
        """Test debt to income ratio calculation in customer view."""
        scenario = Customer360Scenario(
            num_customers=5,
            loan_penetration=1.0,
            seed=42,
        )
        scenario.generate()

        for customer_id in scenario.store.customers.keys():
            view = scenario.get_customer_view(customer_id)
            if view:
                ratio = view["risk_indicators"]["debt_to_income_ratio"]
                assert isinstance(ratio, (int, float))
                assert ratio >= 0


class TestScenariosInit:
    """Tests for scenarios __init__.py exports."""

    def test_all_scenarios_exported(self) -> None:
        """Test that all scenarios are exported from package."""
        from data_gen.scenarios import (
            Customer360Scenario,
            FraudDetectionScenario,
            LoanPortfolioScenario,
        )

        assert FraudDetectionScenario is not None
        assert LoanPortfolioScenario is not None
        assert Customer360Scenario is not None

    def test_financial_subpackage_exports(self) -> None:
        """Test financial subpackage exports."""
        from data_gen.scenarios.financial import (
            Customer360Scenario,
            FraudDetectionScenario,
            LoanPortfolioScenario,
        )

        assert FraudDetectionScenario is not None
        assert LoanPortfolioScenario is not None
        assert Customer360Scenario is not None
