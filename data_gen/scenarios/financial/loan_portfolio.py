"""Loan portfolio scenario for generating loan data with payment behavior."""

from __future__ import annotations

import logging
import random
from datetime import date, timedelta
from typing import Any

from data_gen.config import ScenarioConfig
from data_gen.generators.financial import (
    AccountGenerator,
    CustomerGenerator,
    LoanGenerator,
)
from data_gen.generators.financial.patterns import PaymentBehavior
from data_gen.models.financial.enums import InstallmentStatus, LoanStatus, LoanType
from data_gen.store.financial import FinancialDataStore

logger = logging.getLogger(__name__)


class LoanPortfolioScenario:
    """Generate a realistic loan portfolio with payment behavior.

    This scenario creates:
    - Customers with varying credit scores
    - Personal and housing loans based on eligibility
    - Installment schedules with realistic payment behavior:
        - On-time payments
        - Late payments (1-30 days)
        - Defaults (90+ days)
    """

    def __init__(
        self,
        num_customers: int = 1000,
        loan_penetration: float = 0.30,
        housing_loan_rate: float = 0.10,
        on_time_rate: float = 0.85,
        late_rate: float = 0.10,
        default_rate: float = 0.05,
        seed: int | None = None,
        *,
        config: ScenarioConfig | None = None,
    ) -> None:
        """Initialize loan portfolio scenario.

        Parameters
        ----------
        num_customers : int
            Number of customers to generate.
        loan_penetration : float
            Percentage of customers with loans (0.0 to 1.0).
        housing_loan_rate : float
            Percentage of loan customers with housing finance.
        on_time_rate : float
            Percentage of installments paid on time.
        late_rate : float
            Percentage of installments paid late.
        default_rate : float
            Percentage of installments in default.
        seed : int | None
            Random seed for reproducibility.
        config : ScenarioConfig | None
            Optional scenario configuration. If provided, overrides
            num_customers.
        """
        if config is not None:
            self.num_customers = config.num_customers
            self.seed = seed
            self.config = config
        else:
            self.num_customers = num_customers
            self.seed = seed
            self.config = None

        self.loan_penetration = loan_penetration
        self.housing_loan_rate = housing_loan_rate
        self.on_time_rate = on_time_rate
        self.late_rate = late_rate
        self.default_rate = default_rate

        if seed is not None:
            random.seed(seed)

        self.store = FinancialDataStore()
        self._customer_gen = CustomerGenerator(seed=seed)
        self._account_gen = AccountGenerator(seed=seed)
        self._loan_gen = LoanGenerator(seed=seed)
        self._payment_behavior = PaymentBehavior(seed=seed)

    def generate(self) -> FinancialDataStore:
        """Generate all data for the loan portfolio scenario.

        Returns
        -------
        FinancialDataStore
            Store containing all generated data.
        """
        logger.info(
            "Starting loan portfolio scenario: %d customers, %.0f%% with loans",
            self.num_customers,
            self.loan_penetration * 100,
        )

        # Generate customers
        for _ in range(self.num_customers):
            customer = self._customer_gen.generate()
            self.store.add_customer(customer)

            # Each customer gets at least one account
            account = self._account_gen.generate(customer.customer_id)
            self.store.add_account(account)

        logger.info("Generated %d customers", len(self.store.customers))

        # Determine which customers get loans
        customers = list(self.store.customers.values())
        num_loan_customers = int(len(customers) * self.loan_penetration)

        # Prefer customers with better credit scores for loans
        customers_sorted = sorted(customers, key=lambda c: c.credit_score, reverse=True)
        loan_customers = customers_sorted[:num_loan_customers]

        # Generate loans
        num_housing = int(len(loan_customers) * self.housing_loan_rate)

        for i, customer in enumerate(loan_customers):
            if i < num_housing and customer.credit_score >= 650:
                # Housing loan for higher credit score customers
                loan_type = LoanType.HOUSING
                # Generate property for housing loan
                from data_gen.generators.financial.loan import PropertyGenerator

                prop_gen = PropertyGenerator(seed=self.seed)
                prop = prop_gen.generate()
                self.store.add_property(prop)
                property_id = prop.property_id
            else:
                loan_type = LoanType.PERSONAL
                property_id = None

            # Generate loan with installments
            loan, installments = self._loan_gen.generate_with_installments(
                customer_id=customer.customer_id,
                loan_type=loan_type,
                property_id=property_id,
            )
            self.store.add_loan(loan)

            # Apply payment behavior to installments
            modified_installments = self._payment_behavior.apply_payment_behavior(
                installments,
                on_time_rate=self.on_time_rate,
                late_rate=self.late_rate,
                default_rate=self.default_rate,
            )

            for inst in modified_installments:
                self.store.add_installment(inst)

        logger.info(
            "Generated %d loans (%d housing, %d personal) with %d installments",
            len(self.store.loans),
            sum(1 for l in self.store.loans.values() if l.loan_type == LoanType.HOUSING),
            sum(1 for l in self.store.loans.values() if l.loan_type == LoanType.PERSONAL),
            len(self.store.installments),
        )

        # Update loan statuses based on installment behavior
        self._update_loan_statuses()

        return self.store

    def _update_loan_statuses(self) -> None:
        """Update loan statuses based on installment payment behavior."""
        for loan_id, loan in self.store.loans.items():
            installments = self.store.get_loan_installments(loan_id)
            if not installments:
                continue

            # Check for defaults
            defaults = [i for i in installments if i.status == InstallmentStatus.DEFAULT]
            if defaults:
                loan.status = LoanStatus.DEFAULT
                continue

            # Check if all paid
            all_paid = all(i.status == InstallmentStatus.PAID for i in installments)
            if all_paid:
                loan.status = LoanStatus.PAID_OFF
                continue

            # Check for late payments
            late = [i for i in installments if i.status == InstallmentStatus.LATE]
            if len(late) >= 3:
                loan.status = LoanStatus.DELINQUENT
            else:
                loan.status = LoanStatus.ACTIVE

    def export(self, sinks: list[Any]) -> None:
        """Export generated data to sinks.

        Parameters
        ----------
        sinks : list[Any]
            List of sink instances (KafkaSink, PostgresSink, etc.).
        """
        for sink in sinks:
            sink.write_batch("customers", list(self.store.customers.values()))
            sink.write_batch("accounts", list(self.store.accounts.values()))
            sink.write_batch("properties", list(self.store.properties.values()))
            sink.write_batch("loans", list(self.store.loans.values()))
            sink.write_batch("installments", self.store.installments)

        logger.info("Exported loan portfolio to %d sinks", len(sinks))

    def get_portfolio_summary(self) -> dict[str, Any]:
        """Get summary statistics for the loan portfolio.

        Returns
        -------
        dict[str, Any]
            Portfolio summary statistics.
        """
        loans = list(self.store.loans.values())
        installments = self.store.installments

        if not loans:
            return {}

        total_principal = sum(l.principal for l in loans)
        avg_rate = sum(l.interest_rate for l in loans) / len(loans)

        status_counts = {}
        for loan in loans:
            status_counts[loan.status] = status_counts.get(loan.status, 0) + 1

        installment_status = {}
        for inst in installments:
            installment_status[inst.status] = installment_status.get(inst.status, 0) + 1

        return {
            "total_loans": len(loans),
            "total_principal": float(total_principal),
            "average_interest_rate": float(avg_rate),
            "loan_status_distribution": status_counts,
            "installment_status_distribution": installment_status,
            "housing_loans": sum(1 for l in loans if l.loan_type == LoanType.HOUSING),
            "personal_loans": sum(1 for l in loans if l.loan_type == LoanType.PERSONAL),
        }
