"""Fraud detection scenario for generating transactions with fraud patterns."""

import logging
import random
from typing import Any

from data_gen.generators.financial import (
    AccountGenerator,
    CustomerGenerator,
    TransactionGenerator,
)
from data_gen.generators.financial.patterns import FraudPatternGenerator
from data_gen.store.financial import FinancialDataStore

logger = logging.getLogger(__name__)


class FraudDetectionScenario:
    """Generate transaction data with injected fraud patterns.

    This scenario creates:
    - Normal customers with accounts and transactions
    - Fraudulent transactions with various patterns:
        - Velocity attacks (many transactions in short time)
        - Amount anomalies (unusually large amounts)
        - Geographic impossibilities (transactions far apart)
        - Night activity (unusual hours)
        - New payee large amounts
    """

    def __init__(
        self,
        num_customers: int = 1000,
        transactions_per_customer: int = 50,
        fraud_rate: float = 0.02,
        seed: int | None = None,
    ) -> None:
        """Initialize fraud detection scenario.

        Parameters
        ----------
        num_customers : int
            Number of customers to generate.
        transactions_per_customer : int
            Average transactions per customer.
        fraud_rate : float
            Percentage of transactions that are fraudulent (0.0 to 1.0).
        seed : int | None
            Random seed for reproducibility.
        """
        self.num_customers = num_customers
        self.transactions_per_customer = transactions_per_customer
        self.fraud_rate = fraud_rate
        self.seed = seed

        if seed is not None:
            random.seed(seed)

        self.store = FinancialDataStore()
        self._customer_gen = CustomerGenerator(seed=seed)
        self._account_gen = AccountGenerator(seed=seed)
        self._transaction_gen = TransactionGenerator(seed=seed)
        self._fraud_gen = FraudPatternGenerator(seed=seed)

        self._fraud_transactions: list[Any] = []

    def generate(self) -> FinancialDataStore:
        """Generate all data for the fraud detection scenario.

        Returns
        -------
        FinancialDataStore
            Store containing all generated data.
        """
        logger.info(
            "Starting fraud detection scenario: %d customers, %.1f%% fraud rate",
            self.num_customers,
            self.fraud_rate * 100,
        )

        # Generate customers
        for _ in range(self.num_customers):
            customer = self._customer_gen.generate()
            self.store.add_customer(customer)

            # Each customer gets 1-2 accounts
            num_accounts = random.randint(1, 2)
            for _ in range(num_accounts):
                account = self._account_gen.generate(customer.customer_id)
                self.store.add_account(account)

        logger.info(
            "Generated %d customers with %d accounts",
            len(self.store.customers),
            len(self.store.accounts),
        )

        # Generate transactions
        accounts = list(self.store.accounts.values())
        total_transactions = self.num_customers * self.transactions_per_customer
        fraud_count = int(total_transactions * self.fraud_rate)
        normal_count = total_transactions - fraud_count

        # Generate normal transactions
        for _ in range(normal_count):
            account = random.choice(accounts)
            transaction = self._transaction_gen.generate(account.account_id)
            self.store.add_transaction(transaction)

        logger.info("Generated %d normal transactions", normal_count)

        # Generate fraudulent transactions
        fraud_patterns = ["velocity", "amount_anomaly", "night_activity", "new_payee"]
        for _ in range(fraud_count):
            account = random.choice(accounts)
            base_transaction = self._transaction_gen.generate(account.account_id)
            pattern = random.choice(fraud_patterns)

            if pattern == "velocity":
                frauds = self._fraud_gen.inject_velocity_pattern(
                    base_transaction, count=random.randint(3, 8)
                )
                for fraud_tx in frauds:
                    self.store.add_transaction(fraud_tx)
                    self._fraud_transactions.append(fraud_tx)
            elif pattern == "amount_anomaly":
                fraud_tx = self._fraud_gen.inject_amount_anomaly(
                    base_transaction, multiplier=random.uniform(20, 100)
                )
                self.store.add_transaction(fraud_tx)
                self._fraud_transactions.append(fraud_tx)
            elif pattern == "night_activity":
                fraud_tx = self._fraud_gen.inject_night_activity(base_transaction)
                self.store.add_transaction(fraud_tx)
                self._fraud_transactions.append(fraud_tx)
            else:  # new_payee
                fraud_tx = self._fraud_gen.inject_new_payee_large_amount(base_transaction)
                self.store.add_transaction(fraud_tx)
                self._fraud_transactions.append(fraud_tx)

        logger.info(
            "Generated %d fraud transactions (%d total fraud records)",
            fraud_count,
            len(self._fraud_transactions),
        )

        return self.store

    def export(self, sinks: list[Any]) -> None:
        """Export generated data to sinks.

        Parameters
        ----------
        sinks : list[Any]
            List of sink instances (KafkaSink, PostgresSink, etc.).
        """
        for sink in sinks:
            # Export base entities
            sink.write_batch("customers", list(self.store.customers.values()))
            sink.write_batch("accounts", list(self.store.accounts.values()))
            sink.write_batch("transactions", self.store.transactions)

        logger.info("Exported data to %d sinks", len(sinks))

    @property
    def fraud_transactions(self) -> list[Any]:
        """Get list of fraudulent transactions for labeling."""
        return self._fraud_transactions

    def get_labels(self) -> dict[str, bool]:
        """Get fraud labels for all transactions.

        Returns
        -------
        dict[str, bool]
            Mapping of transaction_id to is_fraud.
        """
        fraud_ids = {tx.transaction_id for tx in self._fraud_transactions}
        return {
            tx.transaction_id: tx.transaction_id in fraud_ids
            for tx in self.store.transactions
        }
