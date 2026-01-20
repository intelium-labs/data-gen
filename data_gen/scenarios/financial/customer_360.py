"""Customer 360 scenario for complete customer view generation."""

import logging
import random
from typing import Any

from data_gen.generators.financial import (
    AccountGenerator,
    CreditCardGenerator,
    CustomerGenerator,
    LoanGenerator,
    TransactionGenerator,
)
from data_gen.generators.financial.credit_card import CardTransactionGenerator
from data_gen.store.financial import FinancialDataStore

logger = logging.getLogger(__name__)


class Customer360Scenario:
    """Generate complete customer profiles with all financial products.

    This scenario creates a holistic view of customers including:
    - Customer demographics
    - Bank accounts with transaction history
    - Credit cards with purchase history
    - Loans with payment schedules
    - Risk indicators
    """

    def __init__(
        self,
        num_customers: int = 100,
        accounts_per_customer: tuple[int, int] = (1, 2),
        card_penetration: float = 0.60,
        loan_penetration: float = 0.25,
        transactions_per_account: int = 30,
        card_transactions_per_card: int = 20,
        seed: int | None = None,
    ) -> None:
        """Initialize customer 360 scenario.

        Parameters
        ----------
        num_customers : int
            Number of customers to generate.
        accounts_per_customer : tuple[int, int]
            Min and max accounts per customer.
        card_penetration : float
            Percentage of customers with credit cards.
        loan_penetration : float
            Percentage of customers with loans.
        transactions_per_account : int
            Average transactions per account.
        card_transactions_per_card : int
            Average transactions per credit card.
        seed : int | None
            Random seed for reproducibility.
        """
        self.num_customers = num_customers
        self.accounts_per_customer = accounts_per_customer
        self.card_penetration = card_penetration
        self.loan_penetration = loan_penetration
        self.transactions_per_account = transactions_per_account
        self.card_transactions_per_card = card_transactions_per_card
        self.seed = seed

        if seed is not None:
            random.seed(seed)

        self.store = FinancialDataStore()
        self._customer_gen = CustomerGenerator(seed=seed)
        self._account_gen = AccountGenerator(seed=seed)
        self._transaction_gen = TransactionGenerator(seed=seed)
        self._card_gen = CreditCardGenerator(seed=seed)
        self._card_tx_gen = CardTransactionGenerator(seed=seed)
        self._loan_gen = LoanGenerator(seed=seed)

    def generate(self) -> FinancialDataStore:
        """Generate all data for the customer 360 scenario.

        Returns
        -------
        FinancialDataStore
            Store containing all generated data.
        """
        logger.info(
            "Starting customer 360 scenario: %d customers",
            self.num_customers,
        )

        for _ in range(self.num_customers):
            self._generate_customer_profile()

        logger.info(
            "Generated complete profiles: %d customers, %d accounts, "
            "%d transactions, %d cards, %d card_transactions, %d loans",
            len(self.store.customers),
            len(self.store.accounts),
            len(self.store.transactions),
            len(self.store.credit_cards),
            len(self.store.card_transactions),
            len(self.store.loans),
        )

        return self.store

    def _generate_customer_profile(self) -> None:
        """Generate a complete profile for one customer."""
        # Generate customer
        customer = self._customer_gen.generate()
        self.store.add_customer(customer)

        # Generate accounts
        num_accounts = random.randint(*self.accounts_per_customer)
        for _ in range(num_accounts):
            account = self._account_gen.generate(customer.customer_id)
            self.store.add_account(account)

            # Generate transactions for this account
            num_transactions = random.randint(
                self.transactions_per_account // 2,
                self.transactions_per_account * 2,
            )
            for _ in range(num_transactions):
                transaction = self._transaction_gen.generate(account.account_id)
                self.store.add_transaction(transaction)

        # Maybe generate credit card
        if random.random() < self.card_penetration:
            card = self._card_gen.generate(customer.customer_id)
            self.store.add_credit_card(card)

            # Generate card transactions
            num_card_tx = random.randint(
                self.card_transactions_per_card // 2,
                self.card_transactions_per_card * 2,
            )
            for _ in range(num_card_tx):
                card_tx = self._card_tx_gen.generate(card.card_id)
                self.store.add_card_transaction(card_tx)

        # Maybe generate loan
        if random.random() < self.loan_penetration:
            loan_type = random.choice(["PERSONAL", "VEHICLE"])
            loan, installments = self._loan_gen.generate_with_installments(
                customer_id=customer.customer_id,
                loan_type=loan_type,
            )
            self.store.add_loan(loan)
            for inst in installments:
                self.store.add_installment(inst)

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
            sink.write_batch("transactions", self.store.transactions)
            sink.write_batch("credit_cards", list(self.store.credit_cards.values()))
            sink.write_batch("card_transactions", self.store.card_transactions)
            sink.write_batch("loans", list(self.store.loans.values()))
            sink.write_batch("installments", self.store.installments)

        logger.info("Exported customer 360 data to %d sinks", len(sinks))

    def get_customer_view(self, customer_id: str) -> dict[str, Any] | None:
        """Get complete view of a single customer.

        Parameters
        ----------
        customer_id : str
            Customer ID to retrieve.

        Returns
        -------
        dict[str, Any] | None
            Complete customer view or None if not found.
        """
        customer = self.store.customers.get(customer_id)
        if not customer:
            return None

        accounts = self.store.get_customer_accounts(customer_id)
        cards = self.store.get_customer_cards(customer_id)
        loans = self.store.get_customer_loans(customer_id)

        # Get all transactions for customer's accounts
        all_transactions = []
        for account in accounts:
            txs = self.store.get_account_transactions(account.account_id)
            all_transactions.extend(txs)

        # Get all card transactions
        all_card_transactions = []
        for card in cards:
            card_txs = self.store.get_card_transactions(card.card_id)
            all_card_transactions.extend(card_txs)

        # Get all installments for loans
        all_installments = []
        for loan in loans:
            insts = self.store.get_loan_installments(loan.loan_id)
            all_installments.extend(insts)

        # Calculate risk indicators
        total_balance = sum(a.balance for a in accounts)
        total_debt = sum(l.principal for l in loans if l.status == "ACTIVE")
        debt_to_income = (
            float(total_debt) / float(customer.monthly_income)
            if customer.monthly_income > 0
            else 0
        )

        return {
            "customer": customer,
            "accounts": accounts,
            "transactions": all_transactions,
            "credit_cards": cards,
            "card_transactions": all_card_transactions,
            "loans": loans,
            "installments": all_installments,
            "risk_indicators": {
                "credit_score": customer.credit_score,
                "total_balance": float(total_balance),
                "total_debt": float(total_debt),
                "debt_to_income_ratio": debt_to_income,
                "num_products": len(accounts) + len(cards) + len(loans),
            },
        }

    def get_summary(self) -> dict[str, Any]:
        """Get summary statistics for the generated data.

        Returns
        -------
        dict[str, Any]
            Summary statistics.
        """
        customers = list(self.store.customers.values())

        return {
            "total_customers": len(customers),
            "total_accounts": len(self.store.accounts),
            "total_transactions": len(self.store.transactions),
            "total_credit_cards": len(self.store.credit_cards),
            "total_card_transactions": len(self.store.card_transactions),
            "total_loans": len(self.store.loans),
            "total_installments": len(self.store.installments),
            "avg_credit_score": (
                sum(c.credit_score for c in customers) / len(customers)
                if customers
                else 0
            ),
            "card_penetration_actual": (
                len(self.store.credit_cards) / len(customers)
                if customers
                else 0
            ),
            "loan_penetration_actual": (
                len(self.store.loans) / len(customers) if customers else 0
            ),
        }
