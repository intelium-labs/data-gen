"""Financial domain data store with referential integrity."""

import random as _random
from dataclasses import dataclass, field
from datetime import datetime

from data_gen.exceptions import InvalidEntityStateError, ReferentialIntegrityError
from data_gen.models.financial.enums import AccountType
from data_gen.models.financial import (
    Account,
    CardTransaction,
    CreditCard,
    Customer,
    Installment,
    Loan,
    Property,
    Stock,
    Trade,
    Transaction,
)


@dataclass
class FinancialDataStore:
    """In-memory store for financial entities with relationship tracking."""

    # Primary entities
    customers: dict[str, Customer] = field(default_factory=dict)
    accounts: dict[str, Account] = field(default_factory=dict)
    credit_cards: dict[str, CreditCard] = field(default_factory=dict)
    loans: dict[str, Loan] = field(default_factory=dict)
    properties: dict[str, Property] = field(default_factory=dict)
    stocks: dict[str, Stock] = field(default_factory=dict)

    # Transactions
    transactions: list[Transaction] = field(default_factory=list)
    card_transactions: list[CardTransaction] = field(default_factory=list)
    installments: list[Installment] = field(default_factory=list)
    trades: list[Trade] = field(default_factory=list)

    # Relationship indexes
    _customer_accounts: dict[str, list[str]] = field(default_factory=dict)
    _customer_cards: dict[str, list[str]] = field(default_factory=dict)
    _customer_loans: dict[str, list[str]] = field(default_factory=dict)
    _account_transactions: dict[str, list[int]] = field(default_factory=dict)
    _card_transactions: dict[str, list[int]] = field(default_factory=dict)
    _loan_installments: dict[str, list[int]] = field(default_factory=dict)
    _account_trades: dict[str, list[int]] = field(default_factory=dict)

    def add_customer(self, customer: Customer) -> None:
        """Add a customer to the store."""
        self.customers[customer.customer_id] = customer
        self._customer_accounts[customer.customer_id] = []
        self._customer_cards[customer.customer_id] = []
        self._customer_loans[customer.customer_id] = []

    def add_account(self, account: Account) -> None:
        """Add an account to the store."""
        if account.customer_id not in self.customers:
            raise ReferentialIntegrityError(f"Customer {account.customer_id} not found")

        self.accounts[account.account_id] = account
        self._customer_accounts[account.customer_id].append(account.account_id)
        self._account_transactions[account.account_id] = []

    def add_credit_card(self, card: CreditCard) -> None:
        """Add a credit card to the store."""
        if card.customer_id not in self.customers:
            raise ReferentialIntegrityError(f"Customer {card.customer_id} not found")

        self.credit_cards[card.card_id] = card
        self._customer_cards[card.customer_id].append(card.card_id)
        self._card_transactions[card.card_id] = []

    def add_loan(self, loan: Loan) -> None:
        """Add a loan to the store."""
        if loan.customer_id not in self.customers:
            raise ReferentialIntegrityError(f"Customer {loan.customer_id} not found")

        if loan.property_id and loan.property_id not in self.properties:
            raise ReferentialIntegrityError(f"Property {loan.property_id} not found")

        self.loans[loan.loan_id] = loan
        self._customer_loans[loan.customer_id].append(loan.loan_id)
        self._loan_installments[loan.loan_id] = []

    def add_property(self, prop: Property) -> None:
        """Add a property to the store."""
        if prop.created_at is None:
            prop.created_at = datetime.now()
        self.properties[prop.property_id] = prop

    def add_stock(self, stock: Stock) -> None:
        """Add a stock to the store."""
        self.stocks[stock.stock_id] = stock

    def add_trade(self, trade: Trade) -> None:
        """Add a trade to the store."""
        if trade.account_id not in self.accounts:
            raise ReferentialIntegrityError(f"Account {trade.account_id} not found")

        if trade.stock_id not in self.stocks:
            raise ReferentialIntegrityError(f"Stock {trade.stock_id} not found")

        # Ensure account is investment type
        account = self.accounts[trade.account_id]
        if account.account_type != AccountType.INVESTIMENTOS:
            raise InvalidEntityStateError(f"Account {trade.account_id} is not an investment account")

        if trade.created_at is None:
            trade.created_at = datetime.now()
        idx = len(self.trades)
        self.trades.append(trade)
        if trade.account_id not in self._account_trades:
            self._account_trades[trade.account_id] = []
        self._account_trades[trade.account_id].append(idx)

    def add_transaction(self, transaction: Transaction) -> None:
        """Add a transaction to the store."""
        if transaction.account_id not in self.accounts:
            raise ReferentialIntegrityError(f"Account {transaction.account_id} not found")

        if transaction.created_at is None:
            transaction.created_at = datetime.now()
        idx = len(self.transactions)
        self.transactions.append(transaction)
        self._account_transactions[transaction.account_id].append(idx)

    def add_card_transaction(self, transaction: CardTransaction) -> None:
        """Add a card transaction to the store."""
        if transaction.card_id not in self.credit_cards:
            raise ReferentialIntegrityError(f"Credit card {transaction.card_id} not found")

        if transaction.created_at is None:
            transaction.created_at = datetime.now()
        idx = len(self.card_transactions)
        self.card_transactions.append(transaction)
        self._card_transactions[transaction.card_id].append(idx)

    def add_installment(self, installment: Installment) -> None:
        """Add a loan installment to the store."""
        if installment.loan_id not in self.loans:
            raise ReferentialIntegrityError(f"Loan {installment.loan_id} not found")

        if installment.created_at is None:
            installment.created_at = datetime.now()
        idx = len(self.installments)
        self.installments.append(installment)
        self._loan_installments[installment.loan_id].append(idx)

    # Query methods
    def get_customer_accounts(self, customer_id: str) -> list[Account]:
        """Get all accounts for a customer."""
        account_ids = self._customer_accounts.get(customer_id, [])
        return [self.accounts[aid] for aid in account_ids]

    def get_customer_cards(self, customer_id: str) -> list[CreditCard]:
        """Get all credit cards for a customer."""
        card_ids = self._customer_cards.get(customer_id, [])
        return [self.credit_cards[cid] for cid in card_ids]

    def get_customer_loans(self, customer_id: str) -> list[Loan]:
        """Get all loans for a customer."""
        loan_ids = self._customer_loans.get(customer_id, [])
        return [self.loans[lid] for lid in loan_ids]

    def get_account_transactions(self, account_id: str) -> list[Transaction]:
        """Get all transactions for an account."""
        indices = self._account_transactions.get(account_id, [])
        return [self.transactions[i] for i in indices]

    def get_card_transactions(self, card_id: str) -> list[CardTransaction]:
        """Get all transactions for a credit card."""
        indices = self._card_transactions.get(card_id, [])
        return [self.card_transactions[i] for i in indices]

    def get_loan_installments(self, loan_id: str) -> list[Installment]:
        """Get all installments for a loan."""
        indices = self._loan_installments.get(loan_id, [])
        return [self.installments[i] for i in indices]

    def get_account_trades(self, account_id: str) -> list[Trade]:
        """Get all trades for an investment account."""
        indices = self._account_trades.get(account_id, [])
        return [self.trades[i] for i in indices]

    def get_random_account(self) -> Account | None:
        """Get a random account (for generating counterparty transactions)."""
        if not self.accounts:
            return None
        if not hasattr(self, "_accounts_cache") or len(self._accounts_cache) != len(self.accounts):
            self._accounts_cache = list(self.accounts.values())
        return _random.choice(self._accounts_cache)

    def summary(self) -> dict[str, int]:
        """Return summary counts of all entities."""
        return {
            "customers": len(self.customers),
            "accounts": len(self.accounts),
            "credit_cards": len(self.credit_cards),
            "loans": len(self.loans),
            "properties": len(self.properties),
            "stocks": len(self.stocks),
            "transactions": len(self.transactions),
            "card_transactions": len(self.card_transactions),
            "installments": len(self.installments),
            "trades": len(self.trades),
        }


@dataclass
class MasterDataStore:
    """Lightweight store for master data only — no event lists in memory.

    Designed for bulk loading where events are streamed directly to sinks
    instead of being accumulated in memory.  At 1M customers this saves
    ~80 GB RAM compared to ``FinancialDataStore``.
    """

    # Primary entities
    customers: dict[str, Customer] = field(default_factory=dict)
    accounts: dict[str, Account] = field(default_factory=dict)
    credit_cards: dict[str, CreditCard] = field(default_factory=dict)
    loans: dict[str, Loan] = field(default_factory=dict)
    properties: dict[str, Property] = field(default_factory=dict)
    stocks: dict[str, Stock] = field(default_factory=dict)

    # Relationship indexes (master-to-master only)
    _customer_accounts: dict[str, list[str]] = field(default_factory=dict)
    _customer_cards: dict[str, list[str]] = field(default_factory=dict)
    _customer_loans: dict[str, list[str]] = field(default_factory=dict)

    # Cached account list for O(1) random access
    _accounts_cache: list[Account] = field(default_factory=list)

    # Event counters (for summary without storing events)
    _event_counts: dict[str, int] = field(default_factory=lambda: {
        "transactions": 0,
        "card_transactions": 0,
        "installments": 0,
        "trades": 0,
    })

    def add_customer(self, customer: Customer) -> None:
        """Add a customer to the store."""
        self.customers[customer.customer_id] = customer
        self._customer_accounts[customer.customer_id] = []
        self._customer_cards[customer.customer_id] = []
        self._customer_loans[customer.customer_id] = []

    def add_account(self, account: Account) -> None:
        """Add an account to the store."""
        if account.customer_id not in self.customers:
            raise ReferentialIntegrityError(f"Customer {account.customer_id} not found")

        self.accounts[account.account_id] = account
        self._customer_accounts[account.customer_id].append(account.account_id)
        self._accounts_cache.clear()  # invalidate cache

    def add_credit_card(self, card: CreditCard) -> None:
        """Add a credit card to the store."""
        if card.customer_id not in self.customers:
            raise ReferentialIntegrityError(f"Customer {card.customer_id} not found")

        self.credit_cards[card.card_id] = card
        self._customer_cards[card.customer_id].append(card.card_id)

    def add_loan(self, loan: Loan) -> None:
        """Add a loan to the store."""
        if loan.customer_id not in self.customers:
            raise ReferentialIntegrityError(f"Customer {loan.customer_id} not found")

        if loan.property_id and loan.property_id not in self.properties:
            raise ReferentialIntegrityError(f"Property {loan.property_id} not found")

        self.loans[loan.loan_id] = loan
        self._customer_loans[loan.customer_id].append(loan.loan_id)

    def add_property(self, prop: Property) -> None:
        """Add a property to the store."""
        if prop.created_at is None:
            prop.created_at = datetime.now()
        self.properties[prop.property_id] = prop

    def add_stock(self, stock: Stock) -> None:
        """Add a stock to the store."""
        self.stocks[stock.stock_id] = stock

    def count_event(self, event_type: str, count: int = 1) -> None:
        """Increment event counter without storing events in memory.

        Parameters
        ----------
        event_type : str
            One of: transactions, card_transactions, installments, trades.
        count : int
            Number of events to count (default: 1).
        """
        self._event_counts[event_type] = self._event_counts.get(event_type, 0) + count

    def validate_transaction_fk(self, account_id: str) -> None:
        """Validate that account exists for a transaction."""
        if account_id not in self.accounts:
            raise ReferentialIntegrityError(f"Account {account_id} not found")

    def validate_card_transaction_fk(self, card_id: str) -> None:
        """Validate that credit card exists for a card transaction."""
        if card_id not in self.credit_cards:
            raise ReferentialIntegrityError(f"Credit card {card_id} not found")

    def validate_trade_fk(self, account_id: str, stock_id: str) -> None:
        """Validate that account and stock exist for a trade."""
        if account_id not in self.accounts:
            raise ReferentialIntegrityError(f"Account {account_id} not found")
        if stock_id not in self.stocks:
            raise ReferentialIntegrityError(f"Stock {stock_id} not found")
        account = self.accounts[account_id]
        if account.account_type != AccountType.INVESTIMENTOS:
            raise InvalidEntityStateError(f"Account {account_id} is not an investment account")

    def validate_installment_fk(self, loan_id: str) -> None:
        """Validate that loan exists for an installment."""
        if loan_id not in self.loans:
            raise ReferentialIntegrityError(f"Loan {loan_id} not found")

    def get_customer_accounts(self, customer_id: str) -> list[Account]:
        """Get all accounts for a customer."""
        account_ids = self._customer_accounts.get(customer_id, [])
        return [self.accounts[aid] for aid in account_ids]

    def get_customer_cards(self, customer_id: str) -> list[CreditCard]:
        """Get all credit cards for a customer."""
        card_ids = self._customer_cards.get(customer_id, [])
        return [self.credit_cards[cid] for cid in card_ids]

    def get_customer_loans(self, customer_id: str) -> list[Loan]:
        """Get all loans for a customer."""
        loan_ids = self._customer_loans.get(customer_id, [])
        return [self.loans[lid] for lid in loan_ids]

    def get_random_account(self) -> Account | None:
        """Get a random account with O(1) amortized access."""
        if not self.accounts:
            return None
        if not self._accounts_cache:
            self._accounts_cache = list(self.accounts.values())
        return _random.choice(self._accounts_cache)

    def summary(self) -> dict[str, int]:
        """Return summary counts of all entities."""
        return {
            "customers": len(self.customers),
            "accounts": len(self.accounts),
            "credit_cards": len(self.credit_cards),
            "loans": len(self.loans),
            "properties": len(self.properties),
            "stocks": len(self.stocks),
            **self._event_counts,
        }
