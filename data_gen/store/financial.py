"""Financial domain data store with referential integrity."""

from dataclasses import dataclass, field

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
            raise ValueError(f"Customer {account.customer_id} not found")

        self.accounts[account.account_id] = account
        self._customer_accounts[account.customer_id].append(account.account_id)
        self._account_transactions[account.account_id] = []

    def add_credit_card(self, card: CreditCard) -> None:
        """Add a credit card to the store."""
        if card.customer_id not in self.customers:
            raise ValueError(f"Customer {card.customer_id} not found")

        self.credit_cards[card.card_id] = card
        self._customer_cards[card.customer_id].append(card.card_id)
        self._card_transactions[card.card_id] = []

    def add_loan(self, loan: Loan) -> None:
        """Add a loan to the store."""
        if loan.customer_id not in self.customers:
            raise ValueError(f"Customer {loan.customer_id} not found")

        if loan.property_id and loan.property_id not in self.properties:
            raise ValueError(f"Property {loan.property_id} not found")

        self.loans[loan.loan_id] = loan
        self._customer_loans[loan.customer_id].append(loan.loan_id)
        self._loan_installments[loan.loan_id] = []

    def add_property(self, prop: Property) -> None:
        """Add a property to the store."""
        self.properties[prop.property_id] = prop

    def add_stock(self, stock: Stock) -> None:
        """Add a stock to the store."""
        self.stocks[stock.stock_id] = stock

    def add_trade(self, trade: Trade) -> None:
        """Add a trade to the store."""
        if trade.account_id not in self.accounts:
            raise ValueError(f"Account {trade.account_id} not found")

        if trade.stock_id not in self.stocks:
            raise ValueError(f"Stock {trade.stock_id} not found")

        # Ensure account is investment type
        account = self.accounts[trade.account_id]
        if account.account_type != "INVESTIMENTOS":
            raise ValueError(f"Account {trade.account_id} is not an investment account")

        idx = len(self.trades)
        self.trades.append(trade)
        if trade.account_id not in self._account_trades:
            self._account_trades[trade.account_id] = []
        self._account_trades[trade.account_id].append(idx)

    def add_transaction(self, transaction: Transaction) -> None:
        """Add a transaction to the store."""
        if transaction.account_id not in self.accounts:
            raise ValueError(f"Account {transaction.account_id} not found")

        idx = len(self.transactions)
        self.transactions.append(transaction)
        self._account_transactions[transaction.account_id].append(idx)

    def add_card_transaction(self, transaction: CardTransaction) -> None:
        """Add a card transaction to the store."""
        if transaction.card_id not in self.credit_cards:
            raise ValueError(f"Credit card {transaction.card_id} not found")

        idx = len(self.card_transactions)
        self.card_transactions.append(transaction)
        self._card_transactions[transaction.card_id].append(idx)

    def add_installment(self, installment: Installment) -> None:
        """Add a loan installment to the store."""
        if installment.loan_id not in self.loans:
            raise ValueError(f"Loan {installment.loan_id} not found")

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
        import random

        return random.choice(list(self.accounts.values()))

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
