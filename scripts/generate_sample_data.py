#!/usr/bin/env python3
"""Generate sample data files for validation.

This script generates sample JSON files in the local/ folder for each entity type.
These files can be used for manual validation and testing.
"""

import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data_gen.generators.financial import (
    AccountGenerator,
    CreditCardGenerator,
    CustomerGenerator,
    LoanGenerator,
    StockGenerator,
    TradeGenerator,
    TransactionGenerator,
)
from data_gen.generators.financial.loan import PropertyGenerator
from data_gen.generators.financial.patterns import FraudPatternGenerator, PaymentBehavior
from data_gen.store.financial import FinancialDataStore


def serialize_value(obj: object) -> object:
    """Serialize Python objects to JSON-compatible types."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if hasattr(obj, "__dataclass_fields__"):
        return {k: serialize_value(v) for k, v in obj.__dict__.items()}
    if isinstance(obj, dict):
        return {k: serialize_value(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [serialize_value(item) for item in obj]
    return obj


def save_json(data: list, filename: str, output_dir: Path) -> None:
    """Save data to JSON file."""
    filepath = output_dir / filename
    serialized = [serialize_value(item) for item in data]
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(serialized, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(data)} records to {filepath}")


def generate_customers(
    customer_gen: CustomerGenerator,
    store: FinancialDataStore,
    num_customers: int,
    output_dir: Path,
) -> list:
    """Generate customer data."""
    print("\n1. Generating customers...")
    customers = []
    for _ in range(num_customers):
        customer = customer_gen.generate()
        store.add_customer(customer)
        customers.append(customer)
    save_json(customers, "customers.json", output_dir)
    return customers


def generate_accounts(
    account_gen: AccountGenerator,
    store: FinancialDataStore,
    customers: list,
    output_dir: Path,
) -> list:
    """Generate account data."""
    print("\n2. Generating accounts...")
    accounts = []
    for customer in customers:
        for account in account_gen.generate_for_customer(
            customer.customer_id,
            customer.created_at,
            customer.monthly_income,
        ):
            store.add_account(account)
            accounts.append(account)
    save_json(accounts, "accounts.json", output_dir)
    return accounts


def generate_transactions(
    transaction_gen: TransactionGenerator,
    store: FinancialDataStore,
    accounts: list,
    output_dir: Path,
) -> list:
    """Generate transaction data."""
    print("\n3. Generating transactions...")
    transactions = []
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)

    for account in accounts[:5]:
        for tx in transaction_gen.generate_for_account(
            account, store, start_date, end_date, avg_transactions_per_day=0.5
        ):
            store.add_transaction(tx)
            transactions.append(tx)
    save_json(transactions, "transactions.json", output_dir)
    return transactions


def generate_credit_cards(
    credit_card_gen: CreditCardGenerator,
    store: FinancialDataStore,
    customers: list,
    output_dir: Path,
) -> tuple[list, list]:
    """Generate credit card and card transaction data."""
    print("\n4. Generating credit cards...")
    credit_cards = []
    card_transactions = []

    card_start_date = datetime(2024, 1, 1)
    card_end_date = datetime(2024, 3, 31)

    for customer in customers[:7]:
        card = credit_card_gen.generate(customer.customer_id)
        store.add_credit_card(card)
        credit_cards.append(card)

        for card_tx in credit_card_gen.generate_transactions(
            card, card_start_date, card_end_date, avg_transactions_per_day=0.5
        ):
            store.add_card_transaction(card_tx)
            card_transactions.append(card_tx)

    save_json(credit_cards, "credit_cards.json", output_dir)
    save_json(card_transactions, "card_transactions.json", output_dir)
    return credit_cards, card_transactions


def generate_properties(
    property_gen: PropertyGenerator,
    store: FinancialDataStore,
    output_dir: Path,
) -> list:
    """Generate property data."""
    print("\n5. Generating properties...")
    properties = []
    for _ in range(3):
        prop = property_gen.generate()
        store.add_property(prop)
        properties.append(prop)
    save_json(properties, "properties.json", output_dir)
    return properties


def generate_loans(
    loan_gen: LoanGenerator,
    payment_behavior: PaymentBehavior,
    store: FinancialDataStore,
    customers: list,
    properties: list,
    output_dir: Path,
) -> tuple[list, list]:
    """Generate loan and installment data."""
    print("\n6. Generating loans and installments...")
    loans = []
    installments = []

    # Personal loans for 40% of customers
    for customer in customers[:4]:
        loan, loan_installments = loan_gen.generate_with_installments(
            customer_id=customer.customer_id,
            loan_type="PERSONAL",
        )
        store.add_loan(loan)
        loans.append(loan)

        modified = payment_behavior.apply_payment_behavior(
            loan_installments, on_time_rate=0.80, late_rate=0.15, default_rate=0.05
        )
        for inst in modified:
            store.add_installment(inst)
            installments.append(inst)

    # Housing loan for highest income customer
    high_income_customer = max(customers, key=lambda c: c.monthly_income)
    housing_loan, housing_installments = loan_gen.generate_with_installments(
        customer_id=high_income_customer.customer_id,
        loan_type="HOUSING",
        property_id=properties[0].property_id,
    )
    store.add_loan(housing_loan)
    loans.append(housing_loan)

    for inst in housing_installments[:24]:
        store.add_installment(inst)
        installments.append(inst)

    save_json(loans, "loans.json", output_dir)
    save_json(installments, "installments.json", output_dir)
    return loans, installments


def generate_stocks(
    stock_gen: StockGenerator,
    store: FinancialDataStore,
    output_dir: Path,
) -> list:
    """Generate B3 stock data."""
    print("\n7. Generating B3 stocks...")
    stocks = []
    for stock in stock_gen.generate_all():
        store.add_stock(stock)
        stocks.append(stock)
    save_json(stocks, "stocks.json", output_dir)
    return stocks


def generate_trades(
    trade_gen: TradeGenerator,
    store: FinancialDataStore,
    accounts: list,
    stocks: list,
    output_dir: Path,
) -> list:
    """Generate stock trade data."""
    print("\n8. Generating trades...")
    trades = []

    # Find investment accounts
    investment_accounts = [a for a in accounts if a.account_type == "INVESTIMENTOS"]

    if not investment_accounts or not stocks:
        save_json(trades, "trades.json", output_dir)
        return trades

    # Generate trades for investment accounts
    for account in investment_accounts:
        account_trades = trade_gen.generate_trades_for_account(
            account_id=account.account_id,
            stocks=stocks,
            num_trades=15,
        )
        for trade in account_trades:
            store.add_trade(trade)
            trades.append(trade)

    save_json(trades, "trades.json", output_dir)
    return trades


def generate_fraud_examples(
    fraud_gen: FraudPatternGenerator,
    transactions: list,
    output_dir: Path,
) -> list:
    """Generate fraud pattern examples."""
    print("\n9. Generating fraud pattern examples...")
    fraud_examples = []

    if not transactions:
        save_json(fraud_examples, "fraud_examples.json", output_dir)
        return fraud_examples

    base_tx = transactions[0]
    fraud_examples.extend(fraud_gen.inject_velocity_pattern(base_tx, count=5))
    fraud_examples.append(fraud_gen.inject_amount_anomaly(base_tx, multiplier=50))
    fraud_examples.append(fraud_gen.inject_night_activity(base_tx))
    fraud_examples.append(fraud_gen.inject_new_payee_large_amount(base_tx))
    fraud_examples.extend(fraud_gen.inject_round_amounts(base_tx, count=3))

    save_json(fraud_examples, "fraud_examples.json", output_dir)
    return fraud_examples


def print_summary(data: dict[str, Any], output_dir: Path) -> None:
    """Print generation summary."""
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    for name, items in data.items():
        print(f"{name + ':':18}{len(items)}")
    print(f"\nAll files saved to: {output_dir}")
    print("=" * 60)


def main() -> None:
    """Generate all sample data files."""
    output_dir = project_root / "local"
    output_dir.mkdir(exist_ok=True)

    seed = 42
    num_customers = 10

    print("=" * 60)
    print("Generating Sample Data for Validation")
    print("=" * 60)

    # Initialize store and generators
    store = FinancialDataStore()
    customer_gen = CustomerGenerator(seed=seed)
    account_gen = AccountGenerator(seed=seed)
    transaction_gen = TransactionGenerator(seed=seed)
    credit_card_gen = CreditCardGenerator(seed=seed)
    loan_gen = LoanGenerator(seed=seed)
    property_gen = PropertyGenerator(seed=seed)
    stock_gen = StockGenerator(seed=seed)
    trade_gen = TradeGenerator(seed=seed)
    payment_behavior = PaymentBehavior(seed=seed)
    fraud_gen = FraudPatternGenerator(seed=seed)

    # Generate all data
    customers = generate_customers(customer_gen, store, num_customers, output_dir)
    accounts = generate_accounts(account_gen, store, customers, output_dir)
    transactions = generate_transactions(transaction_gen, store, accounts, output_dir)
    credit_cards, card_transactions = generate_credit_cards(
        credit_card_gen, store, customers, output_dir
    )
    properties = generate_properties(property_gen, store, output_dir)
    loans, installments = generate_loans(
        loan_gen, payment_behavior, store, customers, properties, output_dir
    )
    stocks = generate_stocks(stock_gen, store, output_dir)
    trades = generate_trades(trade_gen, store, accounts, stocks, output_dir)
    fraud_examples = generate_fraud_examples(fraud_gen, transactions, output_dir)

    # Print summary
    print_summary(
        {
            "Customers": customers,
            "Accounts": accounts,
            "Transactions": transactions,
            "Credit Cards": credit_cards,
            "Card Transactions": card_transactions,
            "Properties": properties,
            "Loans": loans,
            "Installments": installments,
            "Stocks": stocks,
            "Trades": trades,
            "Fraud Examples": fraud_examples,
        },
        output_dir,
    )


if __name__ == "__main__":
    main()
