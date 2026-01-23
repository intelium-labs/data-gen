#!/usr/bin/env python3
"""Load generated data to PostgreSQL and Kafka.

This script generates synthetic banking data and loads it to:
- PostgreSQL: Master data (customers, accounts, credit_cards, loans, properties, stocks)
- Kafka: Event streams (transactions, card_transactions, trades, installments)

The data maintains referential integrity across both systems.
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

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
from data_gen.generators.financial.patterns import PaymentBehavior
from data_gen.sinks.kafka import KafkaSink, ProducerConfig
from data_gen.sinks.postgres import PostgresSink
from data_gen.store.financial import FinancialDataStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def generate_master_data(
    store: FinancialDataStore,
    num_customers: int,
    seed: int,
) -> None:
    """Generate all master data and store in FinancialDataStore.

    Parameters
    ----------
    store : FinancialDataStore
        Data store to hold generated data.
    num_customers : int
        Number of customers to generate.
    seed : int
        Random seed for reproducibility.
    """
    logger.info("Generating master data for %d customers...", num_customers)

    # Initialize generators
    customer_gen = CustomerGenerator(seed=seed)
    account_gen = AccountGenerator(seed=seed)
    credit_card_gen = CreditCardGenerator(seed=seed)
    loan_gen = LoanGenerator(seed=seed)
    property_gen = PropertyGenerator(seed=seed)
    stock_gen = StockGenerator(seed=seed)
    payment_behavior = PaymentBehavior(seed=seed)

    # 1. Generate customers
    logger.info("Generating customers...")
    for _ in range(num_customers):
        customer = customer_gen.generate()
        store.add_customer(customer)

    # 2. Generate properties (for housing loans)
    logger.info("Generating properties...")
    num_properties = max(1, num_customers // 10)
    for _ in range(num_properties):
        prop = property_gen.generate()
        store.add_property(prop)

    # 3. Generate stocks (B3 reference data)
    logger.info("Generating stocks...")
    for stock in stock_gen.generate_all():
        store.add_stock(stock)

    # 4. Generate accounts for each customer
    logger.info("Generating accounts...")
    for customer in store.customers.values():
        for account in account_gen.generate_for_customer(
            customer.customer_id,
            customer.created_at,
            customer.monthly_income,
        ):
            store.add_account(account)

    # 5. Generate credit cards (70% of customers)
    logger.info("Generating credit cards...")
    customers_list = list(store.customers.values())
    num_with_cards = int(len(customers_list) * 0.7)
    for customer in customers_list[:num_with_cards]:
        card = credit_card_gen.generate(customer.customer_id)
        store.add_credit_card(card)

    # 6. Generate loans (30% personal, 5% housing)
    logger.info("Generating loans...")
    properties_list = list(store.properties.values())
    property_idx = 0

    # Personal loans for 30% of customers
    num_personal_loans = int(len(customers_list) * 0.3)
    for customer in customers_list[:num_personal_loans]:
        loan, installments = loan_gen.generate_with_installments(
            customer_id=customer.customer_id,
            loan_type="PERSONAL",
        )
        store.add_loan(loan)

        # Apply payment behavior
        modified = payment_behavior.apply_payment_behavior(
            installments,
            on_time_rate=0.85,
            late_rate=0.10,
            default_rate=0.05,
        )
        for inst in modified:
            store.add_installment(inst)

    # Housing loans for 5% of highest income customers
    sorted_by_income = sorted(
        customers_list,
        key=lambda c: c.monthly_income,
        reverse=True,
    )
    num_housing_loans = int(len(customers_list) * 0.05)
    for customer in sorted_by_income[:num_housing_loans]:
        if property_idx >= len(properties_list):
            break
        loan, installments = loan_gen.generate_with_installments(
            customer_id=customer.customer_id,
            loan_type="HOUSING",
            property_id=properties_list[property_idx].property_id,
        )
        store.add_loan(loan)
        property_idx += 1

        # Housing loans typically have better payment behavior
        modified = payment_behavior.apply_payment_behavior(
            installments[:24],  # First 2 years
            on_time_rate=0.95,
            late_rate=0.04,
            default_rate=0.01,
        )
        for inst in modified:
            store.add_installment(inst)

    logger.info("Master data generation complete: %s", store.summary())


def load_to_postgres(
    store: FinancialDataStore,
    connection_string: str,
    truncate: bool = False,
) -> None:
    """Load master data to PostgreSQL.

    Parameters
    ----------
    store : FinancialDataStore
        Data store with generated data.
    connection_string : str
        PostgreSQL connection string.
    truncate : bool
        If True, truncate tables before inserting (default: False).
    """
    logger.info("Loading master data to PostgreSQL...")

    sink = PostgresSink(connection_string)

    try:
        # Create tables
        sink.create_tables()

        # Truncate tables if requested
        if truncate:
            sink.truncate_tables()

        # Load in FK order
        sink.write_batch("customers", list(store.customers.values()))
        sink.write_batch("properties", list(store.properties.values()))
        sink.write_batch("stocks", list(store.stocks.values()))
        sink.write_batch("accounts", list(store.accounts.values()))
        sink.write_batch("credit_cards", list(store.credit_cards.values()))
        sink.write_batch("loans", list(store.loans.values()))

        logger.info("PostgreSQL load complete")
    finally:
        sink.close()


def load_to_kafka(
    store: FinancialDataStore,
    bootstrap_servers: str,
    schema_registry_url: str | None,
    seed: int,
    use_cloudevents: bool = True,
) -> None:
    """Generate and load event data to Kafka.

    Parameters
    ----------
    store : FinancialDataStore
        Data store with master data (for FK references).
    bootstrap_servers : str
        Kafka bootstrap servers.
    schema_registry_url : str | None
        Schema Registry URL (optional, for Avro serialization).
    seed : int
        Random seed for reproducibility.
    use_cloudevents : bool
        If True, include CloudEvents headers in messages (default: True).
    """
    logger.info("Loading event data to Kafka...")
    if use_cloudevents:
        logger.info("CloudEvents headers: ENABLED")
    else:
        logger.info("CloudEvents headers: DISABLED")

    # Initialize generators
    transaction_gen = TransactionGenerator(seed=seed)
    credit_card_gen = CreditCardGenerator(seed=seed)
    trade_gen = TradeGenerator(seed=seed)

    # Create Kafka sink
    config = ProducerConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=schema_registry_url,
    )
    sink = KafkaSink(config, use_cloudevents=use_cloudevents)

    try:
        # Generate and send transactions
        logger.info("Generating transactions...")
        accounts_list = list(store.accounts.values())
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 12, 31)

        transaction_count = 0
        for account in accounts_list:
            for tx in transaction_gen.generate_for_account(
                account, store, start_date, end_date, avg_transactions_per_day=0.3
            ):
                store.add_transaction(tx)
                sink.send("banking.transactions", tx)
                transaction_count += 1

        logger.info("Sent %d transactions to Kafka", transaction_count)

        # Generate and send card transactions
        logger.info("Generating card transactions...")
        cards_list = list(store.credit_cards.values())
        card_start = datetime(2024, 1, 1)
        card_end = datetime(2024, 12, 31)

        card_tx_count = 0
        for card in cards_list:
            for card_tx in credit_card_gen.generate_transactions(
                card, card_start, card_end, avg_transactions_per_day=0.5
            ):
                store.add_card_transaction(card_tx)
                sink.send("banking.card-transactions", card_tx)
                card_tx_count += 1

        logger.info("Sent %d card transactions to Kafka", card_tx_count)

        # Generate and send trades
        logger.info("Generating trades...")
        investment_accounts = [
            a for a in accounts_list if a.account_type == "INVESTIMENTOS"
        ]
        stocks_list = list(store.stocks.values())

        trade_count = 0
        for account in investment_accounts:
            trades = trade_gen.generate_trades_for_account(
                account_id=account.account_id,
                stocks=stocks_list,
                num_trades=20,
            )
            for trade in trades:
                store.add_trade(trade)
                sink.send("banking.trades", trade)
                trade_count += 1

        logger.info("Sent %d trades to Kafka", trade_count)

        # Send installments
        logger.info("Sending installments...")
        installments_list = store.installments
        for inst in installments_list:
            sink.send("banking.installments", inst)

        logger.info("Sent %d installments to Kafka", len(installments_list))

        sink.flush()
        logger.info("Kafka load complete")
    finally:
        sink.close()


def create_kafka_topics(bootstrap_servers: str) -> None:
    """Create Kafka topics if they don't exist.

    Parameters
    ----------
    bootstrap_servers : str
        Kafka bootstrap servers.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    topics = [
        NewTopic("banking.transactions", num_partitions=3, replication_factor=1),
        NewTopic("banking.card-transactions", num_partitions=3, replication_factor=1),
        NewTopic("banking.trades", num_partitions=3, replication_factor=1),
        NewTopic("banking.installments", num_partitions=3, replication_factor=1),
    ]

    # Check existing topics
    existing = admin.list_topics(timeout=10).topics
    topics_to_create = [t for t in topics if t.topic not in existing]

    if topics_to_create:
        futures = admin.create_topics(topics_to_create)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info("Created topic: %s", topic)
            except Exception as e:
                logger.warning("Failed to create topic %s: %s", topic, e)
    else:
        logger.info("All topics already exist")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load generated data to PostgreSQL and Kafka"
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=100,
        help="Number of customers to generate (default: 100)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--postgres-url",
        type=str,
        default="postgresql://postgres:postgres@localhost:5432/datagen",
        help="PostgreSQL connection string",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--schema-registry",
        type=str,
        default="http://localhost:8081",
        help="Schema Registry URL (optional)",
    )
    parser.add_argument(
        "--skip-postgres",
        action="store_true",
        help="Skip PostgreSQL loading",
    )
    parser.add_argument(
        "--skip-kafka",
        action="store_true",
        help="Skip Kafka loading",
    )
    parser.add_argument(
        "--create-topics",
        action="store_true",
        help="Create Kafka topics before loading",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate PostgreSQL tables before loading (allows re-running)",
    )
    parser.add_argument(
        "--no-cloudevents",
        action="store_true",
        help="Disable CloudEvents headers in Kafka messages",
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Data Generator - Load to PostgreSQL & Kafka")
    logger.info("=" * 60)
    logger.info("Customers: %d", args.customers)
    logger.info("Seed: %d", args.seed)
    logger.info("PostgreSQL: %s", args.postgres_url if not args.skip_postgres else "SKIPPED")
    logger.info("Kafka: %s", args.kafka_bootstrap if not args.skip_kafka else "SKIPPED")
    logger.info("=" * 60)

    # Initialize store
    store = FinancialDataStore()

    # Generate master data
    generate_master_data(store, args.customers, args.seed)

    # Create Kafka topics if requested
    if args.create_topics and not args.skip_kafka:
        create_kafka_topics(args.kafka_bootstrap)

    # Load to PostgreSQL
    if not args.skip_postgres:
        load_to_postgres(store, args.postgres_url, truncate=args.truncate)

    # Load to Kafka
    if not args.skip_kafka:
        load_to_kafka(
            store,
            args.kafka_bootstrap,
            args.schema_registry,
            args.seed,
            use_cloudevents=not args.no_cloudevents,
        )

    # Print summary
    logger.info("=" * 60)
    logger.info("Load Complete!")
    logger.info("=" * 60)
    summary = store.summary()
    logger.info("PostgreSQL (Master Data):")
    logger.info("  - Customers: %d", summary["customers"])
    logger.info("  - Accounts: %d", summary["accounts"])
    logger.info("  - Credit Cards: %d", summary["credit_cards"])
    logger.info("  - Loans: %d", summary["loans"])
    logger.info("  - Properties: %d", summary["properties"])
    logger.info("  - Stocks: %d", summary["stocks"])
    logger.info("Kafka (Event Streams):")
    logger.info("  - Transactions: %d", summary["transactions"])
    logger.info("  - Card Transactions: %d", summary["card_transactions"])
    logger.info("  - Trades: %d", summary["trades"])
    logger.info("  - Installments: %d", summary["installments"])
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
