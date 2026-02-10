#!/usr/bin/env python3
"""Load generated data to PostgreSQL and Kafka.

This script generates synthetic banking data and loads it to:
- PostgreSQL: Master data (customers, accounts, credit_cards, loans, properties, stocks)
- Kafka: Event streams (transactions, card_transactions, trades, installments)

The data maintains referential integrity across both systems.

Performance modes:
- Default: Uses FinancialDataStore (keeps events in memory) for small datasets.
- --fast: Uses MasterDataStore + COPY + BULK for large datasets (10K+ customers).
"""

import argparse
import logging
import sys
import time
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
from data_gen.models.financial.enums import AccountType, LoanType
from data_gen.sinks.kafka import BULK, KafkaSink, ProducerConfig
from data_gen.sinks.postgres import PostgresSink
from data_gen.store.financial import FinancialDataStore, MasterDataStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def generate_master_data(
    store: FinancialDataStore | MasterDataStore,
    num_customers: int,
    seed: int,
) -> list:
    """Generate all master data and store in data store.

    Parameters
    ----------
    store : FinancialDataStore | MasterDataStore
        Data store to hold generated data.
    num_customers : int
        Number of customers to generate.
    seed : int
        Random seed for reproducibility.

    Returns
    -------
    list
        List of all installments (needed for Kafka streaming).
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

    all_installments = []

    # 1. Generate customers
    t0 = time.perf_counter()
    for _ in range(num_customers):
        customer = customer_gen.generate()
        store.add_customer(customer)
    logger.info("Generated %d customers in %.1fs", num_customers, time.perf_counter() - t0)

    # 2. Generate properties (for housing loans)
    num_properties = max(1, num_customers // 10)
    t0 = time.perf_counter()
    for _ in range(num_properties):
        prop = property_gen.generate()
        store.add_property(prop)
    logger.info("Generated %d properties in %.1fs", num_properties, time.perf_counter() - t0)

    # 3. Generate stocks (B3 reference data)
    t0 = time.perf_counter()
    for stock in stock_gen.generate_all():
        store.add_stock(stock)
    logger.info("Generated %d stocks in %.1fs", len(store.stocks), time.perf_counter() - t0)

    # 4. Generate accounts for each customer
    t0 = time.perf_counter()
    for customer in store.customers.values():
        for account in account_gen.generate_for_customer(
            customer.customer_id,
            customer.created_at,
            customer.monthly_income,
        ):
            store.add_account(account)
    logger.info("Generated %d accounts in %.1fs", len(store.accounts), time.perf_counter() - t0)

    # 5. Generate credit cards (70% of customers)
    t0 = time.perf_counter()
    customers_list = list(store.customers.values())
    num_with_cards = int(len(customers_list) * 0.7)
    for customer in customers_list[:num_with_cards]:
        card = credit_card_gen.generate(customer.customer_id)
        store.add_credit_card(card)
    logger.info("Generated %d credit cards in %.1fs", len(store.credit_cards), time.perf_counter() - t0)

    # 6. Generate loans (30% personal, 5% housing)
    t0 = time.perf_counter()
    properties_list = list(store.properties.values())
    property_idx = 0

    # Personal loans for 30% of customers
    num_personal_loans = int(len(customers_list) * 0.3)
    for customer in customers_list[:num_personal_loans]:
        loan, installments = loan_gen.generate_with_installments(
            customer_id=customer.customer_id,
            loan_type=LoanType.PERSONAL,
        )
        store.add_loan(loan)

        # Apply payment behavior
        modified = payment_behavior.apply_payment_behavior(
            installments,
            on_time_rate=0.85,
            late_rate=0.10,
            default_rate=0.05,
        )
        if isinstance(store, FinancialDataStore):
            for inst in modified:
                store.add_installment(inst)
        all_installments.extend(modified)

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
            loan_type=LoanType.HOUSING,
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
        if isinstance(store, FinancialDataStore):
            for inst in modified:
                store.add_installment(inst)
        all_installments.extend(modified)

    logger.info("Generated %d loans + %d installments in %.1fs",
                len(store.loans), len(all_installments), time.perf_counter() - t0)
    logger.info("Master data generation complete")

    return all_installments


def _load_table(
    connection_string: str,
    entity_type: str,
    records: list,
) -> tuple[str, int, float]:
    """Load a single table using its own connection (for parallel loading).

    Returns
    -------
    tuple[str, int, float]
        (entity_type, row_count, elapsed_seconds).
    """
    sink = PostgresSink(connection_string)
    try:
        sink.disable_constraints()
        t0 = time.perf_counter()
        sink.write_batch(entity_type, records, use_copy=True)
        elapsed = time.perf_counter() - t0
        sink.enable_constraints()
        return entity_type, len(records), elapsed
    finally:
        sink.close()


def load_to_postgres(
    store: FinancialDataStore | MasterDataStore,
    connection_string: str,
    truncate: bool = False,
    use_copy: bool = False,
) -> None:
    """Load master data to PostgreSQL.

    Parameters
    ----------
    store : FinancialDataStore | MasterDataStore
        Data store with generated data.
    connection_string : str
        PostgreSQL connection string.
    truncate : bool
        If True, truncate tables before inserting (default: False).
    use_copy : bool
        If True, use COPY protocol for faster bulk loading (default: False).
    """
    logger.info("Loading master data to PostgreSQL (COPY=%s, parallel=%s)...", use_copy, use_copy)

    sink = PostgresSink(connection_string)

    try:
        sink.create_tables()

        if truncate:
            sink.truncate_tables()

        if use_copy:
            # Parallel loading: each table gets its own connection + COPY stream.
            # FK constraints are disabled per-session so order doesn't matter.
            from concurrent.futures import ThreadPoolExecutor, as_completed

            tables = {
                "customers": list(store.customers.values()),
                "properties": list(store.properties.values()),
                "stocks": list(store.stocks.values()),
                "accounts": list(store.accounts.values()),
                "credit_cards": list(store.credit_cards.values()),
                "loans": list(store.loans.values()),
            }

            t0 = time.perf_counter()
            total = 0

            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = {
                    executor.submit(_load_table, connection_string, entity, records): entity
                    for entity, records in tables.items()
                }
                for future in as_completed(futures):
                    entity, count, elapsed = future.result()
                    total += count
                    logger.info(
                        "  %s: %d rows in %.1fs (%.0f rows/sec)",
                        entity, count, elapsed, count / max(elapsed, 0.001),
                    )

            overall = time.perf_counter() - t0
            logger.info(
                "PostgreSQL load complete: %d rows in %.1fs (%.0f rows/sec)",
                total, overall, total / max(overall, 0.001),
            )
        else:
            t0 = time.perf_counter()

            # Sequential load in FK order
            sink.write_batch("customers", list(store.customers.values()), use_copy=False)
            sink.write_batch("properties", list(store.properties.values()), use_copy=False)
            sink.write_batch("stocks", list(store.stocks.values()), use_copy=False)
            sink.write_batch("accounts", list(store.accounts.values()), use_copy=False)
            sink.write_batch("credit_cards", list(store.credit_cards.values()), use_copy=False)
            sink.write_batch("loans", list(store.loans.values()), use_copy=False)

            elapsed = time.perf_counter() - t0
            total = (len(store.customers) + len(store.properties) + len(store.stocks)
                     + len(store.accounts) + len(store.credit_cards) + len(store.loans))
            logger.info("PostgreSQL load complete: %d rows in %.1fs (%.0f rows/sec)",
                         total, elapsed, total / max(elapsed, 0.001))
    finally:
        sink.close()


def load_to_kafka_streaming(
    store: MasterDataStore,
    bootstrap_servers: str,
    schema_registry_url: str | None,
    seed: int,
    all_installments: list,
    use_cloudevents: bool = True,
) -> None:
    """Generate and stream event data to Kafka without storing in memory.

    Parameters
    ----------
    store : MasterDataStore
        Master data store (for FK references).
    bootstrap_servers : str
        Kafka bootstrap servers.
    schema_registry_url : str | None
        Schema Registry URL (optional, for Avro serialization).
    seed : int
        Random seed for reproducibility.
    all_installments : list
        Pre-generated installments to send.
    use_cloudevents : bool
        If True, include CloudEvents headers (default: True).
    """
    logger.info("Streaming event data to Kafka (BULK mode)...")

    # Initialize generators
    transaction_gen = TransactionGenerator(seed=seed)
    credit_card_gen = CreditCardGenerator(seed=seed)
    trade_gen = TradeGenerator(seed=seed)

    # Create Kafka sink with BULK preset
    config = ProducerConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=schema_registry_url,
        acks=BULK.acks,
        batch_size=BULK.batch_size,
        linger_ms=BULK.linger_ms,
        compression=BULK.compression,
    )
    sink = KafkaSink(config, use_cloudevents=use_cloudevents, poll_interval=10000)

    try:
        # Generate and send transactions (streaming — not stored in memory)
        t0 = time.perf_counter()
        accounts_list = list(store.accounts.values())
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 12, 31)

        transaction_count = 0
        for account in accounts_list:
            for tx in transaction_gen.generate_for_account(
                account, store, start_date, end_date, avg_transactions_per_day=0.3
            ):
                sink.send("banking.transactions", tx)
                transaction_count += 1

        elapsed = time.perf_counter() - t0
        store.count_event("transactions", transaction_count)
        logger.info("Sent %d transactions in %.1fs (%.0f/sec)",
                     transaction_count, elapsed, transaction_count / max(elapsed, 0.001))

        # Generate and send card transactions
        t0 = time.perf_counter()
        cards_list = list(store.credit_cards.values())
        card_start = datetime(2024, 1, 1)
        card_end = datetime(2024, 12, 31)

        card_tx_count = 0
        for card in cards_list:
            for card_tx in credit_card_gen.generate_transactions(
                card, card_start, card_end, avg_transactions_per_day=0.5
            ):
                sink.send("banking.card-transactions", card_tx)
                card_tx_count += 1

        elapsed = time.perf_counter() - t0
        store.count_event("card_transactions", card_tx_count)
        logger.info("Sent %d card transactions in %.1fs (%.0f/sec)",
                     card_tx_count, elapsed, card_tx_count / max(elapsed, 0.001))

        # Generate and send trades
        t0 = time.perf_counter()
        investment_accounts = [
            a for a in accounts_list if a.account_type == AccountType.INVESTIMENTOS
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
                sink.send("banking.trades", trade)
                trade_count += 1

        elapsed = time.perf_counter() - t0
        store.count_event("trades", trade_count)
        logger.info("Sent %d trades in %.1fs (%.0f/sec)",
                     trade_count, elapsed, trade_count / max(elapsed, 0.001))

        # Send installments
        t0 = time.perf_counter()
        for inst in all_installments:
            sink.send("banking.installments", inst)

        elapsed = time.perf_counter() - t0
        store.count_event("installments", len(all_installments))
        logger.info("Sent %d installments in %.1fs (%.0f/sec)",
                     len(all_installments), elapsed, len(all_installments) / max(elapsed, 0.001))

        sink.flush()
        logger.info("Kafka streaming complete")
    finally:
        sink.close()


def load_to_kafka(
    store: FinancialDataStore,
    bootstrap_servers: str,
    schema_registry_url: str | None,
    seed: int,
    use_cloudevents: bool = True,
) -> None:
    """Generate and load event data to Kafka (legacy mode — stores in memory).

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
            a for a in accounts_list if a.account_type == AccountType.INVESTIMENTOS
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


def create_kafka_topics(
    bootstrap_servers: str,
    retention_hours: int = 168,
) -> None:
    """Create Kafka topics if they don't exist.

    Parameters
    ----------
    bootstrap_servers : str
        Kafka bootstrap servers.
    retention_hours : int
        Per-topic retention in hours (default: 168 = 7 days).
        Use 24 for bulk test loads to save disk space.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    retention_ms = str(retention_hours * 3600 * 1000)
    topic_config = {"retention.ms": retention_ms}

    topics = [
        NewTopic("banking.transactions", num_partitions=3, replication_factor=1, config=topic_config),
        NewTopic("banking.card-transactions", num_partitions=3, replication_factor=1, config=topic_config),
        NewTopic("banking.trades", num_partitions=3, replication_factor=1, config=topic_config),
        NewTopic("banking.installments", num_partitions=3, replication_factor=1, config=topic_config),
    ]

    logger.info("Topic retention: %d hours", retention_hours)

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


def _validate_postgres(postgres_url: str, expected: dict[str, int]) -> list[str]:
    """Validate PostgreSQL row counts against expected values.

    Returns
    -------
    list[str]
        List of mismatch descriptions (empty = all OK).
    """
    import psycopg

    pg_tables = {
        "customers": "customers",
        "accounts": "accounts",
        "credit_cards": "credit_cards",
        "loans": "loans",
        "properties": "properties",
        "stocks": "stocks",
    }
    mismatches: list[str] = []
    try:
        with psycopg.connect(postgres_url) as conn:
            with conn.cursor() as cur:
                for key, table in pg_tables.items():
                    cur.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
                    actual = cur.fetchone()[0]
                    exp = expected.get(key, 0)
                    if actual != exp:
                        mismatches.append(f"{table}: expected {exp}, got {actual}")
                    else:
                        logger.info("  [OK] %s: %d rows", table, actual)
    except Exception as e:
        mismatches.append(f"connection failed: {e}")
    return mismatches


KAFKA_TOPIC_MAP: dict[str, str] = {
    "banking.transactions": "transactions",
    "banking.card-transactions": "card_transactions",
    "banking.trades": "trades",
    "banking.installments": "installments",
}


def get_kafka_offsets(bootstrap_servers: str) -> dict[str, int]:
    """Snapshot current high-water-mark offsets for all event topics.

    Returns
    -------
    dict[str, int]
        Mapping of topic name -> total offset across all partitions.
    """
    from confluent_kafka.admin import AdminClient
    from confluent_kafka import TopicPartition, Consumer

    offsets: dict[str, int] = {}
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin.list_topics(timeout=10)
        consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"datagen-offsets-{int(time.time())}",
            "auto.offset.reset": "earliest",
        })
        try:
            for topic_name in KAFKA_TOPIC_MAP:
                if topic_name not in metadata.topics:
                    offsets[topic_name] = 0
                    continue
                total = 0
                for pid in metadata.topics[topic_name].partitions:
                    _, hi = consumer.get_watermark_offsets(
                        TopicPartition(topic_name, pid), timeout=5.0,
                    )
                    total += hi
                offsets[topic_name] = total
        finally:
            consumer.close()
    except Exception:
        logger.debug("Could not snapshot Kafka offsets (topics may not exist yet)")
    return offsets


def _validate_kafka(
    bootstrap_servers: str,
    expected: dict[str, int],
    offsets_before: dict[str, int] | None = None,
) -> list[str]:
    """Validate Kafka topic offsets against expected event counts.

    Compares the delta (current - before) when offsets_before is provided,
    so pre-existing messages in topics don't cause false mismatches.

    Returns
    -------
    list[str]
        List of mismatch descriptions (empty = all OK).
    """
    offsets_after = get_kafka_offsets(bootstrap_servers)
    if not offsets_after:
        return ["connection failed: could not read offsets"]

    if offsets_before is None:
        offsets_before = dict.fromkeys(KAFKA_TOPIC_MAP, 0)

    mismatches: list[str] = []
    for topic_name, key in KAFKA_TOPIC_MAP.items():
        delta = offsets_after.get(topic_name, 0) - offsets_before.get(topic_name, 0)
        exp = expected.get(key, 0)
        if delta != exp:
            mismatches.append(f"{topic_name}: expected {exp}, got {delta}")
        else:
            logger.info("  [OK] %s: %d events", topic_name, delta)
    return mismatches


def _run_validation(
    summary: dict[str, int],
    postgres_url: str | None,
    kafka_bootstrap: str | None,
    kafka_offsets_before: dict[str, int] | None = None,
) -> list[str]:
    """Run PG and/or Kafka validation, log results, return errors."""
    errors: list[str] = []
    if postgres_url:
        logger.info("-" * 60)
        logger.info("Validating PostgreSQL row counts...")
        for err in _validate_postgres(postgres_url, summary):
            logger.error("  [FAIL] %s", err)
            errors.append(err)
    if kafka_bootstrap:
        logger.info("-" * 60)
        logger.info("Validating Kafka topic offsets...")
        for err in _validate_kafka(kafka_bootstrap, summary, kafka_offsets_before):
            logger.error("  [FAIL] %s", err)
            errors.append(err)
    return errors


def print_summary(
    summary: dict[str, int],
    total: int,
    elapsed: float,
    postgres_url: str | None = None,
    kafka_bootstrap: str | None = None,
    kafka_offsets_before: dict[str, int] | None = None,
) -> None:
    """Print load summary and optionally validate against PG/Kafka.

    Parameters
    ----------
    summary : dict[str, int]
        Entity counts from the data store.
    total : int
        Total row count.
    elapsed : float
        Total elapsed time in seconds.
    postgres_url : str | None
        If provided, validate row counts against PostgreSQL.
    kafka_bootstrap : str | None
        If provided, validate offsets against Kafka topics.
    kafka_offsets_before : dict[str, int] | None
        Offsets snapshot taken before producing. Used to compute delta
        so pre-existing messages don't cause false mismatches.
    """
    logger.info("=" * 60)
    logger.info("Load Complete! (%.1fs total)", elapsed)
    logger.info("=" * 60)
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
    logger.info(
        "Total: %d rows in %.1fs (%.0f rows/sec)",
        total, elapsed, total / max(elapsed, 0.001),
    )

    # Validation
    errors = _run_validation(summary, postgres_url, kafka_bootstrap, kafka_offsets_before)
    logger.info("=" * 60)
    if postgres_url or kafka_bootstrap:
        if errors:
            logger.warning("Validation: MISMATCHES DETECTED (see errors above)")
        else:
            logger.info("Validation: ALL OK")
    logger.info("=" * 60)


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
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Use COPY + BULK + streaming for faster loading (recommended for 10K+ customers)",
    )
    parser.add_argument(
        "--no-avro",
        action="store_true",
        help="Disable Avro serialization (use JSON instead, ~3-4x larger messages)",
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=8,
        help="Kafka topic retention in hours (default: 8). Suited for test-and-clean workflows.",
    )

    args = parser.parse_args()

    # Auto-enable fast mode for large datasets
    use_fast = args.fast or args.customers >= 10000

    # Schema registry URL: None disables Avro (falls back to JSON)
    schema_registry = None if args.no_avro else args.schema_registry

    logger.info("=" * 60)
    logger.info("Data Generator - Load to PostgreSQL & Kafka")
    logger.info("=" * 60)
    logger.info("Customers: %d", args.customers)
    logger.info("Seed: %d", args.seed)
    logger.info("Mode: %s", "FAST (COPY+BULK+streaming)" if use_fast else "STANDARD")
    logger.info("Serialization: %s", "JSON" if args.no_avro else "Avro (via Schema Registry)")
    logger.info("PostgreSQL: %s", args.postgres_url if not args.skip_postgres else "SKIPPED")
    logger.info("Kafka: %s", args.kafka_bootstrap if not args.skip_kafka else "SKIPPED")
    logger.info("=" * 60)

    # Snapshot Kafka offsets before producing (for delta validation)
    kafka_offsets_before: dict[str, int] | None = None
    if not args.skip_kafka:
        kafka_offsets_before = get_kafka_offsets(args.kafka_bootstrap)

    overall_start = time.perf_counter()

    if use_fast:
        # Fast mode: MasterDataStore + COPY + BULK + streaming
        store = MasterDataStore()
        all_installments = generate_master_data(store, args.customers, args.seed)

        # Create Kafka topics if requested
        if args.create_topics and not args.skip_kafka:
            create_kafka_topics(args.kafka_bootstrap, retention_hours=args.retention_hours)

        # Load to PostgreSQL with COPY
        if not args.skip_postgres:
            load_to_postgres(store, args.postgres_url, truncate=args.truncate, use_copy=True)

        # Stream to Kafka with BULK
        if not args.skip_kafka:
            load_to_kafka_streaming(
                store,
                args.kafka_bootstrap,
                schema_registry,
                args.seed,
                all_installments,
                use_cloudevents=not args.no_cloudevents,
            )
    else:
        # Standard mode: FinancialDataStore (keeps events in memory)
        store = FinancialDataStore()
        generate_master_data(store, args.customers, args.seed)

        # Create Kafka topics if requested
        if args.create_topics and not args.skip_kafka:
            create_kafka_topics(args.kafka_bootstrap, retention_hours=args.retention_hours)

        # Load to PostgreSQL
        if not args.skip_postgres:
            load_to_postgres(store, args.postgres_url, truncate=args.truncate)

        # Load to Kafka
        if not args.skip_kafka:
            load_to_kafka(
                store,
                args.kafka_bootstrap,
                schema_registry,
                args.seed,
                use_cloudevents=not args.no_cloudevents,
            )

    overall_elapsed = time.perf_counter() - overall_start

    # Print summary
    summary = store.summary()
    total = sum(summary.values())
    print_summary(
        summary,
        total,
        overall_elapsed,
        postgres_url=args.postgres_url if not args.skip_postgres else None,
        kafka_bootstrap=args.kafka_bootstrap if not args.skip_kafka else None,
        kafka_offsets_before=kafka_offsets_before,
    )


if __name__ == "__main__":
    main()
