#!/usr/bin/env python3
"""Parallel data generation and loading.

Uses multiprocessing to parallelise event generation across CPU cores.
Master data is generated single-threaded (shared state), then event
generation is split across workers, each with its own Kafka producer.

Usage:
    python scripts/load_data_parallel.py --customers 100000
    python scripts/load_data_parallel.py --customers 1000000 --workers 8
"""

import argparse
import logging
import multiprocessing as mp
import os
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
from data_gen.store.financial import MasterDataStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def generate_master_data(num_customers: int, seed: int) -> tuple[MasterDataStore, list]:
    """Generate all master data in the main process.

    Parameters
    ----------
    num_customers : int
        Number of customers to generate.
    seed : int
        Random seed for reproducibility.

    Returns
    -------
    tuple[MasterDataStore, list]
        The populated store and list of all installments.
    """
    store = MasterDataStore()
    payment_behavior = PaymentBehavior(seed=seed)

    # Initialize generators
    customer_gen = CustomerGenerator(seed=seed)
    account_gen = AccountGenerator(seed=seed)
    credit_card_gen = CreditCardGenerator(seed=seed)
    loan_gen = LoanGenerator(seed=seed)
    property_gen = PropertyGenerator(seed=seed)
    stock_gen = StockGenerator(seed=seed)

    all_installments = []

    # Customers
    t0 = time.perf_counter()
    for _ in range(num_customers):
        store.add_customer(customer_gen.generate())
    logger.info("Generated %d customers in %.1fs", num_customers, time.perf_counter() - t0)

    # Properties
    num_properties = max(1, num_customers // 10)
    t0 = time.perf_counter()
    for _ in range(num_properties):
        store.add_property(property_gen.generate())
    logger.info("Generated %d properties in %.1fs", num_properties, time.perf_counter() - t0)

    # Stocks
    t0 = time.perf_counter()
    for stock in stock_gen.generate_all():
        store.add_stock(stock)
    logger.info("Generated %d stocks in %.1fs", len(store.stocks), time.perf_counter() - t0)

    # Accounts
    t0 = time.perf_counter()
    for customer in store.customers.values():
        for account in account_gen.generate_for_customer(
            customer.customer_id, customer.created_at, customer.monthly_income
        ):
            store.add_account(account)
    logger.info("Generated %d accounts in %.1fs", len(store.accounts), time.perf_counter() - t0)

    # Credit cards (70%)
    t0 = time.perf_counter()
    customers_list = list(store.customers.values())
    num_with_cards = int(len(customers_list) * 0.7)
    for customer in customers_list[:num_with_cards]:
        store.add_credit_card(credit_card_gen.generate(customer.customer_id))
    logger.info(
        "Generated %d credit cards in %.1fs", len(store.credit_cards), time.perf_counter() - t0
    )

    # Loans
    t0 = time.perf_counter()
    properties_list = list(store.properties.values())
    property_idx = 0

    # Personal loans (30%)
    num_personal = int(len(customers_list) * 0.3)
    for customer in customers_list[:num_personal]:
        loan, installments = loan_gen.generate_with_installments(
            customer_id=customer.customer_id,
            loan_type=LoanType.PERSONAL,
        )
        store.add_loan(loan)
        modified = payment_behavior.apply_payment_behavior(
            installments, on_time_rate=0.85, late_rate=0.10, default_rate=0.05
        )
        all_installments.extend(modified)

    # Housing loans (5%)
    sorted_by_income = sorted(customers_list, key=lambda c: c.monthly_income, reverse=True)
    num_housing = int(len(customers_list) * 0.05)
    for customer in sorted_by_income[:num_housing]:
        if property_idx >= len(properties_list):
            break
        loan, installments = loan_gen.generate_with_installments(
            customer_id=customer.customer_id,
            loan_type=LoanType.HOUSING,
            property_id=properties_list[property_idx].property_id,
        )
        store.add_loan(loan)
        property_idx += 1
        modified = payment_behavior.apply_payment_behavior(
            installments[:24], on_time_rate=0.95, late_rate=0.04, default_rate=0.01
        )
        all_installments.extend(modified)

    logger.info(
        "Generated %d loans + %d installments in %.1fs",
        len(store.loans),
        len(all_installments),
        time.perf_counter() - t0,
    )

    return store, all_installments


def _worker_generate_events(
    worker_id: int,
    accounts_chunk: list,
    cards_chunk: list,
    invest_accounts_chunk: list,
    stocks_list: list,
    store_accounts_dict: dict,
    store_credit_cards_dict: dict,
    bootstrap_servers: str,
    schema_registry_url: str | None,
    seed: int,
    use_cloudevents: bool,
) -> dict[str, int]:
    """Worker function: generate events for a chunk of accounts/cards and send to Kafka.

    Parameters
    ----------
    worker_id : int
        Worker identifier for logging.
    accounts_chunk : list
        Accounts assigned to this worker for transaction generation.
    cards_chunk : list
        Cards assigned to this worker for card transaction generation.
    invest_accounts_chunk : list
        Investment accounts assigned to this worker for trade generation.
    stocks_list : list
        All stocks (shared read-only).
    store_accounts_dict : dict
        Accounts dict for counterparty lookups (passed as dict for pickling).
    store_credit_cards_dict : dict
        Credit cards dict (passed as dict for pickling).
    bootstrap_servers : str
        Kafka bootstrap servers.
    schema_registry_url : str | None
        Schema Registry URL.
    seed : int
        Base seed (worker adds worker_id offset).
    use_cloudevents : bool
        Whether to use CloudEvents headers.

    Returns
    -------
    dict[str, int]
        Counts of events generated per type.
    """
    # Each worker gets a unique seed for different data
    worker_seed = seed + worker_id + 1

    # Rebuild a minimal MasterDataStore for FK lookups
    mini_store = MasterDataStore()
    mini_store.accounts = store_accounts_dict
    mini_store.credit_cards = store_credit_cards_dict

    # Initialize generators with worker-specific seed
    transaction_gen = TransactionGenerator(seed=worker_seed)
    card_gen = CreditCardGenerator(seed=worker_seed)
    trade_gen = TradeGenerator(seed=worker_seed)

    # Create per-worker Kafka producer
    config = ProducerConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=schema_registry_url,
        acks=BULK.acks,
        batch_size=BULK.batch_size,
        linger_ms=BULK.linger_ms,
        compression=BULK.compression,
    )
    sink = KafkaSink(config, use_cloudevents=use_cloudevents, poll_interval=10000)

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    counts = {"transactions": 0, "card_transactions": 0, "trades": 0}

    try:
        # Transactions
        for account in accounts_chunk:
            for tx in transaction_gen.generate_for_account(
                account, mini_store, start_date, end_date, avg_transactions_per_day=0.3
            ):
                sink.send("banking.transactions", tx)
                counts["transactions"] += 1

        # Card transactions
        for card in cards_chunk:
            for card_tx in card_gen.generate_transactions(
                card, start_date, end_date, avg_transactions_per_day=0.5
            ):
                sink.send("banking.card-transactions", card_tx)
                counts["card_transactions"] += 1

        # Trades
        for account in invest_accounts_chunk:
            trades = trade_gen.generate_trades_for_account(
                account_id=account.account_id,
                stocks=stocks_list,
                num_trades=20,
            )
            for trade in trades:
                sink.send("banking.trades", trade)
                counts["trades"] += 1

        sink.flush()
    finally:
        sink.close()

    logger.info(
        "Worker %d done: %d tx, %d card_tx, %d trades",
        worker_id,
        counts["transactions"],
        counts["card_transactions"],
        counts["trades"],
    )
    return counts


def _chunk_list(lst: list, n: int) -> list[list]:
    """Split a list into n roughly equal chunks.

    Parameters
    ----------
    lst : list
        List to split.
    n : int
        Number of chunks.

    Returns
    -------
    list[list]
        List of chunks.
    """
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


def create_kafka_topics(
    bootstrap_servers: str,
    retention_hours: int = 8,
) -> None:
    """Create Kafka topics if they don't exist.

    Parameters
    ----------
    bootstrap_servers : str
        Kafka bootstrap servers.
    retention_hours : int
        Per-topic retention in hours (default: 8).
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    retention_ms = str(retention_hours * 3600 * 1000)
    topic_config = {"retention.ms": retention_ms}

    topics = [
        NewTopic("banking.transactions", num_partitions=6, replication_factor=1, config=topic_config),
        NewTopic("banking.card-transactions", num_partitions=6, replication_factor=1, config=topic_config),
        NewTopic("banking.trades", num_partitions=3, replication_factor=1, config=topic_config),
        NewTopic("banking.installments", num_partitions=3, replication_factor=1, config=topic_config),
    ]

    logger.info("Topic retention: %d hours", retention_hours)

    existing = admin.list_topics(timeout=10).topics
    to_create = [t for t in topics if t.topic not in existing]

    if to_create:
        futures = admin.create_topics(to_create)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info("Created topic: %s", topic)
            except Exception as e:
                logger.warning("Failed to create topic %s: %s", topic, e)
    else:
        logger.info("All topics already exist")


def _load_master_to_postgres(
    store: MasterDataStore, connection_string: str, truncate: bool
) -> None:
    """Load master data to PostgreSQL using COPY protocol."""
    logger.info("[Phase 2] Loading master data to PostgreSQL (COPY)...")
    pg_sink = PostgresSink(connection_string)
    try:
        pg_sink.create_tables()
        if truncate:
            pg_sink.truncate_tables()
        pg_sink.disable_constraints()

        t0 = time.perf_counter()
        pg_sink.write_batch("customers", list(store.customers.values()), use_copy=True)
        pg_sink.write_batch("properties", list(store.properties.values()), use_copy=True)
        pg_sink.write_batch("stocks", list(store.stocks.values()), use_copy=True)
        pg_sink.write_batch("accounts", list(store.accounts.values()), use_copy=True)
        pg_sink.write_batch("credit_cards", list(store.credit_cards.values()), use_copy=True)
        pg_sink.write_batch("loans", list(store.loans.values()), use_copy=True)

        pg_elapsed = time.perf_counter() - t0
        pg_total = (
            len(store.customers)
            + len(store.properties)
            + len(store.stocks)
            + len(store.accounts)
            + len(store.credit_cards)
            + len(store.loans)
        )
        logger.info(
            "PostgreSQL COPY complete: %d rows in %.1fs (%.0f rows/sec)",
            pg_total,
            pg_elapsed,
            pg_total / max(pg_elapsed, 0.001),
        )
        pg_sink.enable_constraints()
    finally:
        pg_sink.close()


def main() -> None:
    """Run parallel data generation and loading."""
    parser = argparse.ArgumentParser(description="Parallel data-gen loader")
    parser.add_argument(
        "--customers", type=int, default=10000, help="Number of customers (default: 10000)"
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Number of worker processes (default: CPU count)",
    )
    parser.add_argument(
        "--postgres-url",
        type=str,
        default="postgresql://postgres:postgres@localhost:5432/datagen",
        help="PostgreSQL connection string",
    )
    parser.add_argument(
        "--kafka-bootstrap", type=str, default="localhost:9092", help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--schema-registry",
        type=str,
        default="http://localhost:8081",
        help="Schema Registry URL",
    )
    parser.add_argument("--skip-postgres", action="store_true", help="Skip PostgreSQL loading")
    parser.add_argument("--skip-kafka", action="store_true", help="Skip Kafka loading")
    parser.add_argument("--create-topics", action="store_true", help="Create Kafka topics first")
    parser.add_argument("--truncate", action="store_true", help="Truncate PG tables first")
    parser.add_argument(
        "--no-cloudevents", action="store_true", help="Disable CloudEvents headers"
    )
    parser.add_argument(
        "--no-avro", action="store_true", help="Disable Avro serialization (use JSON instead)"
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=8,
        help="Kafka topic retention in hours (default: 8). Suited for test-and-clean workflows.",
    )
    args = parser.parse_args()

    num_workers = args.workers or os.cpu_count() or 4

    # Schema registry URL: None disables Avro (falls back to JSON)
    schema_registry = None if args.no_avro else args.schema_registry

    logger.info("=" * 60)
    logger.info("Parallel Data Generator")
    logger.info("=" * 60)
    logger.info("Customers: %d", args.customers)
    logger.info("Workers: %d", num_workers)
    logger.info("Seed: %d", args.seed)
    logger.info("Serialization: %s", "JSON" if args.no_avro else "Avro (via Schema Registry)")
    logger.info("=" * 60)

    # Snapshot Kafka offsets before producing (for delta validation)
    kafka_offsets_before: dict[str, int] | None = None
    if not args.skip_kafka:
        from scripts.load_data import get_kafka_offsets
        kafka_offsets_before = get_kafka_offsets(args.kafka_bootstrap)

    overall_start = time.perf_counter()

    # Phase 1: Generate master data (single-threaded)
    logger.info("[Phase 1] Generating master data...")
    store, all_installments = generate_master_data(args.customers, args.seed)

    # Phase 2: Create topics
    if args.create_topics and not args.skip_kafka:
        create_kafka_topics(args.kafka_bootstrap, retention_hours=args.retention_hours)

    # Phase 3: Load master data to PostgreSQL (COPY)
    if not args.skip_postgres:
        _load_master_to_postgres(store, args.postgres_url, args.truncate)

    # Phase 4: Parallel event generation + Kafka streaming
    if not args.skip_kafka:
        logger.info("[Phase 3] Streaming events to Kafka (%d workers)...", num_workers)

        accounts_list = list(store.accounts.values())
        cards_list = list(store.credit_cards.values())
        invest_accounts = [
            a for a in accounts_list if a.account_type == AccountType.INVESTIMENTOS
        ]
        stocks_list = list(store.stocks.values())

        # Chunk workloads
        accounts_chunks = _chunk_list(accounts_list, num_workers)
        cards_chunks = _chunk_list(cards_list, num_workers)
        invest_chunks = _chunk_list(invest_accounts, num_workers)

        # Serializable dicts for workers
        accounts_dict = dict(store.accounts)
        cards_dict = dict(store.credit_cards)

        # First send installments from main process (already generated)
        logger.info("Sending %d installments from main process...", len(all_installments))
        config = ProducerConfig(
            bootstrap_servers=args.kafka_bootstrap,
            schema_registry_url=schema_registry,
            acks=BULK.acks,
            batch_size=BULK.batch_size,
            linger_ms=BULK.linger_ms,
            compression=BULK.compression,
        )
        inst_sink = KafkaSink(
            config, use_cloudevents=not args.no_cloudevents, poll_interval=10000
        )
        t0 = time.perf_counter()
        for inst in all_installments:
            inst_sink.send("banking.installments", inst)
        inst_sink.flush()
        inst_sink.close()
        inst_elapsed = time.perf_counter() - t0
        logger.info(
            "Sent %d installments in %.1fs (%.0f/sec)",
            len(all_installments),
            inst_elapsed,
            len(all_installments) / max(inst_elapsed, 0.001),
        )

        # Launch workers for transactions, card_transactions, trades
        t0 = time.perf_counter()
        worker_args = [
            (
                i,
                accounts_chunks[i],
                cards_chunks[i],
                invest_chunks[i],
                stocks_list,
                accounts_dict,
                cards_dict,
                args.kafka_bootstrap,
                schema_registry,
                args.seed,
                not args.no_cloudevents,
            )
            for i in range(num_workers)
        ]

        with mp.Pool(processes=num_workers) as pool:
            results = pool.starmap(_worker_generate_events, worker_args)

        kafka_elapsed = time.perf_counter() - t0

        # Aggregate counts
        total_counts = {"transactions": 0, "card_transactions": 0, "trades": 0}
        for r in results:
            for k, v in r.items():
                total_counts[k] += v

        store.count_event("transactions", total_counts["transactions"])
        store.count_event("card_transactions", total_counts["card_transactions"])
        store.count_event("trades", total_counts["trades"])
        store.count_event("installments", len(all_installments))

        total_events = sum(total_counts.values()) + len(all_installments)
        logger.info(
            "Kafka streaming complete: %d events in %.1fs (%.0f events/sec)",
            total_events,
            kafka_elapsed + inst_elapsed,
            total_events / max(kafka_elapsed + inst_elapsed, 0.001),
        )

    # Summary
    overall_elapsed = time.perf_counter() - overall_start
    summary = store.summary()
    total = sum(summary.values())

    from scripts.load_data import print_summary

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
