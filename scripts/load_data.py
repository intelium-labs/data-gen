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
import itertools
import logging
import random
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data_gen.generators.financial import (
    AccountGenerator,
    CardTransactionGenerator,
    CreditCardGenerator,
    CustomerGenerator,
    LoanGenerator,
    StockGenerator,
    TradeGenerator,
    TransactionGenerator,
)
from data_gen.generators.financial.loan import PropertyGenerator
from data_gen.generators.financial.patterns import PaymentBehavior
from data_gen.generators.pool import FakerPool
from data_gen.models.financial.enums import AccountType, LoanType
from data_gen.sinks.kafka import BULK, STREAMING, KafkaSink, ProducerConfig
from data_gen.sinks.postgres import PostgresSink
from data_gen.store.financial import FinancialDataStore, MasterDataStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Cluster connection presets for --kafka-cluster flag
CLUSTER_PRESETS: dict[str, dict[str, str | int]] = {
    "cp": {
        "kafka_bootstrap": "localhost:9092",
        "schema_registry": "http://localhost:8081",
        "postgres_url": "postgresql://postgres:postgres@localhost:5432/datagen",
        "replication_factor": 1,
    },
    "oss": {
        "kafka_bootstrap": "localhost:19092,localhost:19093,localhost:19094",
        "schema_registry": "http://localhost:18081",
        "postgres_url": "postgresql://postgres:postgres@localhost:15432/datagen",
        "replication_factor": 3,
    },
}


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

    # Shared FakerPool — pre-generates all Faker values at startup
    # for 2-4x faster generation via random.choice() lookups.
    pool = FakerPool(seed=seed)

    # Initialize generators (all share the same pool)
    customer_gen = CustomerGenerator(seed=seed, pool=pool)
    account_gen = AccountGenerator(seed=seed, pool=pool)
    credit_card_gen = CreditCardGenerator(seed=seed, pool=pool)
    loan_gen = LoanGenerator(seed=seed, pool=pool)
    property_gen = PropertyGenerator(seed=seed, pool=pool)
    stock_gen = StockGenerator(seed=seed, pool=pool)
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


class TokenBucketRateLimiter:
    """Token bucket rate limiter for steady event emission.

    Parameters
    ----------
    rate : float
        Target events per second.
    burst : int | None
        Maximum burst size. Defaults to max(1, rate // 10).
    """

    def __init__(self, rate: float, burst: int | None = None) -> None:
        self.rate = rate
        self.burst = burst or max(1, int(rate // 10))
        self.tokens = float(self.burst)
        self.last_refill = time.monotonic()

    def acquire(self) -> None:
        """Block until a token is available, then consume one."""
        while True:
            now = time.monotonic()
            self.tokens = min(self.burst, self.tokens + (now - self.last_refill) * self.rate)
            self.last_refill = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            time.sleep((1.0 - self.tokens) / self.rate)


def _format_duration(seconds: float) -> str:
    """Format seconds as HH:MM:SS."""
    h, remainder = divmod(int(seconds), 3600)
    m, s = divmod(remainder, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def load_to_kafka_realtime(
    store: MasterDataStore,
    bootstrap_servers: str,
    schema_registry_url: str | None,
    seed: int,
    all_installments: list,
    duration_seconds: int,
    rate_per_second: int,
    use_cloudevents: bool = True,
) -> dict[str, int]:
    """Generate and stream events to Kafka at a controlled rate.

    Parameters
    ----------
    store : MasterDataStore
        Master data store with customers, accounts, cards, stocks.
    bootstrap_servers : str
        Kafka bootstrap servers.
    schema_registry_url : str | None
        Schema Registry URL (None for JSON serialization).
    seed : int
        Random seed for generators.
    all_installments : list
        Pre-generated installments from warm-up.
    duration_seconds : int
        How long to stream in seconds.
    rate_per_second : int
        Target events per second.
    use_cloudevents : bool
        Include CloudEvents headers (default: True).

    Returns
    -------
    dict[str, int]
        Counts of events sent per type.
    """
    logger.info(
        "Real-time streaming to Kafka: %d events/sec for %s...",
        rate_per_second, _format_duration(duration_seconds),
    )

    # Shared FakerPool for event generators
    pool = FakerPool(seed=seed)

    # Initialize generators
    transaction_gen = TransactionGenerator(seed=seed, pool=pool)
    card_tx_gen = CardTransactionGenerator(seed=seed, pool=pool)
    trade_gen = TradeGenerator(seed=seed, pool=pool)

    # Create STREAMING config
    config = ProducerConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=schema_registry_url,
        acks=STREAMING.acks,
        batch_size=STREAMING.batch_size,
        linger_ms=STREAMING.linger_ms,
        compression=STREAMING.compression,
        queue_buffering_max_messages=STREAMING.queue_buffering_max_messages,
        queue_buffering_max_kbytes=STREAMING.queue_buffering_max_kbytes,
    )

    sink = KafkaSink(config, use_cloudevents=use_cloudevents)

    # Pre-compute master data lists for O(1) random access
    accounts_list = list(store.accounts.values())
    cards_list = list(store.credit_cards.values())
    investment_accounts = [a for a in accounts_list if a.account_type == AccountType.INVESTIMENTOS]
    stocks_list = list(store.stocks.values())

    # Event type distribution and dispatch
    event_types = ["transactions", "card_transactions", "trades", "installments"]
    weights = [0.50, 0.30, 0.10, 0.10]

    # Adjust weights if some entity types are missing
    if not cards_list:
        logger.warning("No credit cards generated — card_transactions weight redistributed to transactions")
        weights = [0.80, 0.00, 0.10, 0.10]
    if not investment_accounts or not stocks_list:
        logger.warning("No investment accounts/stocks — trades weight redistributed to transactions")
        weights[0] += weights[2]
        weights[2] = 0.0
    if not all_installments:
        logger.warning("No installments generated — weight redistributed to transactions")
        weights[0] += weights[3]
        weights[3] = 0.0

    installments_iter = itertools.cycle(all_installments) if all_installments else None

    # Rate limiter
    limiter = TokenBucketRateLimiter(rate_per_second)

    # Graceful shutdown
    shutdown_requested = False
    original_sigint = signal.getsignal(signal.SIGINT)

    def _signal_handler(signum: int, frame: object) -> None:
        nonlocal shutdown_requested
        shutdown_requested = True
        logger.info("[STREAM] Shutdown requested, flushing remaining events...")

    signal.signal(signal.SIGINT, _signal_handler)

    # Main streaming loop
    counts: dict[str, int] = {et: 0 for et in event_types}
    total_sent = 0
    start_time = time.monotonic()
    deadline = start_time + duration_seconds
    last_report = start_time

    try:
        while time.monotonic() < deadline and not shutdown_requested:
            limiter.acquire()

            # Pick event type
            event_type = random.choices(event_types, weights=weights, k=1)[0]

            # Generate and send
            if event_type == "transactions":
                account = random.choice(accounts_list)
                record = transaction_gen.generate(account.account_id, account.customer_id)
                sink.send_fast("banking.transactions", record)
            elif event_type == "card_transactions":
                card = random.choice(cards_list)
                record = card_tx_gen.generate(card.card_id, card.customer_id)
                sink.send_fast("banking.card-transactions", record)
            elif event_type == "trades":
                account = random.choice(investment_accounts)
                stock = random.choice(stocks_list)
                record = trade_gen.generate(
                    account.account_id, stock, customer_id=account.customer_id,
                )
                sink.send_fast("banking.trades", record)
            else:  # installments
                record = next(installments_iter)
                sink.send_fast("banking.installments", record)

            counts[event_type] += 1
            total_sent += 1

            # Progress report every 10 seconds
            now = time.monotonic()
            if now - last_report >= 10.0:
                elapsed = now - start_time
                remaining = deadline - now
                actual_rate = total_sent / elapsed
                logger.info(
                    "[STREAM] %s events | %.0f/sec (target: %d) | "
                    "elapsed: %s | remaining: %s | "
                    "txn=%d card=%d trade=%d inst=%d",
                    f"{total_sent:,}", actual_rate, rate_per_second,
                    _format_duration(elapsed), _format_duration(remaining),
                    counts["transactions"], counts["card_transactions"],
                    counts["trades"], counts["installments"],
                )
                last_report = now
    finally:
        # Restore original signal handler
        signal.signal(signal.SIGINT, original_sigint)

        # Flush and close
        elapsed = time.monotonic() - start_time
        logger.info(
            "[STREAM] Flushing producer buffer (timeout: %.0fs)...",
            min(60.0, 30.0 + total_sent / 10000),
        )
        sink.flush()
        sink.close()

        avg_rate = total_sent / max(elapsed, 0.001)
        logger.info(
            "[STREAM] Complete! %s events in %s | avg rate: %.0f/sec",
            f"{total_sent:,}", _format_duration(elapsed), avg_rate,
        )
        logger.info(
            "[STREAM] Breakdown: txn=%s card=%s trade=%s inst=%s",
            f"{counts['transactions']:,}",
            f"{counts['card_transactions']:,}",
            f"{counts['trades']:,}",
            f"{counts['installments']:,}",
        )

    # Update store event counters for summary/validation
    for et, count in counts.items():
        store.count_event(et, count)

    return counts


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
    logger.info("Streaming event data to Kafka (BULK mode, 4 producers)...")

    # Shared FakerPool for event generators
    pool = FakerPool(seed=seed)

    # Initialize generators (all share the same pool)
    transaction_gen = TransactionGenerator(seed=seed, pool=pool)
    credit_card_gen = CreditCardGenerator(seed=seed, pool=pool)
    trade_gen = TradeGenerator(seed=seed, pool=pool)

    # Create BULK config (shared by all 4 producers)
    config = ProducerConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=schema_registry_url,
        acks=BULK.acks,
        batch_size=BULK.batch_size,
        linger_ms=BULK.linger_ms,
        compression=BULK.compression,
        queue_buffering_max_messages=BULK.queue_buffering_max_messages,
        queue_buffering_max_kbytes=BULK.queue_buffering_max_kbytes,
    )

    import queue
    import threading

    # 4 topic-specific queues + senders for I/O parallelism.
    # Each KafkaSink has an independent librdkafka Producer that releases
    # the GIL during C-level produce(), enabling genuine parallelism.
    TOPICS = [
        "banking.transactions",
        "banking.card-transactions",
        "banking.trades",
        "banking.installments",
    ]
    BATCH_SIZE = 1024
    _SENTINEL = object()

    topic_queues: dict[str, queue.Queue] = {t: queue.Queue(maxsize=10_000) for t in TOPICS}
    sender_sinks: list[KafkaSink] = []
    sender_results: dict[str, int] = {}

    def sender_loop(topic: str, q: queue.Queue, sink: KafkaSink) -> None:
        """Sender thread: drain batches from queue and produce to Kafka."""
        sent = 0
        while True:
            item = q.get()
            if item is _SENTINEL:
                break
            for _topic, record in item:
                sink.send_fast(_topic, record)
                sent += 1
        sender_results[topic] = sent

    # Create 4 sinks + start 4 sender threads
    sender_threads: list[threading.Thread] = []
    for topic in TOPICS:
        s = KafkaSink(config, use_cloudevents=use_cloudevents, poll_interval=10000)
        sender_sinks.append(s)
        t = threading.Thread(target=sender_loop, args=(topic, topic_queues[topic], s), daemon=True)
        t.start()
        sender_threads.append(t)

    try:
        accounts_list = list(store.accounts.values())
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 12, 31)
        cards_list = list(store.credit_cards.values())
        investment_accounts = [
            a for a in accounts_list if a.account_type == AccountType.INVESTIMENTOS
        ]
        stocks_list = list(store.stocks.values())

        # Counters shared with generator thread
        counts = {"transactions": 0, "card_transactions": 0, "trades": 0}

        def generate_all_events() -> None:
            """Generator thread: buffer events into batches and route to topic queues."""
            batch: dict[str, list] = {t: [] for t in TOPICS}

            # Transactions
            for account in accounts_list:
                for tx in transaction_gen.generate_for_account(
                    account, store, start_date, end_date, avg_transactions_per_day=0.3
                ):
                    batch["banking.transactions"].append(("banking.transactions", tx))
                    counts["transactions"] += 1
                    if len(batch["banking.transactions"]) >= BATCH_SIZE:
                        topic_queues["banking.transactions"].put(batch["banking.transactions"])
                        batch["banking.transactions"] = []

            # Card transactions
            for card in cards_list:
                for card_tx in credit_card_gen.generate_transactions(
                    card, start_date, end_date, avg_transactions_per_day=0.5
                ):
                    batch["banking.card-transactions"].append(("banking.card-transactions", card_tx))
                    counts["card_transactions"] += 1
                    if len(batch["banking.card-transactions"]) >= BATCH_SIZE:
                        topic_queues["banking.card-transactions"].put(batch["banking.card-transactions"])
                        batch["banking.card-transactions"] = []

            # Trades
            for account in investment_accounts:
                trades = trade_gen.generate_trades_for_account(
                    account_id=account.account_id,
                    stocks=stocks_list,
                    num_trades=20,
                    customer_id=account.customer_id,
                )
                for trade in trades:
                    batch["banking.trades"].append(("banking.trades", trade))
                    counts["trades"] += 1
                    if len(batch["banking.trades"]) >= BATCH_SIZE:
                        topic_queues["banking.trades"].put(batch["banking.trades"])
                        batch["banking.trades"] = []

            # Installments
            for inst in all_installments:
                batch["banking.installments"].append(("banking.installments", inst))
                if len(batch["banking.installments"]) >= BATCH_SIZE:
                    topic_queues["banking.installments"].put(batch["banking.installments"])
                    batch["banking.installments"] = []

            # Flush remaining batches + sentinel to each queue
            for t in TOPICS:
                if batch[t]:
                    topic_queues[t].put(batch[t])
                topic_queues[t].put(_SENTINEL)

        # Start generator thread
        t0 = time.perf_counter()
        gen_thread = threading.Thread(target=generate_all_events, daemon=True)
        gen_thread.start()

        # Wait for all sender threads to finish
        for t in sender_threads:
            t.join()
        gen_thread.join()

        elapsed = time.perf_counter() - t0
        total_sent = sum(sender_results.values())

        # Record counts
        store.count_event("transactions", counts["transactions"])
        store.count_event("card_transactions", counts["card_transactions"])
        store.count_event("trades", counts["trades"])
        store.count_event("installments", len(all_installments))

        logger.info(
            "Streamed %d events in %.1fs (%.0f/sec) — "
            "txns=%d, card_txns=%d, trades=%d, installments=%d",
            total_sent,
            elapsed,
            total_sent / max(elapsed, 0.001),
            counts["transactions"],
            counts["card_transactions"],
            counts["trades"],
            len(all_installments),
        )

        # Flush all producers
        for s in sender_sinks:
            s.flush()
        logger.info("Kafka streaming complete")
    finally:
        for s in sender_sinks:
            s.close()


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

    # Shared FakerPool for event generators
    pool = FakerPool(seed=seed)

    # Initialize generators (all share the same pool)
    transaction_gen = TransactionGenerator(seed=seed, pool=pool)
    credit_card_gen = CreditCardGenerator(seed=seed, pool=pool)
    trade_gen = TradeGenerator(seed=seed, pool=pool)

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
                customer_id=account.customer_id,
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
    replication_factor: int = 1,
) -> None:
    """Create Kafka topics if they don't exist.

    Parameters
    ----------
    bootstrap_servers : str
        Kafka bootstrap servers.
    retention_hours : int
        Per-topic retention in hours (default: 168 = 7 days).
        Use 24 for bulk test loads to save disk space.
    replication_factor : int
        Topic replication factor (default: 1, use 3 for multi-broker clusters).
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    retention_ms = str(retention_hours * 3600 * 1000)
    topic_config = {"retention.ms": retention_ms}

    topics = [
        NewTopic("banking.transactions", num_partitions=6, replication_factor=replication_factor, config=topic_config),
        NewTopic("banking.card-transactions", num_partitions=6, replication_factor=replication_factor, config=topic_config),
        NewTopic("banking.trades", num_partitions=3, replication_factor=replication_factor, config=topic_config),
        NewTopic("banking.installments", num_partitions=3, replication_factor=replication_factor, config=topic_config),
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
    parser.add_argument(
        "--kafka-cluster",
        type=str,
        choices=["cp", "oss"],
        default=None,
        help="Cluster preset: 'cp' (Confluent Platform, 1 broker) or 'oss' (Apache Kafka, 3 brokers). "
        "Overrides --kafka-bootstrap, --schema-registry, --postgres-url, and replication factor.",
    )
    parser.add_argument(
        "--replication-factor",
        type=int,
        default=None,
        help="Kafka topic replication factor (auto-set by --kafka-cluster, or specify manually)",
    )

    # Real-time streaming mode
    stream_group = parser.add_argument_group("real-time streaming")
    stream_group.add_argument(
        "--stream",
        action="store_true",
        help="Enable real-time streaming mode (continuous event generation at a fixed rate)",
    )
    stream_group.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Streaming duration in seconds (e.g., 3600 for 1 hour). Required with --stream.",
    )
    stream_group.add_argument(
        "--rate",
        type=int,
        default=None,
        help="Target events per second (default: 1000)",
    )

    args = parser.parse_args()

    # Apply cluster preset if specified
    if args.kafka_cluster:
        preset = CLUSTER_PRESETS[args.kafka_cluster]
        args.kafka_bootstrap = preset["kafka_bootstrap"]
        args.schema_registry = preset["schema_registry"]
        args.postgres_url = preset["postgres_url"]
        if args.replication_factor is None:
            args.replication_factor = preset["replication_factor"]

    # Default replication factor
    if args.replication_factor is None:
        args.replication_factor = 1

    # Validate streaming mode flags
    if args.stream:
        if args.duration is None:
            parser.error("--duration is required with --stream")
        if args.fast:
            parser.error("--stream and --fast are mutually exclusive")

    # Auto-enable fast mode for large datasets
    use_fast = args.fast or (args.customers >= 10000 and not args.stream)

    # Schema registry URL: None disables Avro (falls back to JSON)
    schema_registry = None if args.no_avro else args.schema_registry

    # Real-time streaming mode
    if args.stream:
        rate = args.rate or 1000

        logger.info("=" * 60)
        logger.info("Data Generator - Real-Time Streaming Mode")
        logger.info("=" * 60)
        logger.info("Duration: %s (%d seconds)", _format_duration(args.duration), args.duration)
        logger.info("Target rate: %d events/sec", rate)
        logger.info("Expected total: ~%s events", f"{rate * args.duration:,}")
        logger.info("Customers (warm-up): %d", args.customers)
        logger.info("Seed: %d", args.seed)
        logger.info("Serialization: %s", "JSON" if args.no_avro else "Avro (via Schema Registry)")
        logger.info("Kafka: %s", args.kafka_bootstrap)
        logger.info("=" * 60)

        overall_start = time.perf_counter()

        # Warm-up: generate master data
        store = MasterDataStore()
        all_installments = generate_master_data(store, args.customers, args.seed)

        # Create topics if requested
        if args.create_topics:
            create_kafka_topics(
                args.kafka_bootstrap,
                retention_hours=args.retention_hours,
                replication_factor=args.replication_factor,
            )

        # Snapshot offsets before streaming
        kafka_offsets_before = get_kafka_offsets(args.kafka_bootstrap)

        # Stream events at controlled rate
        load_to_kafka_realtime(
            store=store,
            bootstrap_servers=args.kafka_bootstrap,
            schema_registry_url=schema_registry,
            seed=args.seed,
            all_installments=all_installments,
            duration_seconds=args.duration,
            rate_per_second=rate,
            use_cloudevents=not args.no_cloudevents,
        )

        overall_elapsed = time.perf_counter() - overall_start

        # Print summary + validation
        summary = store.summary()
        total = sum(summary.values())
        print_summary(
            summary,
            total,
            overall_elapsed,
            kafka_bootstrap=args.kafka_bootstrap,
            kafka_offsets_before=kafka_offsets_before,
        )
        return

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
            create_kafka_topics(args.kafka_bootstrap, retention_hours=args.retention_hours, replication_factor=args.replication_factor)

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
            create_kafka_topics(args.kafka_bootstrap, retention_hours=args.retention_hours, replication_factor=args.replication_factor)

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
