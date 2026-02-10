#!/usr/bin/env python3
"""Benchmark data generation and sink performance.

Measures:
- Data generation rate (records/sec per entity type)
- PostgreSQL insert rate (executemany vs COPY)
- Kafka produce rate (per preset)
- Memory usage at different scales

Usage:
    python scripts/benchmark.py
    python scripts/benchmark.py --scale 10000
    python scripts/benchmark.py --skip-postgres --skip-kafka
"""

import argparse
import logging
import os
import sys
import time
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
    TransactionGenerator,
)
from data_gen.generators.financial.loan import PropertyGenerator
from data_gen.generators.financial.patterns import PaymentBehavior
from data_gen.models.financial.enums import LoanType
from data_gen.store.financial import FinancialDataStore

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_memory_mb() -> float:
    """Get current process memory usage in MB."""
    try:
        import resource
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return usage.ru_maxrss / (1024 * 1024)  # macOS reports in bytes
    except Exception:
        return 0.0


def benchmark_generation(num_customers: int, seed: int) -> FinancialDataStore:
    """Benchmark data generation speed.

    Parameters
    ----------
    num_customers : int
        Number of customers to generate.
    seed : int
        Random seed.

    Returns
    -------
    FinancialDataStore
        Store with generated data (for sink benchmarks).
    """
    store = FinancialDataStore()
    mem_before = get_memory_mb()

    # Generators
    customer_gen = CustomerGenerator(seed=seed)
    account_gen = AccountGenerator(seed=seed)
    credit_card_gen = CreditCardGenerator(seed=seed)
    loan_gen = LoanGenerator(seed=seed)
    property_gen = PropertyGenerator(seed=seed)
    stock_gen = StockGenerator(seed=seed)
    payment_behavior = PaymentBehavior(seed=seed)

    # Customers
    t0 = time.perf_counter()
    for _ in range(num_customers):
        store.add_customer(customer_gen.generate())
    t_customers = time.perf_counter() - t0
    print(f"  Customers:     {num_customers:>8,} in {t_customers:.2f}s  ({num_customers / t_customers:,.0f}/sec)")

    # Properties
    num_properties = max(1, num_customers // 10)
    t0 = time.perf_counter()
    for _ in range(num_properties):
        store.add_property(property_gen.generate())
    t_props = time.perf_counter() - t0
    print(f"  Properties:    {num_properties:>8,} in {t_props:.2f}s  ({num_properties / t_props:,.0f}/sec)")

    # Stocks
    t0 = time.perf_counter()
    for stock in stock_gen.generate_all():
        store.add_stock(stock)
    t_stocks = time.perf_counter() - t0
    num_stocks = len(store.stocks)
    print(f"  Stocks:        {num_stocks:>8,} in {t_stocks:.2f}s  ({num_stocks / max(t_stocks, 0.001):,.0f}/sec)")

    # Accounts
    t0 = time.perf_counter()
    for customer in store.customers.values():
        for account in account_gen.generate_for_customer(
            customer.customer_id, customer.created_at, customer.monthly_income
        ):
            store.add_account(account)
    t_accounts = time.perf_counter() - t0
    num_accounts = len(store.accounts)
    print(f"  Accounts:      {num_accounts:>8,} in {t_accounts:.2f}s  ({num_accounts / t_accounts:,.0f}/sec)")

    # Credit cards (70%)
    t0 = time.perf_counter()
    customers_list = list(store.customers.values())
    num_with_cards = int(len(customers_list) * 0.7)
    for customer in customers_list[:num_with_cards]:
        card = credit_card_gen.generate(customer.customer_id)
        store.add_credit_card(card)
    t_cards = time.perf_counter() - t0
    num_cards = len(store.credit_cards)
    print(f"  Credit Cards:  {num_cards:>8,} in {t_cards:.2f}s  ({num_cards / t_cards:,.0f}/sec)")

    # Loans (30% personal)
    t0 = time.perf_counter()
    num_personal = int(len(customers_list) * 0.3)
    properties_list = list(store.properties.values())
    property_idx = 0

    for customer in customers_list[:num_personal]:
        loan, installments = loan_gen.generate_with_installments(
            customer_id=customer.customer_id,
            loan_type=LoanType.PERSONAL,
        )
        store.add_loan(loan)
        modified = payment_behavior.apply_payment_behavior(
            installments, on_time_rate=0.85, late_rate=0.10, default_rate=0.05
        )
        for inst in modified:
            store.add_installment(inst)

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
        for inst in modified:
            store.add_installment(inst)

    t_loans = time.perf_counter() - t0
    num_loans = len(store.loans)
    num_installments = len(store.installments)
    print(f"  Loans:         {num_loans:>8,} in {t_loans:.2f}s  ({num_loans / t_loans:,.0f}/sec)")
    print(f"  Installments:  {num_installments:>8,} in {t_loans:.2f}s  (included in loans)")

    mem_after = get_memory_mb()
    print(f"\n  Memory: {mem_after:.1f} MB (delta: +{mem_after - mem_before:.1f} MB)")

    return store


def benchmark_postgres(
    store: FinancialDataStore, connection_string: str
) -> None:
    """Benchmark PostgreSQL insert methods.

    Parameters
    ----------
    store : FinancialDataStore
        Store with generated data.
    connection_string : str
        PostgreSQL connection string.
    """
    from data_gen.sinks.postgres import PostgresSink

    sink = PostgresSink(connection_string)
    try:
        sink.create_tables()
        sink.truncate_tables()

        customers = list(store.customers.values())
        properties = list(store.properties.values())
        stocks = list(store.stocks.values())
        accounts = list(store.accounts.values())
        credit_cards = list(store.credit_cards.values())
        loans = list(store.loans.values())

        # Benchmark executemany
        print("\n  executemany:")
        for entity_type, records in [
            ("customers", customers),
            ("properties", properties),
            ("stocks", stocks),
            ("accounts", accounts),
            ("credit_cards", credit_cards),
            ("loans", loans),
        ]:
            t0 = time.perf_counter()
            sink.write_batch(entity_type, records, use_copy=False)
            elapsed = time.perf_counter() - t0
            rate = len(records) / max(elapsed, 0.001)
            print(f"    {entity_type:<16} {len(records):>8,} in {elapsed:.3f}s  ({rate:>10,.0f}/sec)")

        sink.truncate_tables()

        # Benchmark COPY
        print("\n  COPY:")
        for entity_type, records in [
            ("customers", customers),
            ("properties", properties),
            ("stocks", stocks),
            ("accounts", accounts),
            ("credit_cards", credit_cards),
            ("loans", loans),
        ]:
            t0 = time.perf_counter()
            sink.write_batch(entity_type, records, use_copy=True)
            elapsed = time.perf_counter() - t0
            rate = len(records) / max(elapsed, 0.001)
            print(f"    {entity_type:<16} {len(records):>8,} in {elapsed:.3f}s  ({rate:>10,.0f}/sec)")

        sink.truncate_tables()
    finally:
        sink.close()


def benchmark_kafka(
    store: FinancialDataStore,
    bootstrap_servers: str,
    schema_registry_url: str | None,
) -> None:
    """Benchmark Kafka produce rates with different presets.

    Parameters
    ----------
    store : FinancialDataStore
        Store with generated data.
    bootstrap_servers : str
        Kafka bootstrap servers.
    schema_registry_url : str | None
        Schema Registry URL.
    """
    from data_gen.sinks.kafka import BULK, RELIABLE, KafkaSink, ProducerConfig

    # Use installments as a representative event type
    installments = store.installments
    if not installments:
        print("  No installments to benchmark")
        return

    sample = installments[:min(len(installments), 10000)]
    topic = "banking.installments"

    for label, config in [
        ("RELIABLE (acks=all, batch=16KB, poll=1)", ProducerConfig(
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
        )),
        ("BULK (acks=1, batch=512KB, poll=10K)", ProducerConfig(
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            acks="1",
            batch_size=524288,
            linger_ms=100,
            compression="lz4",
        )),
    ]:
        poll_interval = 1 if "RELIABLE" in label else 10000
        sink = KafkaSink(config, use_cloudevents=False, poll_interval=poll_interval)
        try:
            t0 = time.perf_counter()
            for record in sample:
                sink.send(topic, record)
            sink.flush()
            elapsed = time.perf_counter() - t0
            rate = len(sample) / max(elapsed, 0.001)
            print(f"    {label}")
            print(f"      {len(sample):>8,} msgs in {elapsed:.3f}s  ({rate:>10,.0f}/sec)")
        finally:
            sink.close()


def main() -> None:
    """Run benchmarks."""
    parser = argparse.ArgumentParser(description="Benchmark data-gen performance")
    parser.add_argument("--scale", type=int, default=1000, help="Number of customers (default: 1000)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--skip-postgres", action="store_true", help="Skip PostgreSQL benchmarks")
    parser.add_argument("--skip-kafka", action="store_true", help="Skip Kafka benchmarks")
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
        help="Schema Registry URL",
    )
    args = parser.parse_args()

    print("=" * 60)
    print(f"  data-gen Benchmark  |  scale={args.scale:,}  seed={args.seed}")
    print("=" * 60)

    print("\n[1] Data Generation")
    store = benchmark_generation(args.scale, args.seed)

    summary = store.summary()
    total = sum(summary.values())
    print(f"\n  Total entities: {total:,}")

    if not args.skip_postgres:
        print("\n[2] PostgreSQL Insert")
        try:
            benchmark_postgres(store, args.postgres_url)
        except Exception as e:
            print(f"  SKIPPED: {e}")

    if not args.skip_kafka:
        print("\n[3] Kafka Produce")
        try:
            benchmark_kafka(store, args.kafka_bootstrap, args.schema_registry)
        except Exception as e:
            print(f"  SKIPPED: {e}")

    print("\n" + "=" * 60)
    print("  Benchmark complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
