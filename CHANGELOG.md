# Changelog

All notable changes to the **data-gen** project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Author tags**: `(@MateusHenriqueOliveira)` for Mateus Oliveira, `(@claude-code)` for Claude Code AI.

---

## [Unreleased]

### Added

- Add real-time streaming mode (`--stream --duration --rate`) for continuous event ingestion at a fixed rate, simulating production-like Kafka event streams `(@claude-code)`
- Add `STREAMING` producer preset: `acks=1`, 32KB batches, 20ms linger, snappy compression — balanced for steady-state streaming `(@claude-code)`
- Add `TokenBucketRateLimiter` for precise event emission control with burst handling `(@claude-code)`
- Add graceful shutdown (Ctrl+C) with in-flight event flushing for streaming mode `(@claude-code)`
- Denormalize `customer_id` into all 4 event models (`Transaction`, `CardTransaction`, `Trade`, `Installment`) for direct stream-table joins in Flink/ksqlDB/Trino without multi-hop lookups `(@claude-code)`
- Add `customer_id` to Avro schemas for transactions, card-transactions, trades, and installments `(@claude-code)`
- Add `customer_id` columns to PostgreSQL DDL for `transactions`, `card_transactions`, and `installments` tables `(@claude-code)`
- Add `BaseGenerator` ABC with Faker initialization and seed-based reproducibility (`data_gen/generators/base.py`) `(@claude-code)`
- Add `AddressFactory` for centralized Brazilian address generation (`data_gen/generators/address.py`) `(@claude-code)`
- Add custom exception hierarchy: `DataGenError`, `EntityNotFoundError`, `ReferentialIntegrityError`, `InvalidEntityStateError`, `ConfigurationError`, `SinkError` (`data_gen/exceptions.py`) `(@claude-code)`
- Add `enums.py` module centralizing all financial domain enumerations (`data_gen/models/financial/enums.py`) `(@claude-code)`
- Add `serialization.py` module with `to_dict()` and `to_dict_fast()` for standardized data serialization (`data_gen/sinks/serialization.py`) `(@claude-code)`
- Add PostgreSQL `COPY` protocol support for 10-50x faster bulk loading (`data_gen/sinks/postgres.py`) `(@claude-code)`
- Add Kafka producer presets: `RELIABLE`, `FAST`, `BULK`, `EVENT_BY_EVENT` (`data_gen/sinks/kafka.py`) `(@claude-code)`
- Add Kafka `BufferError` handling with poll-and-retry pattern `(@claude-code)`
- Add flush timeout scaling: `min(300.0, 30.0 + sent/10000)` for large batches `(@claude-code)`
- Add batched polling (every 10K messages) instead of per-message `poll(0)` `(@claude-code)`
- Add automated validation summary: PostgreSQL row counts + Kafka delta offset verification (`scripts/load_data.py`) `(@claude-code)`
- Add parallel data loading script with multiprocessing (`scripts/load_data_parallel.py`) `(@claude-code)`
- Add benchmark script for measuring generation, PG, and Kafka throughput (`scripts/benchmark.py`) `(@claude-code)`
- Add `requirements.txt`, `requirements-dev.txt`, `requirements-lock.txt` for pip compatibility `(@claude-code)`
- Add unit tests for exceptions, serialization, address factory, config/logging, and expanded sink/store tests `(@claude-code)`
- Add Roadmap v2 document: poison pills, realistic patterns, new products, TUI (`docs/roadmap-v2.md`) `(@claude-code)`
- Add Windows setup guide with Docker Desktop and WSL2 (`docs/windows-setup.md`) `(@claude-code)`
- Add 3-broker Apache Kafka OSS cluster Docker Compose for high-throughput benchmarking (`docker/docker-compose.oss-kafka.yml`) `(@claude-code)`
- Add `--kafka-cluster cp|oss` flag to auto-configure cluster connections in `load_data.py` and `load_data_parallel.py` `(@claude-code)`
- Add `--replication-factor` CLI flag for topic creation with multi-broker clusters `(@claude-code)`
- Add `FastAvroSerializer` bypassing per-message confluent-kafka overhead with fastavro direct serialization `(@claude-code)`
- Add `send_fast()` method with error-only callbacks, pre-encoded CloudEvents headers, and UUID pool `(@claude-code)`
- Add `FakerPool` class with pre-generated value pools (names: 5K, cities: 500, CPFs: 10K, etc.) replacing per-call Faker methods with `random.choice()` for 2-4x generation speedup (`data_gen/generators/pool.py`) `(@claude-code)`
- Add `UUIDPool` class with batch `os.urandom(16 * 8192)` UUID generation — 3x faster than per-call `uuid.uuid4()` `(@claude-code)`
- Add pure-arithmetic CPF/CNPJ generators with check-digit validation, bypassing Faker overhead `(@claude-code)`
- Add producer-consumer threading to `load_to_kafka_streaming()` — generator thread fills `Queue(maxsize=100K)` while main thread drains via `send_fast()`, overlapping CPU-bound generation with GIL-releasing Kafka I/O `(@claude-code)`
- Add multi-producer threading: 4 independent `KafkaSink` instances (one per topic) with batch queue transfer (1024 items/batch), reducing queue operations from ~77M to ~75K and enabling 4-way I/O parallelism `(@claude-code)`

### Changed

- Refactor all financial model dataclasses to use centralized enums from `enums.py` `(@claude-code)`
- Refactor all financial generators to inherit from `BaseGenerator` `(@claude-code)`
- Enhance `FinancialDataStore` with improved entity management, validation, and FK indexes `(@claude-code)`
- Enhance all sinks (PostgreSQL, Kafka, JSON, Console) with shared serialization and improved error handling `(@claude-code)`
- Improve all three scenarios (FraudDetection, LoanPortfolio, Customer360) with better orchestration `(@claude-code)`
- Enhance `DataGenConfig` with additional configuration options `(@claude-code)`
- Expand test suite from initial coverage to 390 tests at 97% coverage `(@claude-code)`
- Refactor `load_data.py` with COPY protocol, BULK producer, streaming architecture, and progress bars `(@claude-code)`
- Switch BULK producer preset to `acks=0` (fire-and-forget) and `linger_ms=100` for ~40-60% throughput gain on synthetic data `(@claude-code)`
- Fix `FastAvroSerializer.serialize()` to write prefix into buffer first, eliminating per-message bytes concatenation `(@claude-code)`
- Pre-encode message key bytes once in `send_fast()`, reusing for both Kafka key and CloudEvents `ce_subject` header `(@claude-code)`
- Update `load_data_parallel.py` workers to use `send_fast()` with full BULK preset (queue depth + buffer size) `(@claude-code)`
- Update all 7 generators to use `FakerPool` instead of direct Faker calls, eliminating per-call overhead `(@claude-code)`
- Update all 3 scenarios and both load scripts to share a single `FakerPool` instance across generators `(@claude-code)`

### Infrastructure

- Configure PostgreSQL WAL for Debezium CDC: `wal_level=logical`, `max_wal_senders=3`, `max_replication_slots=3` (`docker/docker-compose.yml`) `(@claude-code)`
- Replace Confluent Control Center with Kafka UI (`provectuslabs/kafka-ui`) in Docker Compose `(@claude-code)`
- Update Docker Compose for Confluent Platform 8.1.1 KRaft mode compatibility `(@claude-code)`
- Add Prometheus, Alertmanager, and recording/trigger rules config files for monitoring `(@claude-code)`
- Tune PostgreSQL for bulk loading: shared_buffers, work_mem, max_wal_size `(@claude-code)`
- Fix Confluent license topic replication factor for single-broker stability (`docker/docker-compose.yml`) `(@claude-code)`
- Tune BULK producer preset: 1MB batch, 100ms linger, 1M queue depth, LZ4 compression, `acks=0` `(@claude-code)`
- Add `socket.send.buffer.bytes=1MB` and `message.send.max.retries=0` (for `acks=0`) to producer config `(@claude-code)`
- Tune OSS Kafka brokers: `num.io.threads=16`, `num.network.threads=8`, `num.replica.fetchers=2`, disable synchronous log flush (`docker/docker-compose.oss-kafka.yml`) `(@claude-code)`

### Documentation

- Update data catalog with `customer_id` field in all 4 event entities and cross-reference tables `(@claude-code)`
- Update data ingestion guide with `customer_id` FK references in Kafka-PostgreSQL mapping `(@claude-code)`
- Update Docker docs with PostgreSQL WAL/CDC configuration and CDC notes `(@claude-code)`
- Rewrite data ingestion guide with producer presets, resilience patterns, and validation docs `(@claude-code)`
- Update data catalog with all entity field changes and enum references `(@claude-code)`
- Rewrite Docker documentation for KRaft mode and Kafka UI `(@claude-code)`
- Update project README with 390 tests, updated architecture, and usage examples `(@claude-code)`

---

## [0.1.0] - 2026-01-23

Initial release of data-gen: a synthetic data generator for Brazilian banking scenarios.

**Author**: Mateus Oliveira `(@MateusHenriqueOliveira)`

### Added

#### Core Framework
- Project scaffolding with `pyproject.toml` (hatchling), Black, Ruff, mypy, pytest `(@MateusHenriqueOliveira)`
- `DataGenConfig` dataclass for centralized configuration `(@MateusHenriqueOliveira)`
- `FinancialDataStore` for in-memory entity storage with referential integrity `(@MateusHenriqueOliveira)`

#### Data Models (10 entities)
- `Customer` — Brazilian individual with CPF, income (log-normal), credit score `(@MateusHenriqueOliveira)`
- `Account` — Bank account (corrente, poupanca, investimentos) linked to customer `(@MateusHenriqueOliveira)`
- `Transaction` — Financial transaction with Pareto-distributed amounts and Pix support `(@MateusHenriqueOliveira)`
- `CreditCard` — Credit card with brand, limit, billing cycle `(@MateusHenriqueOliveira)`
- `CardTransaction` — Card purchase with merchant, MCC, installments `(@MateusHenriqueOliveira)`
- `Loan` — Loan product (personal, mortgage, auto, payroll) with SAC/PRICE amortization `(@MateusHenriqueOliveira)`
- `Installment` — Loan installment with payment tracking `(@MateusHenriqueOliveira)`
- `Property` — Real estate collateral for mortgage loans `(@MateusHenriqueOliveira)`
- `Stock` — B3 stock ticker with sector and market cap `(@MateusHenriqueOliveira)`
- `Trade` — Stock trade with buy/sell, quantity, and P&L tracking `(@MateusHenriqueOliveira)`

#### Generators (7 generators)
- `CustomerGenerator` — Realistic Brazilian profiles using Faker pt_BR `(@MateusHenriqueOliveira)`
- `AccountGenerator` — Bank accounts with FK validation to customers `(@MateusHenriqueOliveira)`
- `TransactionGenerator` — Transactions with business-hour weighting `(@MateusHenriqueOliveira)`
- `CreditCardGenerator` — Credit cards with income-based limits `(@MateusHenriqueOliveira)`
- `LoanGenerator` — Loans with installment generation `(@MateusHenriqueOliveira)`
- `StockGenerator` — B3 stocks and trades with market-hours simulation `(@MateusHenriqueOliveira)`
- Financial patterns module for cross-entity behavioral realism `(@MateusHenriqueOliveira)`

#### Sinks (4 output targets)
- `PostgresSink` — DDL auto-creation, batch upserts with ON CONFLICT `(@MateusHenriqueOliveira)`
- `KafkaSink` — Avro serialization via Schema Registry, CloudEvents binary content mode `(@MateusHenriqueOliveira)`
- `JsonFileSink` — Timestamped JSON output files `(@MateusHenriqueOliveira)`
- `ConsoleSink` — Pretty-printed terminal output with tqdm progress `(@MateusHenriqueOliveira)`

#### Scenarios (3 end-to-end scenarios)
- `FraudDetection` — Customers with suspicious transaction patterns and labeled fraud indicators `(@MateusHenriqueOliveira)`
- `LoanPortfolio` — Diversified loan portfolio with collateral and installments `(@MateusHenriqueOliveira)`
- `Customer360` — Full customer view: accounts, cards, transactions, loans, investments `(@MateusHenriqueOliveira)`

#### Scripts
- `scripts/load_data.py` — Load generated data into PostgreSQL and Kafka `(@MateusHenriqueOliveira)`

### Infrastructure

- Docker Compose with Confluent Platform 8.1.1 (KRaft mode): broker, schema-registry, connect, ksqldb, rest-proxy `(@MateusHenriqueOliveira)`
- PostgreSQL 15 for master data storage `(@MateusHenriqueOliveira)`
- GitHub issue templates (bug report, feature request) `(@MateusHenriqueOliveira)`

### Documentation

- Project README with architecture overview and quick start `(@MateusHenriqueOliveira)`
- Data catalog with all 10 entities (`docs/data-catalog.md`) `(@MateusHenriqueOliveira)`
- Data ingestion guide for PostgreSQL and Kafka (`docs/data-ingestion.md`) `(@MateusHenriqueOliveira)`
- Docker setup guide (`docs/docker.md`) `(@MateusHenriqueOliveira)`

---

[Unreleased]: https://github.com/intelium-labs/data-gen/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/intelium-labs/data-gen/releases/tag/v0.1.0
