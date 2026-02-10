# Changelog

All notable changes to the **data-gen** project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Author tags**: `(@MateusHenriqueOliveira)` for Mateus Oliveira, `(@claude-code)` for Claude Code AI.

---

## [Unreleased]

### Added

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

### Changed

- Refactor all financial model dataclasses to use centralized enums from `enums.py` `(@claude-code)`
- Refactor all financial generators to inherit from `BaseGenerator` `(@claude-code)`
- Enhance `FinancialDataStore` with improved entity management, validation, and FK indexes `(@claude-code)`
- Enhance all sinks (PostgreSQL, Kafka, JSON, Console) with shared serialization and improved error handling `(@claude-code)`
- Improve all three scenarios (FraudDetection, LoanPortfolio, Customer360) with better orchestration `(@claude-code)`
- Enhance `DataGenConfig` with additional configuration options `(@claude-code)`
- Expand test suite from initial coverage to 390 tests at 99% coverage `(@claude-code)`
- Refactor `load_data.py` with COPY protocol, BULK producer, streaming architecture, and progress bars `(@claude-code)`

### Infrastructure

- Replace Confluent Control Center with Kafka UI (`provectuslabs/kafka-ui`) in Docker Compose `(@claude-code)`
- Update Docker Compose for Confluent Platform 8.1.1 KRaft mode compatibility `(@claude-code)`
- Add Prometheus, Alertmanager, and recording/trigger rules config files for monitoring `(@claude-code)`
- Tune PostgreSQL for bulk loading: shared_buffers, work_mem, max_wal_size `(@claude-code)`

### Documentation

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
