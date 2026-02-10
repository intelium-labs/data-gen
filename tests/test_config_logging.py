"""Comprehensive tests for config and logging - 100% coverage."""

import logging
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from data_gen.config import (
    DataGenConfig,
    KafkaConfig,
    OutputConfig,
    PostgresConfig,
    ScenarioConfig,
    StreamConfig,
)
from data_gen.logging import JsonFormatter, get_logger, setup_logging


class TestKafkaConfig:
    """Tests for KafkaConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = KafkaConfig()

        assert config.bootstrap_servers == "localhost:9092"
        assert config.acks == "all"
        assert config.batch_size == 16384
        assert config.linger_ms == 5
        assert config.compression == "snappy"
        assert config.retries == 3

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9092",
            acks="1",
            batch_size=32768,
            linger_ms=10,
            compression="gzip",
            retries=5,
        )

        assert config.bootstrap_servers == "kafka:9092"
        assert config.acks == "1"
        assert config.batch_size == 32768

    def test_to_dict(self) -> None:
        """Test conversion to confluent-kafka config dict."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9092",
            acks="all",
            batch_size=16384,
            linger_ms=5,
            compression="snappy",
            retries=3,
        )

        result = config.to_dict()

        assert result["bootstrap.servers"] == "kafka:9092"
        assert result["acks"] == "all"
        assert result["batch.size"] == 16384
        assert result["linger.ms"] == 5
        assert result["compression.type"] == "snappy"
        assert result["retries"] == 3


class TestPostgresConfig:
    """Tests for PostgresConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = PostgresConfig()

        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "datagen"
        assert config.user == "postgres"
        assert config.password == "postgres"

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = PostgresConfig(
            host="db.example.com",
            port=5433,
            database="mydb",
            user="myuser",
            password="mypass",
        )

        assert config.host == "db.example.com"
        assert config.port == 5433
        assert config.database == "mydb"

    def test_connection_string(self) -> None:
        """Test connection string property."""
        config = PostgresConfig(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
        )

        expected = "postgresql://testuser:testpass@localhost:5432/testdb"
        assert config.connection_string == expected


class TestOutputConfig:
    """Tests for OutputConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = OutputConfig()

        assert config.json_output_dir == Path("output")
        assert config.pretty_json is False

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = OutputConfig(
            json_output_dir=Path("/tmp/output"),
            pretty_json=True,
        )

        assert config.json_output_dir == Path("/tmp/output")
        assert config.pretty_json is True


class TestStreamConfig:
    """Tests for StreamConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = StreamConfig()

        assert config.rate_per_second == 100.0
        assert config.duration_seconds == 60.0
        assert config.topic_prefix == "dev.financial"

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = StreamConfig(
            rate_per_second=500.0,
            duration_seconds=300.0,
            topic_prefix="prod.banking",
        )

        assert config.rate_per_second == 500.0
        assert config.duration_seconds == 300.0
        assert config.topic_prefix == "prod.banking"


class TestDataGenConfig:
    """Tests for DataGenConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = DataGenConfig()

        assert isinstance(config.kafka, KafkaConfig)
        assert isinstance(config.postgres, PostgresConfig)
        assert isinstance(config.output, OutputConfig)
        assert isinstance(config.stream, StreamConfig)
        assert config.seed is None
        assert config.log_level == "INFO"
        assert config.country_weights is None

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        kafka = KafkaConfig(bootstrap_servers="kafka:9092")
        postgres = PostgresConfig(host="db.example.com")

        config = DataGenConfig(
            kafka=kafka,
            postgres=postgres,
            seed=42,
            log_level="DEBUG",
        )

        assert config.kafka.bootstrap_servers == "kafka:9092"
        assert config.postgres.host == "db.example.com"
        assert config.seed == 42
        assert config.log_level == "DEBUG"

    def test_from_env_default(self) -> None:
        """Test creating config from environment with defaults."""
        # Clear relevant env vars
        env_vars = [
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_ACKS",
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "OUTPUT_DIR",
            "PRETTY_JSON",
            "STREAM_RATE",
            "STREAM_DURATION",
            "TOPIC_PREFIX",
            "SEED",
            "LOG_LEVEL",
            "COUNTRY_WEIGHTS",
        ]

        # Save and clear env vars
        saved = {k: os.environ.get(k) for k in env_vars}
        for k in env_vars:
            if k in os.environ:
                del os.environ[k]

        try:
            config = DataGenConfig.from_env()

            assert config.kafka.bootstrap_servers == "localhost:9092"
            assert config.kafka.acks == "all"
            assert config.postgres.host == "localhost"
            assert config.seed is None
            assert config.log_level == "INFO"
            assert config.country_weights is None
        finally:
            # Restore env vars
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

    def test_from_env_custom(self) -> None:
        """Test creating config from custom environment variables."""
        env_vars = {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka-cluster:9092",
            "KAFKA_ACKS": "1",
            "POSTGRES_HOST": "db.example.com",
            "POSTGRES_PORT": "5433",
            "POSTGRES_DB": "production",
            "POSTGRES_USER": "admin",
            "POSTGRES_PASSWORD": "secret",
            "OUTPUT_DIR": "/data/output",
            "PRETTY_JSON": "true",
            "STREAM_RATE": "500",
            "STREAM_DURATION": "300",
            "TOPIC_PREFIX": "prod.financial",
            "SEED": "12345",
            "LOG_LEVEL": "DEBUG",
        }

        # Save existing values
        saved = {k: os.environ.get(k) for k in env_vars}

        try:
            # Set env vars
            for k, v in env_vars.items():
                os.environ[k] = v

            config = DataGenConfig.from_env()

            assert config.kafka.bootstrap_servers == "kafka-cluster:9092"
            assert config.kafka.acks == "1"
            assert config.postgres.host == "db.example.com"
            assert config.postgres.port == 5433
            assert config.postgres.database == "production"
            assert config.output.json_output_dir == Path("/data/output")
            assert config.output.pretty_json is True
            assert config.stream.rate_per_second == 500.0
            assert config.stream.duration_seconds == 300.0
            assert config.stream.topic_prefix == "prod.financial"
            assert config.seed == 12345
            assert config.log_level == "DEBUG"
        finally:
            # Restore env vars
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v
                elif k in os.environ:
                    del os.environ[k]


    def test_from_env_country_weights(self) -> None:
        """Test COUNTRY_WEIGHTS parsed from env."""
        saved = os.environ.get("COUNTRY_WEIGHTS")

        try:
            os.environ["COUNTRY_WEIGHTS"] = '{"BR": 0.5, "US": 0.3, "DE": 0.2}'
            config = DataGenConfig.from_env()
            assert config.country_weights == {"BR": 0.5, "US": 0.3, "DE": 0.2}
        finally:
            if saved is not None:
                os.environ["COUNTRY_WEIGHTS"] = saved
            elif "COUNTRY_WEIGHTS" in os.environ:
                del os.environ["COUNTRY_WEIGHTS"]


class TestScenarioConfig:
    """Tests for ScenarioConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = ScenarioConfig(name="test_scenario")

        assert config.name == "test_scenario"
        assert config.num_customers == 100
        assert config.transactions_per_customer == 50
        assert config.start_date is None
        assert config.end_date is None
        assert config.enable_fraud_patterns is False
        assert config.fraud_rate == 0.05
        assert config.enable_poison_pills is False
        assert config.poison_pill_rate == 0.01
        assert config.labels == {}

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        from datetime import datetime

        start = datetime(2024, 1, 1)
        end = datetime(2024, 12, 31)

        config = ScenarioConfig(
            name="fraud_detection",
            num_customers=500,
            transactions_per_customer=100,
            start_date=start,
            end_date=end,
            enable_fraud_patterns=True,
            fraud_rate=0.10,
            enable_poison_pills=True,
            poison_pill_rate=0.05,
            labels={"environment": "staging"},
        )

        assert config.name == "fraud_detection"
        assert config.num_customers == 500
        assert config.transactions_per_customer == 100
        assert config.start_date == start
        assert config.end_date == end
        assert config.enable_fraud_patterns is True
        assert config.fraud_rate == 0.10
        assert config.enable_poison_pills is True
        assert config.poison_pill_rate == 0.05
        assert config.labels == {"environment": "staging"}

    def test_datagen_config_has_scenario(self) -> None:
        """Test that DataGenConfig includes optional scenario field."""
        config = DataGenConfig()
        assert config.scenario is None

    def test_datagen_config_with_scenario(self) -> None:
        """Test DataGenConfig with ScenarioConfig."""
        scenario = ScenarioConfig(name="customer_360", num_customers=200)
        config = DataGenConfig(scenario=scenario)

        assert config.scenario is not None
        assert config.scenario.name == "customer_360"
        assert config.scenario.num_customers == 200


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_setup_logging_default(self) -> None:
        """Test default logging setup."""
        setup_logging()

        logger = logging.getLogger("data_gen")
        assert logger.level == logging.INFO

    def test_setup_logging_debug(self) -> None:
        """Test debug level logging setup."""
        setup_logging(level="DEBUG")

        logger = logging.getLogger()
        assert logger.level == logging.DEBUG

    def test_setup_logging_warning(self) -> None:
        """Test warning level logging setup."""
        setup_logging(level="WARNING")

        logger = logging.getLogger()
        assert logger.level == logging.WARNING

    def test_setup_logging_invalid_level(self) -> None:
        """Test logging with invalid level defaults to INFO."""
        setup_logging(level="INVALID")

        logger = logging.getLogger()
        assert logger.level == logging.INFO

    def test_setup_logging_standard_format(self) -> None:
        """Test standard format logging."""
        setup_logging(format_type="standard")

        logger = logging.getLogger()
        # Check that a handler was added
        assert len(logger.handlers) > 0

    def test_setup_logging_json_format(self) -> None:
        """Test JSON format logging."""
        setup_logging(format_type="json")

        logger = logging.getLogger()
        # Check that a handler with JsonFormatter was added
        has_json_formatter = any(
            isinstance(h.formatter, JsonFormatter) for h in logger.handlers
        )
        assert has_json_formatter

    def test_setup_logging_replaces_handlers(self) -> None:
        """Test that setup_logging replaces existing handlers."""
        logger = logging.getLogger()

        # Add some handlers
        logger.addHandler(logging.StreamHandler())
        logger.addHandler(logging.StreamHandler())
        initial_count = len(logger.handlers)

        setup_logging()

        # Should have removed old handlers and added new one
        assert len(logger.handlers) == 1

    def test_external_loggers_quieted(self) -> None:
        """Test that external library loggers are quieted."""
        setup_logging(level="DEBUG")

        confluent_logger = logging.getLogger("confluent_kafka")
        psycopg_logger = logging.getLogger("psycopg")

        # These should be at WARNING level regardless of main level
        assert confluent_logger.level == logging.WARNING
        assert psycopg_logger.level == logging.WARNING


class TestJsonFormatter:
    """Tests for JsonFormatter."""

    def test_format_basic(self) -> None:
        """Test basic log formatting."""
        formatter = JsonFormatter()

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        result = formatter.format(record)

        import json

        data = json.loads(result)

        assert data["level"] == "INFO"
        assert data["logger"] == "test.logger"
        assert data["message"] == "Test message"
        assert "timestamp" in data

    def test_format_with_exception(self) -> None:
        """Test formatting with exception info."""
        formatter = JsonFormatter()

        try:
            raise ValueError("Test error")
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test.logger",
            level=logging.ERROR,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Error occurred",
            args=(),
            exc_info=exc_info,
        )

        result = formatter.format(record)

        import json

        data = json.loads(result)

        assert data["level"] == "ERROR"
        assert "exception" in data
        assert "ValueError" in data["exception"]

    def test_format_with_extra(self) -> None:
        """Test formatting with extra fields."""
        formatter = JsonFormatter()

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        record.extra = {"custom_field": "custom_value"}

        result = formatter.format(record)

        import json

        data = json.loads(result)

        assert data["custom_field"] == "custom_value"


class TestGetLogger:
    """Tests for get_logger function."""

    def test_get_logger(self) -> None:
        """Test getting a logger."""
        logger = get_logger("test.module")

        assert isinstance(logger, logging.Logger)
        assert logger.name == "test.module"

    def test_get_logger_same_instance(self) -> None:
        """Test that get_logger returns same instance for same name."""
        logger1 = get_logger("test.same")
        logger2 = get_logger("test.same")

        assert logger1 is logger2

    def test_get_logger_different_instances(self) -> None:
        """Test that get_logger returns different instances for different names."""
        logger1 = get_logger("test.one")
        logger2 = get_logger("test.two")

        assert logger1 is not logger2
        assert logger1.name != logger2.name


class TestDataGenInit:
    """Tests for data_gen __init__.py."""

    def test_version_exported(self) -> None:
        """Test that __version__ is exported."""
        from data_gen import __version__

        assert __version__ is not None
        assert isinstance(__version__, str)
