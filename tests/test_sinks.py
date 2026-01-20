"""Comprehensive tests for sinks - 100% coverage."""

import json
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import MagicMock, patch

import pytest

from data_gen.models.base import Address
from data_gen.models.financial import Customer, Transaction
from data_gen.sinks.console import ConsoleSink
from data_gen.sinks.json_file import JsonFileSink


class TestConsoleSink:
    """Tests for ConsoleSink."""

    def test_init_default(self) -> None:
        """Test default initialization."""
        sink = ConsoleSink()

        assert sink.pretty is True
        assert sink.max_records is None
        assert sink._counts == {}

    def test_init_custom(self) -> None:
        """Test custom initialization."""
        sink = ConsoleSink(pretty=False, max_records=5)

        assert sink.pretty is False
        assert sink.max_records == 5

    def test_write_batch_dict(self, capsys: pytest.CaptureFixture) -> None:
        """Test writing batch of dictionaries."""
        sink = ConsoleSink(pretty=False)
        records = [
            {"id": 1, "name": "Test 1"},
            {"id": 2, "name": "Test 2"},
        ]

        sink.write_batch("test_entity", records)
        captured = capsys.readouterr()

        assert "test_entity" in captured.out
        assert "2 records" in captured.out
        assert sink._counts["test_entity"] == 2

    def test_write_batch_dataclass(self, capsys: pytest.CaptureFixture) -> None:
        """Test writing batch of dataclass objects."""
        sink = ConsoleSink(pretty=True)

        customer = Customer(
            customer_id="cust-001",
            cpf="123.456.789-00",
            name="Test",
            email="test@test.com",
            phone="+5511999999999",
            address=Address(
                street="Test",
                number="1",
                neighborhood="Test",
                city="Test",
                state="SP",
                postal_code="00000-000",
            ),
            monthly_income=Decimal("5000.00"),
            employment_status="EMPLOYED",
            credit_score=700,
            created_at=datetime.now(),
        )

        sink.write_batch("customers", [customer])
        captured = capsys.readouterr()

        assert "customers" in captured.out
        assert "cust-001" in captured.out

    def test_write_batch_with_max_records(self, capsys: pytest.CaptureFixture) -> None:
        """Test writing batch with max_records limit."""
        sink = ConsoleSink(max_records=2)
        records = [{"id": i} for i in range(10)]

        sink.write_batch("test_entity", records)
        captured = capsys.readouterr()

        assert "10 records" in captured.out
        assert "and 8 more records" in captured.out

    def test_write_batch_accumulates_count(self) -> None:
        """Test that multiple batches accumulate count."""
        sink = ConsoleSink(pretty=False)

        sink.write_batch("test", [{"id": 1}])
        sink.write_batch("test", [{"id": 2}, {"id": 3}])

        assert sink._counts["test"] == 3

    def test_write_stream(self, capsys: pytest.CaptureFixture) -> None:
        """Test streaming records."""
        sink = ConsoleSink(pretty=False)

        def gen() -> Iterator[dict]:
            for i in range(5):
                yield {"id": i}

        sink.write_stream("test_topic", gen(), rate_per_second=1000, duration_seconds=0.1)
        captured = capsys.readouterr()

        assert "Streaming to: test_topic" in captured.out
        assert "Streamed" in captured.out

    def test_write_stream_with_duration_timeout(self, capsys: pytest.CaptureFixture) -> None:
        """Test streaming stops when duration is exceeded (line 68)."""
        import time

        sink = ConsoleSink(pretty=False)

        def infinite_gen() -> Iterator[dict]:
            """Infinite generator that would run forever without timeout."""
            i = 0
            while True:
                yield {"id": i}
                i += 1
                time.sleep(0.01)  # Small delay to allow time to pass

        # Short duration should stop the infinite generator
        sink.write_stream("test_topic", infinite_gen(), rate_per_second=100, duration_seconds=0.05)
        captured = capsys.readouterr()

        assert "Streaming to: test_topic" in captured.out
        assert "Streamed" in captured.out

    def test_write_stream_pretty_printing(self, capsys: pytest.CaptureFixture) -> None:
        """Test streaming with pretty printing enabled (line 72)."""
        sink = ConsoleSink(pretty=True)

        def gen() -> Iterator[dict]:
            yield {"id": 1, "name": "test"}

        sink.write_stream("test_topic", gen(), rate_per_second=1000, duration_seconds=0.5)
        captured = capsys.readouterr()

        # Pretty printed output should have indentation
        assert "Streaming to: test_topic" in captured.out
        # The JSON output should have been printed with indent=2
        assert '"id"' in captured.out

    def test_close(self, capsys: pytest.CaptureFixture) -> None:
        """Test close method prints summary."""
        sink = ConsoleSink()
        sink._counts = {"entity1": 10, "entity2": 20}

        sink.close()
        captured = capsys.readouterr()

        assert "Console Sink Summary" in captured.out
        assert "entity1: 10 records" in captured.out
        assert "entity2: 20 records" in captured.out

    def test_to_dict_other_type(self) -> None:
        """Test converting non-dict, non-dataclass object."""
        sink = ConsoleSink()

        result = sink._to_dict("simple string")
        assert result == {"value": "simple string"}

    def test_serialize_value_decimal(self) -> None:
        """Test serializing Decimal values."""
        sink = ConsoleSink()

        result = sink._serialize_value(Decimal("123.45"))
        assert result == 123.45

    def test_serialize_value_datetime(self) -> None:
        """Test serializing datetime values."""
        sink = ConsoleSink()
        dt = datetime(2024, 1, 15, 10, 30, 0)

        result = sink._serialize_value(dt)
        assert result == "2024-01-15T10:30:00"

    def test_serialize_value_date(self) -> None:
        """Test serializing date values."""
        sink = ConsoleSink()
        from datetime import date

        d = date(2024, 1, 15)

        result = sink._serialize_value(d)
        assert result == "2024-01-15"

    def test_serialize_value_nested_dict(self) -> None:
        """Test serializing nested dictionaries."""
        sink = ConsoleSink()

        data = {"level1": {"level2": Decimal("10.5")}}
        result = sink._serialize_value(data)

        assert result == {"level1": {"level2": 10.5}}

    def test_serialize_value_list(self) -> None:
        """Test serializing lists."""
        sink = ConsoleSink()

        data = [Decimal("1.0"), Decimal("2.0")]
        result = sink._serialize_value(data)

        assert result == [1.0, 2.0]


class TestJsonFileSink:
    """Tests for JsonFileSink."""

    def test_init_creates_directory(self) -> None:
        """Test that init creates output directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "new_subdir"
            sink = JsonFileSink(output_dir)

            assert output_dir.exists()
            assert sink.pretty is False

    def test_init_with_pretty(self) -> None:
        """Test initialization with pretty printing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = JsonFileSink(tmpdir, pretty=True)

            assert sink.pretty is True

    def test_write_batch(self) -> None:
        """Test writing batch to JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = JsonFileSink(tmpdir)
            records = [{"id": 1, "name": "Test"}]

            sink.write_batch("test_entity", records)

            file_path = Path(tmpdir) / "test_entity.json"
            assert file_path.exists()

            with open(file_path) as f:
                data = json.load(f)

            assert len(data) == 1
            assert data[0]["id"] == 1

    def test_write_batch_pretty(self) -> None:
        """Test writing batch with pretty printing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = JsonFileSink(tmpdir, pretty=True)
            records = [{"id": 1}]

            sink.write_batch("test", records)

            file_path = Path(tmpdir) / "test.json"
            with open(file_path) as f:
                content = f.read()

            # Pretty printed JSON has newlines
            assert "\n" in content

    def test_write_batch_dataclass(self) -> None:
        """Test writing dataclass objects."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = JsonFileSink(tmpdir)

            tx = Transaction(
                transaction_id="tx-001",
                account_id="acct-001",
                transaction_type="PIX",
                amount=Decimal("100.50"),
                direction="DEBIT",
                counterparty_key="12345678900",
                counterparty_name="Test Recipient",
                description="Test",
                timestamp=datetime.now(),
                status="COMPLETED",
            )

            sink.write_batch("transactions", [tx])

            file_path = Path(tmpdir) / "transactions.json"
            with open(file_path) as f:
                data = json.load(f)

            assert data[0]["transaction_id"] == "tx-001"
            assert data[0]["amount"] == 100.50

    def test_write_stream(self) -> None:
        """Test streaming to JSON Lines file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = JsonFileSink(tmpdir)

            def gen() -> Iterator[dict]:
                for i in range(5):
                    yield {"id": i}

            sink.write_stream("test.topic", gen(), rate_per_second=1000, duration_seconds=0.1)

            file_path = Path(tmpdir) / "test_topic.jsonl"
            assert file_path.exists()

            with open(file_path) as f:
                lines = f.readlines()

            assert len(lines) > 0

    def test_write_stream_with_duration_timeout(self) -> None:
        """Test streaming stops when duration is exceeded (line 64)."""
        import time

        with tempfile.TemporaryDirectory() as tmpdir:
            sink = JsonFileSink(tmpdir)

            def infinite_gen() -> Iterator[dict]:
                """Infinite generator that would run forever without timeout."""
                i = 0
                while True:
                    yield {"id": i}
                    i += 1
                    time.sleep(0.01)

            # Short duration should stop the infinite generator
            sink.write_stream("timeout_test", infinite_gen(), rate_per_second=100, duration_seconds=0.05)

            file_path = Path(tmpdir) / "timeout_test.jsonl"
            assert file_path.exists()

            with open(file_path) as f:
                lines = f.readlines()

            # Should have written some records but stopped due to timeout
            assert len(lines) > 0
            assert len(lines) < 100  # Would be many more without timeout

    def test_close(self, capsys: pytest.CaptureFixture) -> None:
        """Test close method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = JsonFileSink(tmpdir)
            sink._counts = {"entity1": 5, "entity2": 10}

            sink.close()
            captured = capsys.readouterr()

            assert "JSON files written to" in captured.out
            assert "entity1: 5 records" in captured.out

    def test_to_dict_other_type(self) -> None:
        """Test converting non-dict, non-dataclass object."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = JsonFileSink(tmpdir)

            result = sink._to_dict(12345)
            assert result == {"value": "12345"}

    def test_serialize_nested_structures(self) -> None:
        """Test serializing complex nested structures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = JsonFileSink(tmpdir)

            data = {
                "decimal": Decimal("123.45"),
                "datetime": datetime(2024, 1, 15, 10, 0),
                "nested": {"inner_decimal": Decimal("67.89")},
                "list_of_decimals": [Decimal("1.0"), Decimal("2.0")],
            }

            result = sink._serialize_value(data)

            assert result["decimal"] == 123.45
            assert result["datetime"] == "2024-01-15T10:00:00"
            assert result["nested"]["inner_decimal"] == 67.89
            assert result["list_of_decimals"] == [1.0, 2.0]


class TestKafkaSinkMocked:
    """Tests for KafkaSink using mocks (no actual Kafka connection)."""

    def test_producer_config_dataclass(self) -> None:
        """Test ProducerConfig dataclass."""
        from data_gen.sinks.kafka import ProducerConfig

        config = ProducerConfig(
            bootstrap_servers="localhost:9092",
            acks="all",
            batch_size=16384,
            linger_ms=5,
            compression="snappy",
            retries=3,
        )

        assert config.bootstrap_servers == "localhost:9092"
        assert config.acks == "all"
        assert config.batch_size == 16384

    def test_producer_config_presets(self) -> None:
        """Test producer configuration presets."""
        from data_gen.sinks.kafka import EVENT_BY_EVENT, FAST, RELIABLE

        assert RELIABLE.acks == "all"
        assert FAST.acks == "0"
        assert EVENT_BY_EVENT.batch_size == 1

    def test_producer_stats(self) -> None:
        """Test ProducerStats dataclass."""
        from data_gen.sinks.kafka import ProducerStats

        stats = ProducerStats(sent=100, delivered=95, failed=5)

        assert stats.sent == 100
        assert stats.delivered == 95
        assert stats.failed == 5

    def test_producer_stats_success_rate(self) -> None:
        """Test ProducerStats success rate calculation."""
        from data_gen.sinks.kafka import ProducerStats

        stats = ProducerStats(sent=100, delivered=90, failed=10)
        assert stats.success_rate == 0.9

        # Edge case: no delivered or failed
        empty_stats = ProducerStats()
        assert empty_stats.success_rate == 0.0

    def test_producer_stats_throughput(self) -> None:
        """Test ProducerStats throughput calculation."""
        from data_gen.sinks.kafka import ProducerStats

        stats = ProducerStats(sent=100, start_time=0.0, end_time=10.0)
        assert stats.throughput == 10.0

        # Edge case: no timestamps
        no_time_stats = ProducerStats(sent=100)
        assert no_time_stats.throughput == 0.0

        # Edge case: same start and end
        zero_duration = ProducerStats(sent=100, start_time=5.0, end_time=5.0)
        assert zero_duration.throughput == 0.0

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_init_with_string(self, mock_producer_class: MagicMock) -> None:
        """Test KafkaSink initialization with string."""
        from data_gen.sinks.kafka import KafkaSink

        sink = KafkaSink("localhost:9092")

        assert sink.config.bootstrap_servers == "localhost:9092"
        mock_producer_class.assert_called_once()

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_init_with_config(self, mock_producer_class: MagicMock) -> None:
        """Test KafkaSink initialization with ProducerConfig."""
        from data_gen.sinks.kafka import KafkaSink, ProducerConfig

        config = ProducerConfig(bootstrap_servers="kafka:9092", acks="1")
        sink = KafkaSink(config)

        assert sink.config == config
        assert sink.config.acks == "1"

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_send(self, mock_producer_class: MagicMock) -> None:
        """Test sending a single record."""
        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")
        sink.send("test_topic", {"id": 1, "name": "test"}, key="key-1")

        mock_producer.produce.assert_called_once()
        assert sink.stats.sent == 1

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_send_without_key(self, mock_producer_class: MagicMock) -> None:
        """Test sending without a key."""
        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")
        sink.send("test_topic", {"id": 1})

        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["key"] is None

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_write_batch(self, mock_producer_class: MagicMock) -> None:
        """Test writing a batch of records."""
        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")
        records = [{"id": i} for i in range(10)]

        sink.write_batch("test_topic", records)

        assert mock_producer.produce.call_count == 10
        mock_producer.flush.assert_called_once()

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_delivery_callback_success(self, mock_producer_class: MagicMock) -> None:
        """Test delivery callback on success."""
        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")

        # Simulate successful delivery
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "test_topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 1

        sink._delivery_callback(None, mock_msg)

        assert sink.stats.delivered == 1
        assert sink.stats.failed == 0

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_delivery_callback_failure(self, mock_producer_class: MagicMock) -> None:
        """Test delivery callback on failure."""
        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")

        # Simulate failed delivery
        sink._delivery_callback("Connection error", None)

        assert sink.stats.delivered == 0
        assert sink.stats.failed == 1

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_flush(self, mock_producer_class: MagicMock) -> None:
        """Test flush method."""
        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")
        sink.flush(timeout=10.0)

        mock_producer.flush.assert_called_once_with(10.0)

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_close(self, mock_producer_class: MagicMock) -> None:
        """Test close method."""
        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")
        sink.close()

        mock_producer.flush.assert_called()

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_to_dict_dataclass(self, mock_producer_class: MagicMock) -> None:
        """Test converting dataclass to dict."""
        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")

        tx = Transaction(
            transaction_id="tx-001",
            account_id="acct-001",
            transaction_type="PIX",
            amount=Decimal("100.50"),
            direction="DEBIT",
            counterparty_key="12345678900",
            counterparty_name="Test Recipient",
            description="Test",
            timestamp=datetime(2024, 1, 15, 10, 0),
            status="COMPLETED",
        )

        result = sink._to_dict(tx)

        assert result["transaction_id"] == "tx-001"
        assert result["amount"] == 100.50
        assert result["timestamp"] == "2024-01-15T10:00:00"

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_to_dict_other(self, mock_producer_class: MagicMock) -> None:
        """Test converting non-dict/non-dataclass to dict."""
        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")

        result = sink._to_dict("string value")
        assert result == {"value": "string value"}

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_write_stream(self, mock_producer_class: MagicMock) -> None:
        """Test write_stream method (lines 166-203)."""
        import time

        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")

        records_generated = []

        def gen() -> Iterator[dict]:
            for i in range(10):
                record = {"id": i}
                records_generated.append(record)
                yield record

        stats = sink.write_stream("test_topic", gen(), rate_per_second=1000, duration_seconds=0.5)

        # Should have sent some records
        assert mock_producer.produce.call_count > 0
        assert stats.sent > 0
        mock_producer.flush.assert_called()

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_write_stream_with_timeout(self, mock_producer_class: MagicMock) -> None:
        """Test write_stream stops at duration limit."""
        import time

        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")

        def infinite_gen() -> Iterator[dict]:
            i = 0
            while True:
                yield {"id": i}
                i += 1
                time.sleep(0.01)

        stats = sink.write_stream("test_topic", infinite_gen(), rate_per_second=100, duration_seconds=0.05)

        # Should have stopped due to timeout
        assert stats.sent > 0
        assert stats.sent < 100  # Would be more without timeout

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_serialize_date(self, mock_producer_class: MagicMock) -> None:
        """Test serializing date values (line 242)."""
        from datetime import date

        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")

        result = sink._serialize_value(date(2024, 1, 15))
        assert result == "2024-01-15"

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_serialize_nested_dict(self, mock_producer_class: MagicMock) -> None:
        """Test serializing nested dict (line 244)."""
        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")

        data = {"outer": {"inner": Decimal("10.5")}}
        result = sink._serialize_value(data)
        assert result == {"outer": {"inner": 10.5}}

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_serialize_list(self, mock_producer_class: MagicMock) -> None:
        """Test serializing list values (line 246)."""
        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")

        data = [Decimal("1.0"), Decimal("2.0")]
        result = sink._serialize_value(data)
        assert result == [1.0, 2.0]

    @patch("data_gen.sinks.kafka.Producer")
    def test_kafka_sink_write_stream_progress_logging(self, mock_producer_class: MagicMock) -> None:
        """Test write_stream logs progress every 1000 records (line 187)."""
        from data_gen.sinks.kafka import KafkaSink

        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        sink = KafkaSink("localhost:9092")

        # Generate more than 1000 records to trigger progress logging
        def gen() -> Iterator[dict]:
            for i in range(1500):
                yield {"id": i}

        stats = sink.write_stream("test_topic", gen(), rate_per_second=0, duration_seconds=60)

        # Should have sent all records
        assert stats.sent == 1500
        assert mock_producer.produce.call_count == 1500


class TestPostgresSinkMocked:
    """Tests for PostgresSink using mocks."""

    def test_table_columns_defined(self) -> None:
        """Test that TABLE_COLUMNS are defined for all entities."""
        from data_gen.sinks.postgres import PostgresSink

        expected_tables = [
            "customers",
            "accounts",
            "transactions",
            "credit_cards",
            "card_transactions",
            "loans",
            "installments",
            "properties",
        ]

        for table in expected_tables:
            assert table in PostgresSink.TABLE_COLUMNS

    def test_entity_order_defined(self) -> None:
        """Test ENTITY_ORDER for FK constraints."""
        from data_gen.sinks.postgres import PostgresSink

        # Properties should come before loans (loans reference properties)
        props_idx = PostgresSink.ENTITY_ORDER.index("properties")
        loans_idx = PostgresSink.ENTITY_ORDER.index("loans")
        assert props_idx < loans_idx

        # Customers should come first
        assert PostgresSink.ENTITY_ORDER[0] == "customers"

    def _create_postgres_sink_with_mock(self) -> tuple:
        """Helper to create PostgresSink with mocked psycopg."""
        import sys

        mock_psycopg = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg.connect.return_value = mock_conn

        # Save original
        original = sys.modules.get("psycopg")

        # Install mock
        sys.modules["psycopg"] = mock_psycopg

        return mock_psycopg, mock_conn, mock_cursor, original

    def _cleanup_psycopg_mock(self, original: Any) -> None:
        """Clean up psycopg mock."""
        import sys

        if original is not None:
            sys.modules["psycopg"] = original
        else:
            sys.modules.pop("psycopg", None)

    def test_postgres_sink_init(self) -> None:
        """Test PostgresSink initialization."""
        mock_psycopg, mock_conn, _, original = self._create_postgres_sink_with_mock()

        try:
            from data_gen.sinks.postgres import PostgresSink

            sink = PostgresSink("postgresql://user:pass@localhost/db")

            mock_psycopg.connect.assert_called_with("postgresql://user:pass@localhost/db")
            assert sink.conn == mock_conn
        finally:
            self._cleanup_psycopg_mock(original)

    def test_postgres_sink_write_batch_empty(self) -> None:
        """Test writing empty batch does nothing."""
        _, mock_conn, _, original = self._create_postgres_sink_with_mock()

        try:
            from data_gen.sinks.postgres import PostgresSink

            sink = PostgresSink("postgresql://user:pass@localhost/db")
            mock_conn.cursor.reset_mock()  # Reset after init
            sink.write_batch("customers", [])

            # Cursor should not be used for empty batch
            mock_conn.cursor.assert_not_called()
        finally:
            self._cleanup_psycopg_mock(original)

    def test_postgres_sink_write_batch_unknown_entity(self) -> None:
        """Test writing unknown entity type logs warning."""
        _, mock_conn, _, original = self._create_postgres_sink_with_mock()

        try:
            from data_gen.sinks.postgres import PostgresSink

            sink = PostgresSink("postgresql://user:pass@localhost/db")
            mock_conn.cursor.reset_mock()
            sink.write_batch("unknown_entity", [{"id": 1}])

            # Should not crash, cursor should not be used
            mock_conn.cursor.assert_not_called()
        finally:
            self._cleanup_psycopg_mock(original)

    def test_postgres_sink_write_batch_dict(self) -> None:
        """Test writing batch of dicts."""
        _, mock_conn, mock_cursor, original = self._create_postgres_sink_with_mock()

        try:
            from data_gen.sinks.postgres import PostgresSink

            sink = PostgresSink("postgresql://user:pass@localhost/db")
            mock_conn.commit.reset_mock()

            records = [
                {
                    "customer_id": "cust-001",
                    "cpf": "123.456.789-00",
                    "name": "Test",
                    "email": "test@test.com",
                    "phone": "+55",
                    "street": "Test",
                    "number": "1",
                    "complement": None,
                    "neighborhood": "Test",
                    "city": "Test",
                    "state": "SP",
                    "postal_code": "00000-000",
                    "monthly_income": Decimal("5000.00"),
                    "employment_status": "EMPLOYED",
                    "credit_score": 700,
                    "created_at": datetime.now(),
                }
            ]

            sink.write_batch("customers", records)

            mock_cursor.executemany.assert_called_once()
            mock_conn.commit.assert_called_once()
        finally:
            self._cleanup_psycopg_mock(original)

    def test_postgres_sink_write_batch_dataclass_with_address(self) -> None:
        """Test writing batch flattens nested address."""
        _, mock_conn, mock_cursor, original = self._create_postgres_sink_with_mock()

        try:
            from data_gen.sinks.postgres import PostgresSink

            sink = PostgresSink("postgresql://user:pass@localhost/db")

            customer = Customer(
                customer_id="cust-001",
                cpf="123.456.789-00",
                name="Test",
                email="test@test.com",
                phone="+5511999999999",
                address=Address(
                    street="Test Street",
                    number="100",
                    neighborhood="Centro",
                    city="São Paulo",
                    state="SP",
                    postal_code="01234-567",
                    complement="Apt 1",
                ),
                monthly_income=Decimal("5000.00"),
                employment_status="EMPLOYED",
                credit_score=700,
                created_at=datetime.now(),
            )

            sink.write_batch("customers", [customer])

            mock_cursor.executemany.assert_called()
            # The SQL should contain the flattened address fields
            sql = mock_cursor.executemany.call_args[0][0]
            assert "street" in sql
            assert "city" in sql
        finally:
            self._cleanup_psycopg_mock(original)

    def test_postgres_sink_close(self) -> None:
        """Test close method."""
        _, mock_conn, _, original = self._create_postgres_sink_with_mock()

        try:
            from data_gen.sinks.postgres import PostgresSink

            sink = PostgresSink("postgresql://user:pass@localhost/db")
            sink._counts = {"customers": 10}
            sink.close()

            mock_conn.close.assert_called_once()
        finally:
            self._cleanup_psycopg_mock(original)

    def test_postgres_sink_create_tables(self) -> None:
        """Test create_tables method."""
        _, mock_conn, mock_cursor, original = self._create_postgres_sink_with_mock()

        try:
            from data_gen.sinks.postgres import PostgresSink

            sink = PostgresSink("postgresql://user:pass@localhost/db")
            mock_conn.commit.reset_mock()
            sink.create_tables()

            mock_cursor.execute.assert_called()
            mock_conn.commit.assert_called()

            # Check DDL contains expected tables
            ddl = mock_cursor.execute.call_args[0][0]
            assert "CREATE TABLE IF NOT EXISTS customers" in ddl
            assert "CREATE TABLE IF NOT EXISTS accounts" in ddl
            assert "CREATE TABLE IF NOT EXISTS loans" in ddl
        finally:
            self._cleanup_psycopg_mock(original)

    def test_postgres_sink_write_stream(self) -> None:
        """Test write_stream method."""
        _, mock_conn, _, original = self._create_postgres_sink_with_mock()

        try:
            from data_gen.sinks.postgres import PostgresSink

            sink = PostgresSink("postgresql://user:pass@localhost/db")
            mock_conn.commit.reset_mock()

            def gen() -> Iterator[dict]:
                for i in range(5):
                    yield {
                        "transaction_id": f"tx-{i}",
                        "account_id": "acct-001",
                        "transaction_type": "PIX",
                        "amount": Decimal("100.00"),
                        "direction": "DEBIT",
                        "counterparty_key": None,
                        "counterparty_name": None,
                        "description": "Test",
                        "timestamp": datetime.now(),
                        "status": "COMPLETED",
                        "pix_e2e_id": None,
                        "pix_key_type": None,
                    }

            sink.write_stream(
                "dev.financial.transactions.created.v1",
                gen(),
                rate_per_second=1000,
                duration_seconds=0.1,
            )

            # Should have written batches
            assert mock_conn.commit.called
        finally:
            self._cleanup_psycopg_mock(original)

    def test_postgres_sink_write_stream_unknown_topic(self) -> None:
        """Test write_stream with unknown topic."""
        _, mock_conn, _, original = self._create_postgres_sink_with_mock()

        try:
            from data_gen.sinks.postgres import PostgresSink

            sink = PostgresSink("postgresql://user:pass@localhost/db")

            def gen() -> Iterator[dict]:
                yield {"id": 1}

            # Short topic name that can't be parsed
            sink.write_stream("unknown", gen(), rate_per_second=100, duration_seconds=0.1)

            # Should not crash
        finally:
            self._cleanup_psycopg_mock(original)

    def test_postgres_import_error(self) -> None:
        """Test ImportError when psycopg not installed (lines 149-150)."""
        import sys

        # Remove psycopg from sys.modules to force re-import attempt
        psycopg_backup = sys.modules.pop("psycopg", None)

        try:
            # Patch the import mechanism within PostgresSink.__init__
            with patch.dict(sys.modules, {"psycopg": None}):
                from data_gen.sinks.postgres import PostgresSink

                # When psycopg is None in sys.modules, import will fail
                # We need to actually trigger the import inside __init__
                # The trick is to have sys.modules["psycopg"] = None which
                # makes `import psycopg` raise ImportError

                with pytest.raises(ImportError, match="psycopg is required"):
                    PostgresSink("postgresql://user:pass@localhost/db")
        finally:
            # Restore psycopg
            if psycopg_backup:
                sys.modules["psycopg"] = psycopg_backup

    def test_postgres_write_stream_duration_break(self) -> None:
        """Test write_stream stops at duration limit (line 210)."""
        import time

        _, mock_conn, _, original = self._create_postgres_sink_with_mock()

        try:
            from data_gen.sinks.postgres import PostgresSink

            sink = PostgresSink("postgresql://user:pass@localhost/db")

            def infinite_gen() -> Iterator[dict]:
                i = 0
                while True:
                    yield {
                        "transaction_id": f"tx-{i}",
                        "account_id": "acct-001",
                        "transaction_type": "PIX",
                        "amount": Decimal("100.00"),
                        "direction": "DEBIT",
                        "counterparty_key": None,
                        "counterparty_name": None,
                        "description": "Test",
                        "timestamp": datetime.now(),
                        "status": "COMPLETED",
                        "pix_e2e_id": None,
                        "pix_key_type": None,
                    }
                    i += 1
                    time.sleep(0.01)

            # Should stop due to duration timeout
            sink.write_stream(
                "dev.financial.transactions.created.v1",
                infinite_gen(),
                rate_per_second=100,
                duration_seconds=0.05,
            )

            # Should not hang forever - test passes if we get here
        finally:
            self._cleanup_psycopg_mock(original)

    def test_postgres_write_stream_batch_threshold(self) -> None:
        """Test write_stream batching when batch size reached (lines 214-216)."""
        _, mock_conn, _, original = self._create_postgres_sink_with_mock()

        try:
            from data_gen.sinks.postgres import PostgresSink

            sink = PostgresSink("postgresql://user:pass@localhost/db")

            # Generate more than 1000 records to trigger batch write
            def gen() -> Iterator[dict]:
                for i in range(1500):
                    yield {
                        "transaction_id": f"tx-{i}",
                        "account_id": "acct-001",
                        "transaction_type": "PIX",
                        "amount": Decimal("100.00"),
                        "direction": "DEBIT",
                        "counterparty_key": None,
                        "counterparty_name": None,
                        "description": "Test",
                        "timestamp": datetime.now(),
                        "status": "COMPLETED",
                        "pix_e2e_id": None,
                        "pix_key_type": None,
                    }

            sink.write_stream(
                "dev.financial.transactions.created.v1",
                gen(),
                rate_per_second=0,  # No rate limiting
                duration_seconds=10,  # Long enough to process all
            )

            # Should have called commit multiple times (batch write)
            assert mock_conn.commit.call_count >= 1
        finally:
            self._cleanup_psycopg_mock(original)

    def test_postgres_extract_row_unsupported_type(self) -> None:
        """Test _extract_row raises error for unsupported types (line 362)."""
        _, _, _, original = self._create_postgres_sink_with_mock()

        try:
            from data_gen.sinks.postgres import PostgresSink

            sink = PostgresSink("postgresql://user:pass@localhost/db")

            # Pass a string which is neither dataclass nor dict
            with pytest.raises(ValueError, match="Unsupported record type"):
                sink._extract_row("invalid_record", ["col1"], "customers")
        finally:
            self._cleanup_psycopg_mock(original)

    def test_postgres_extract_row_with_date(self) -> None:
        """Test _extract_row handles date values (line 378)."""
        from datetime import date

        _, _, _, original = self._create_postgres_sink_with_mock()

        try:
            from data_gen.sinks.postgres import PostgresSink

            sink = PostgresSink("postgresql://user:pass@localhost/db")

            record = {
                "loan_id": "loan-001",
                "due_date": date(2024, 1, 15),
                "amount": Decimal("500.00"),
            }

            columns = ["loan_id", "due_date", "amount"]
            row = sink._extract_row(record, columns, "installments")

            assert row[0] == "loan-001"
            assert row[1] == date(2024, 1, 15)
            assert abs(row[2] - 500.00) < 0.01  # Decimal converted to float
        finally:
            self._cleanup_psycopg_mock(original)


class TestSinksInit:
    """Tests for sinks __init__.py exports."""

    def test_all_sinks_exported(self) -> None:
        """Test that all sinks are exported."""
        from data_gen.sinks import ConsoleSink, JsonFileSink, KafkaSink, PostgresSink

        assert ConsoleSink is not None
        assert JsonFileSink is not None
        assert KafkaSink is not None
        assert PostgresSink is not None
