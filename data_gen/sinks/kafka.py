"""Kafka sink for streaming data to Kafka topics."""

import json
import logging
import time
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterator

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField

logger = logging.getLogger(__name__)

# Constants
SCHEMA_NAMESPACE = "com.datagen.banking"
DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081"

# Avro schemas for each entity type
AVRO_SCHEMAS = {
    "transactions": {
        "type": "record",
        "name": "Transaction",
        "namespace": SCHEMA_NAMESPACE,
        "fields": [
            {"name": "transaction_id", "type": "string"},
            {"name": "account_id", "type": "string"},
            {"name": "transaction_type", "type": "string"},
            {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}},
            {"name": "direction", "type": "string"},
            {"name": "counterparty_key", "type": ["null", "string"], "default": None},
            {"name": "counterparty_name", "type": ["null", "string"], "default": None},
            {"name": "description", "type": ["null", "string"], "default": None},
            {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "status", "type": "string"},
            {"name": "pix_e2e_id", "type": ["null", "string"], "default": None},
            {"name": "pix_key_type", "type": ["null", "string"], "default": None},
        ],
    },
    "card_transactions": {
        "type": "record",
        "name": "CardTransaction",
        "namespace": SCHEMA_NAMESPACE,
        "fields": [
            {"name": "transaction_id", "type": "string"},
            {"name": "card_id", "type": "string"},
            {"name": "merchant_name", "type": "string"},
            {"name": "merchant_category", "type": "string"},
            {"name": "mcc_code", "type": "string"},
            {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}},
            {"name": "installments", "type": "int"},
            {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "status", "type": "string"},
            {"name": "location_city", "type": ["null", "string"], "default": None},
            {"name": "location_country", "type": ["null", "string"], "default": None},
        ],
    },
    "trades": {
        "type": "record",
        "name": "Trade",
        "namespace": SCHEMA_NAMESPACE,
        "fields": [
            {"name": "trade_id", "type": "string"},
            {"name": "account_id", "type": "string"},
            {"name": "stock_id", "type": "string"},
            {"name": "ticker", "type": "string"},
            {"name": "trade_type", "type": "string"},
            {"name": "quantity", "type": "int"},
            {"name": "price_per_share", "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}},
            {"name": "total_amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}},
            {"name": "fees", "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}},
            {"name": "net_amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}},
            {"name": "order_type", "type": "string"},
            {"name": "status", "type": "string"},
            {"name": "executed_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "settlement_date", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        ],
    },
    "installments": {
        "type": "record",
        "name": "Installment",
        "namespace": SCHEMA_NAMESPACE,
        "fields": [
            {"name": "installment_id", "type": "string"},
            {"name": "loan_id", "type": "string"},
            {"name": "installment_number", "type": "int"},
            {"name": "due_date", "type": {"type": "int", "logicalType": "date"}},
            {"name": "principal_amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}},
            {"name": "interest_amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}},
            {"name": "total_amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}},
            {"name": "paid_date", "type": ["null", {"type": "int", "logicalType": "date"}], "default": None},
            {"name": "paid_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}], "default": None},
            {"name": "status", "type": "string"},
        ],
    },
}


@dataclass
class ProducerConfig:
    """Kafka producer configuration."""

    bootstrap_servers: str
    schema_registry_url: str | None = None
    acks: str = "all"  # "0", "1", "all"
    batch_size: int = 16384  # bytes
    linger_ms: int = 5  # ms to wait for batching
    compression: str = "snappy"  # none, gzip, snappy, lz4
    retries: int = 3


# Configuration presets
RELIABLE = ProducerConfig(
    bootstrap_servers="localhost:9092",
    schema_registry_url=DEFAULT_SCHEMA_REGISTRY_URL,
    acks="all",
    batch_size=16384,
    linger_ms=5,
)

FAST = ProducerConfig(
    bootstrap_servers="localhost:9092",
    schema_registry_url=DEFAULT_SCHEMA_REGISTRY_URL,
    acks="0",
    batch_size=65536,
    linger_ms=50,
)

EVENT_BY_EVENT = ProducerConfig(
    bootstrap_servers="localhost:9092",
    schema_registry_url=DEFAULT_SCHEMA_REGISTRY_URL,
    acks="all",
    batch_size=1,
    linger_ms=0,
)


@dataclass
class ProducerStats:
    """Track producer delivery statistics."""

    sent: int = 0
    delivered: int = 0
    failed: int = 0
    start_time: float | None = None
    end_time: float | None = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        total = self.delivered + self.failed
        return self.delivered / total if total > 0 else 0.0

    @property
    def throughput(self) -> float:
        """Calculate events per second achieved."""
        if self.start_time is None or self.end_time is None:
            return 0.0
        duration = self.end_time - self.start_time
        return self.sent / duration if duration > 0 else 0.0


class KafkaSink:
    """Output data to Kafka topics with rate control."""

    # Topic to key field mapping
    KEY_FIELDS = {
        "banking.transactions": "account_id",
        "banking.card-transactions": "card_id",
        "banking.trades": "account_id",
        "banking.installments": "loan_id",
    }

    def __init__(self, config: ProducerConfig | str) -> None:
        """Initialize Kafka sink.

        Parameters
        ----------
        config : ProducerConfig | str
            Producer configuration or bootstrap servers string.
        """
        if isinstance(config, str):
            config = ProducerConfig(bootstrap_servers=config)

        self.config = config
        self.producer = self._create_producer()
        self.stats = ProducerStats()
        self._avro_serializers: dict[str, Any] = {}

        # Initialize Avro serializers if Schema Registry is configured
        if config.schema_registry_url:
            self._init_avro_serializers()

    def _create_producer(self) -> Producer:
        """Create Kafka producer with configuration."""
        return Producer(
            {
                "bootstrap.servers": self.config.bootstrap_servers,
                "acks": self.config.acks,
                "retries": self.config.retries,
                "linger.ms": self.config.linger_ms,
                "batch.size": self.config.batch_size,
                "compression.type": self.config.compression,
            }
        )

    def _init_avro_serializers(self) -> None:
        """Initialize Avro serializers for each entity type."""
        try:
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroSerializer

            schema_registry_client = SchemaRegistryClient(
                {"url": self.config.schema_registry_url}
            )

            for entity_type, schema in AVRO_SCHEMAS.items():
                schema_str = json.dumps(schema)
                self._avro_serializers[entity_type] = AvroSerializer(
                    schema_registry_client,
                    schema_str,
                    to_dict=self._to_avro_dict,
                )
            logger.info("Avro serializers initialized for: %s", list(AVRO_SCHEMAS.keys()))
        except ImportError:
            logger.warning(
                "confluent-kafka[avro] not installed. Using JSON serialization. "
                "Install with: pip install 'confluent-kafka[avro]'"
            )

    def _to_avro_dict(self, obj: Any, ctx: SerializationContext) -> dict:
        """Convert object to Avro-compatible dict."""
        if is_dataclass(obj):
            data = asdict(obj)
        elif isinstance(obj, dict):
            data = obj
        else:
            raise ValueError(f"Cannot convert {type(obj)} to Avro dict")

        # Convert types for Avro
        result = {}
        for key, value in data.items():
            if isinstance(value, Decimal):
                # Avro decimal - convert to scaled integer bytes
                # For precision=15, scale=2: multiply by 100 and encode as big-endian bytes
                scaled = int(value * 100)
                # Calculate minimum bytes needed (at least 1 byte)
                byte_length = max(1, (scaled.bit_length() + 8) // 8)
                result[key] = scaled.to_bytes(byte_length, byteorder='big', signed=True)
            elif isinstance(value, datetime):
                # Avro timestamp-millis
                result[key] = int(value.timestamp() * 1000)
            elif isinstance(value, date):
                # Avro date (days since epoch)
                result[key] = (value - date(1970, 1, 1)).days
            else:
                result[key] = value
        return result

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Handle delivery reports."""
        if err:
            self.stats.failed += 1
            logger.error("Delivery failed: %s", err)
        else:
            self.stats.delivered += 1
            logger.debug("Delivered to %s[%d]@%d", msg.topic(), msg.partition(), msg.offset())

    def _get_key(self, topic: str, record: Any) -> str | None:
        """Extract message key from record based on topic."""
        key_field = self.KEY_FIELDS.get(topic)
        if not key_field:
            return None

        if is_dataclass(record):
            return getattr(record, key_field, None)
        elif isinstance(record, dict):
            return record.get(key_field)
        return None

    def _get_entity_type(self, topic: str) -> str | None:
        """Extract entity type from topic name."""
        # banking.transactions -> transactions
        # banking.card-transactions -> card_transactions
        if "." in topic:
            entity = topic.split(".")[-1]
            return entity.replace("-", "_")
        return topic

    def send(self, topic: str, record: Any, key: str | None = None) -> None:
        """Send a single record to Kafka topic."""
        entity_type = self._get_entity_type(topic)

        # Use Avro serialization if available
        if entity_type and entity_type in self._avro_serializers:
            serializer = self._avro_serializers[entity_type]
            ctx = SerializationContext(topic, MessageField.VALUE)
            value = serializer(record, ctx)
        else:
            # Fall back to JSON
            data = self._to_dict(record)
            value = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")

        # Extract key if not provided
        if key is None:
            key = self._get_key(topic, record)

        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8") if key else None,
            value=value,
            callback=self._delivery_callback,
        )
        self.stats.sent += 1
        self.producer.poll(0)

    def write_batch(self, topic: str, records: list[Any]) -> None:
        """Write a batch of records to a Kafka topic."""
        logger.info("Writing batch to %s: %d records", topic, len(records))

        for record in records:
            self.send(topic, record)

        self.flush()
        logger.info("Batch complete: sent=%d, delivered=%d, failed=%d",
                    self.stats.sent, self.stats.delivered, self.stats.failed)

    def write_stream(
        self,
        topic: str,
        generator: Iterator[Any],
        rate_per_second: float,
        duration_seconds: float,
    ) -> ProducerStats:
        """Stream records at a fixed rate for a duration.

        Parameters
        ----------
        topic : str
            Kafka topic name.
        generator : Iterator[Any]
            Data generator (infinite or finite).
        rate_per_second : float
            Target events per second (e.g., 100).
        duration_seconds : float
            How long to stream (e.g., 600 for 10 minutes).

        Returns
        -------
        ProducerStats
            Delivery statistics.
        """
        logger.info(
            "Starting stream to %s: rate=%.1f/sec, duration=%.1fs",
            topic,
            rate_per_second,
            duration_seconds,
        )

        self.stats = ProducerStats()
        self.stats.start_time = time.time()

        interval = 1.0 / rate_per_second if rate_per_second > 0 else 0
        target_count = int(rate_per_second * duration_seconds)

        for i, record in enumerate(generator):
            if time.time() - self.stats.start_time >= duration_seconds:
                break

            self.send(topic, record)

            # Log progress every 1000 records
            if (i + 1) % 1000 == 0:
                logger.debug("Progress: %d/%d records", i + 1, target_count)

            if interval > 0:
                time.sleep(interval)

        self.flush()
        self.stats.end_time = time.time()

        logger.info(
            "Stream complete: sent=%d, delivered=%d, failed=%d, throughput=%.1f/sec",
            self.stats.sent,
            self.stats.delivered,
            self.stats.failed,
            self.stats.throughput,
        )

        return self.stats

    def flush(self, timeout: float = 30.0) -> None:
        """Flush pending messages."""
        self.producer.flush(timeout)

    def close(self) -> None:
        """Flush and close the producer."""
        self.flush()
        logger.info(
            "Kafka sink closed: sent=%d, delivered=%d, failed=%d",
            self.stats.sent,
            self.stats.delivered,
            self.stats.failed,
        )

    def _to_dict(self, obj: Any) -> dict:
        """Convert object to dictionary."""
        if is_dataclass(obj):
            return self._dataclass_to_dict(obj)
        elif isinstance(obj, dict):
            return obj
        else:
            return {"value": str(obj)}

    def _dataclass_to_dict(self, obj: Any) -> dict:
        """Convert dataclass to dict with proper serialization."""
        result = {}
        for key, value in asdict(obj).items():
            result[key] = self._serialize_value(value)
        return result

    def _serialize_value(self, value: Any) -> Any:
        """Serialize a value for JSON output."""
        if isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, date):
            return value.isoformat()
        elif isinstance(value, dict):
            return {k: self._serialize_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._serialize_value(v) for v in value]
        return value
