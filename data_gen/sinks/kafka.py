"""Kafka sink for streaming data to Kafka topics."""

import io
import json
import logging
import os
import struct
import time
import uuid
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Iterator

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField

from data_gen.sinks.serialization import to_dict, to_dict_fast

logger = logging.getLogger(__name__)

# Constants
SCHEMA_NAMESPACE = "com.financial.banking"
DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081"

# CloudEvents configuration (Binary Content Mode)
CLOUDEVENTS_SPEC_VERSION = "1.0"

# Event type mapping (CloudEvents type attribute)
EVENT_TYPES = {
    "transactions": "com.financial.transaction.created.v1",
    "card_transactions": "com.financial.card_transaction.created.v1",
    "trades": "com.financial.trade.executed.v1",
    "installments": "com.financial.installment.created.v1",
}

# Source URI mapping (CloudEvents source attribute)
SOURCE_URIS = {
    "transactions": "/financial/banking/transactions",
    "card_transactions": "/financial/banking/cards",
    "trades": "/financial/banking/investments",
    "installments": "/financial/banking/loans",
}

# Avro schemas for each entity type
AVRO_SCHEMAS = {
    "transactions": {
        "type": "record",
        "name": "Transaction",
        "namespace": SCHEMA_NAMESPACE,
        "fields": [
            {"name": "transaction_id", "type": "string"},
            {"name": "account_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
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
            {"name": "location_lat", "type": ["null", "double"], "default": None},
            {"name": "location_lon", "type": ["null", "double"], "default": None},
            {"name": "created_at", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
            {"name": "updated_at", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
        ],
    },
    "card_transactions": {
        "type": "record",
        "name": "CardTransaction",
        "namespace": SCHEMA_NAMESPACE,
        "fields": [
            {"name": "transaction_id", "type": "string"},
            {"name": "card_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "merchant_name", "type": "string"},
            {"name": "merchant_category", "type": "string"},
            {"name": "mcc_code", "type": "string"},
            {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}},
            {"name": "installments", "type": "int"},
            {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "status", "type": "string"},
            {"name": "location_city", "type": ["null", "string"], "default": None},
            {"name": "location_country", "type": ["null", "string"], "default": None},
            {"name": "created_at", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
            {"name": "updated_at", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
        ],
    },
    "trades": {
        "type": "record",
        "name": "Trade",
        "namespace": SCHEMA_NAMESPACE,
        "fields": [
            {"name": "trade_id", "type": "string"},
            {"name": "account_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
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
            {"name": "created_at", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
            {"name": "updated_at", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
        ],
    },
    "installments": {
        "type": "record",
        "name": "Installment",
        "namespace": SCHEMA_NAMESPACE,
        "fields": [
            {"name": "installment_id", "type": "string"},
            {"name": "loan_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "installment_number", "type": "int"},
            {"name": "due_date", "type": {"type": "int", "logicalType": "date"}},
            {"name": "principal_amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}},
            {"name": "interest_amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}},
            {"name": "total_amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}},
            {"name": "paid_date", "type": ["null", {"type": "int", "logicalType": "date"}], "default": None},
            {"name": "paid_amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": 2}], "default": None},
            {"name": "status", "type": "string"},
            {"name": "created_at", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
            {"name": "updated_at", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": None},
        ],
    },
}

# Pre-encode static CloudEvents header values (computed once at module load)
_CE_SPECVERSION_BYTES = CLOUDEVENTS_SPEC_VERSION.encode("utf-8")
_CE_CONTENT_TYPE_BYTES = b"application/json"

# Pre-encoded per-topic static headers: (ce_specversion, ce_type, ce_source, content-type)
_STATIC_CE_HEADERS: dict[str, list[tuple[str, bytes]]] = {}
for _entity, _event_type in EVENT_TYPES.items():
    _source = SOURCE_URIS.get(_entity, f"/financial/banking/{_entity}")
    _STATIC_CE_HEADERS[_entity] = [
        ("ce_specversion", _CE_SPECVERSION_BYTES),
        ("ce_type", _event_type.encode("utf-8")),
        ("ce_source", _source.encode("utf-8")),
        ("content-type", _CE_CONTENT_TYPE_BYTES),
    ]

# Epoch reference for date conversion
_EPOCH_DATE = date(1970, 1, 1)

# UUID pool for fast CloudEvents ce_id generation
_UUID_POOL_SIZE = 4096
_uuid_pool: list[bytes] = []
_uuid_pool_idx = 0


def _refill_uuid_pool() -> None:
    """Batch-generate UUID bytes to avoid per-message syscalls."""
    global _uuid_pool, _uuid_pool_idx
    raw = os.urandom(16 * _UUID_POOL_SIZE)
    _uuid_pool = [
        uuid.UUID(bytes=raw[i:i + 16], version=4).hex.encode("ascii")
        for i in range(0, len(raw), 16)
    ]
    _uuid_pool_idx = 0


def _fast_uuid_bytes() -> bytes:
    """Return a pre-generated UUID hex as bytes, refilling pool when exhausted."""
    global _uuid_pool_idx
    if _uuid_pool_idx >= len(_uuid_pool):
        _refill_uuid_pool()
    val = _uuid_pool[_uuid_pool_idx]
    _uuid_pool_idx += 1
    return val


# Initialize the pool
_refill_uuid_pool()


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
    enable_idempotence: bool = False
    queue_buffering_max_messages: int = 100000
    queue_buffering_max_kbytes: int = 1048576  # 1GB


# Configuration presets
RELIABLE = ProducerConfig(
    bootstrap_servers="localhost:9092",
    schema_registry_url=DEFAULT_SCHEMA_REGISTRY_URL,
    acks="all",
    batch_size=16384,
    linger_ms=5,
    enable_idempotence=True,
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

BULK = ProducerConfig(
    bootstrap_servers="localhost:9092",
    schema_registry_url=DEFAULT_SCHEMA_REGISTRY_URL,
    acks="0",
    batch_size=1048576,  # 1MB
    linger_ms=100,  # 100ms — allows fuller batches with acks=0
    compression="lz4",
    queue_buffering_max_messages=1000000,  # 1M
    queue_buffering_max_kbytes=2097152,  # 2GB
)

STREAMING = ProducerConfig(
    bootstrap_servers="localhost:9092",
    schema_registry_url=DEFAULT_SCHEMA_REGISTRY_URL,
    acks="1",
    batch_size=32768,  # 32KB — moderate batching for steady-state streaming
    linger_ms=20,  # 20ms — accumulates ~20 events at 1000/sec per batch
    compression="snappy",
    queue_buffering_max_messages=500000,
    queue_buffering_max_kbytes=1048576,  # 1GB
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


class FastAvroSerializer:
    """Minimal Avro serializer that bypasses AvroSerializer per-message overhead.

    Pre-computes the 5-byte schema ID prefix once during init, then directly
    calls fastavro.schemaless_writer with a reusable buffer. Eliminates:
    - subject_name_func call per message
    - _get_reader_schema call per message
    - _known_subjects set lookup per message
    - _ContextStringIO allocation per message
    - prefix_schema_id_serializer BytesIO concatenation per message
    """

    def __init__(
        self,
        schema_registry_client: Any,
        schema_str: str,
        subject_name: str,
        avro_field_set: set[str],
    ) -> None:
        from confluent_kafka.schema_registry import Schema
        from fastavro import parse_schema
        from fastavro.write import schemaless_writer

        self._schemaless_writer = schemaless_writer

        # Register schema once and cache the 5-byte Confluent Wire Format prefix
        schema_obj = Schema(schema_str, "AVRO")
        registered = schema_registry_client.register_schema(subject_name, schema_obj)
        self._prefix = struct.pack(">bI", 0, registered)
        self._parsed_schema = parse_schema(json.loads(schema_str))
        self._buffer = io.BytesIO()
        self._field_set = avro_field_set

    def serialize(self, avro_dict: dict) -> bytes:
        """Serialize an already-converted Avro dict to bytes."""
        buf = self._buffer
        buf.seek(0)
        buf.truncate()
        buf.write(self._prefix)
        self._schemaless_writer(buf, self._parsed_schema, avro_dict)
        return buf.getvalue()


class KafkaSink:
    """Output data to Kafka topics with rate control."""

    # Topic to key field mapping
    KEY_FIELDS = {
        "banking.transactions": "account_id",
        "banking.card-transactions": "card_id",
        "banking.trades": "account_id",
        "banking.installments": "loan_id",
    }

    def __init__(
        self,
        config: ProducerConfig | str,
        use_cloudevents: bool = True,
        poll_interval: int = 10000,
    ) -> None:
        """Initialize Kafka sink.

        Parameters
        ----------
        config : ProducerConfig | str
            Producer configuration or bootstrap servers string.
        use_cloudevents : bool
            If True, include CloudEvents headers in messages (default: True).
        poll_interval : int
            Poll the producer every N messages instead of every message.
            Lower values = more responsive delivery callbacks.
            Higher values = better throughput (default: 10000).
        """
        if isinstance(config, str):
            config = ProducerConfig(bootstrap_servers=config)

        self.config = config
        self.use_cloudevents = use_cloudevents
        self.producer = self._create_producer()
        self.stats = ProducerStats()
        self._avro_serializers: dict[str, Any] = {}
        self._fast_serializers: dict[str, FastAvroSerializer] = {}
        self._poll_interval = poll_interval
        self._since_last_poll = 0
        self._avro_field_cache: dict[str, set[str]] = {}
        self._entity_type_cache: dict[str, str | None] = {}

        # Pre-cached CloudEvents static headers per topic
        self._ce_static_cache: dict[str, list[tuple[str, bytes]]] = {}

        # Cached timestamp for CloudEvents (updated every 1ms)
        self._ce_timestamp_bytes: bytes = b""
        self._ce_timestamp_updated: float = 0.0

        # Initialize Avro serializers if Schema Registry is configured
        if config.schema_registry_url:
            self._init_avro_serializers()

        if use_cloudevents:
            logger.info("CloudEvents headers enabled (Binary Content Mode)")

    def _create_producer(self) -> Producer:
        """Create Kafka producer with configuration."""
        conf: dict[str, Any] = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "acks": self.config.acks,
            "retries": self.config.retries,
            "linger.ms": self.config.linger_ms,
            "batch.size": self.config.batch_size,
            "compression.type": self.config.compression,
            "queue.buffering.max.messages": self.config.queue_buffering_max_messages,
            "queue.buffering.max.kbytes": self.config.queue_buffering_max_kbytes,
            # Disable Nagle's algorithm for lower latency batch sends
            "socket.nagle.disable": True,
            "socket.send.buffer.bytes": 1048576,  # 1MB socket send buffer
            "message.send.max.retries": 0 if self.config.acks == "0" else self.config.retries,
        }
        if self.config.enable_idempotence:
            conf["enable.idempotence"] = True
        return Producer(conf)

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
                # Pre-cache the field set for O(1) lookup during serialization
                field_set = {f["name"] for f in schema["fields"]}
                self._avro_field_cache[entity_type] = field_set

                # Legacy AvroSerializer (kept for compatibility with write_stream/write_batch)
                self._avro_serializers[entity_type] = AvroSerializer(
                    schema_registry_client,
                    schema_str,
                    to_dict=self._to_avro_dict,
                )

                # Fast serializer: bypasses per-message overhead
                topic_map = {
                    "transactions": "banking.transactions",
                    "card_transactions": "banking.card-transactions",
                    "trades": "banking.trades",
                    "installments": "banking.installments",
                }
                topic_name = topic_map.get(entity_type, f"banking.{entity_type}")
                subject_name = f"{topic_name}-value"
                try:
                    self._fast_serializers[entity_type] = FastAvroSerializer(
                        schema_registry_client,
                        schema_str,
                        subject_name,
                        field_set,
                    )
                except Exception:
                    logger.debug(
                        "FastAvroSerializer init failed for %s, using legacy",
                        entity_type,
                    )

            logger.info("Avro serializers initialized for: %s", list(AVRO_SCHEMAS.keys()))
        except ImportError:
            logger.warning(
                "confluent-kafka[avro] not installed. Using JSON serialization. "
                "Install with: pip install 'confluent-kafka[avro]'"
            )

    @staticmethod
    def _convert_avro_value(value: Any) -> Any:
        """Convert a single Python value to Avro-compatible form."""
        if isinstance(value, Decimal):
            scaled = int(value * 100)
            byte_length = max(1, (scaled.bit_length() + 8) // 8)
            return scaled.to_bytes(byte_length, byteorder='big', signed=True)
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, datetime):
            return int(value.timestamp() * 1000)
        if isinstance(value, date):
            return (value - _EPOCH_DATE).days
        return value

    def _to_avro_dict(self, obj: Any, ctx: SerializationContext) -> dict:
        """Convert object to Avro-compatible dict.

        Only includes fields defined in the Avro schema for the entity type.
        Handles Enum, Decimal, datetime and date conversions.
        Uses fields()+getattr() instead of asdict() to avoid deep copy.
        """
        convert = self._convert_avro_value
        schema_fields = self._get_avro_fields_cached(ctx)

        if is_dataclass(obj):
            if schema_fields is not None:
                return {
                    f.name: convert(getattr(obj, f.name))
                    for f in fields(obj)
                    if f.name in schema_fields
                }
            return {f.name: convert(getattr(obj, f.name)) for f in fields(obj)}

        if isinstance(obj, dict):
            if schema_fields is not None:
                return {
                    k: convert(v) for k, v in obj.items() if k in schema_fields
                }
            return {k: convert(v) for k, v in obj.items()}

        raise ValueError(f"Cannot convert {type(obj)} to Avro dict")

    def _to_avro_dict_direct(self, obj: Any, field_set: set[str]) -> dict:
        """Convert object to Avro dict without SerializationContext overhead."""
        convert = self._convert_avro_value
        if is_dataclass(obj):
            return {
                f.name: convert(getattr(obj, f.name))
                for f in fields(obj)
                if f.name in field_set
            }
        if isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items() if k in field_set}
        raise ValueError(f"Cannot convert {type(obj)} to Avro dict")

    def _get_avro_fields_cached(self, ctx: SerializationContext | None) -> set[str] | None:
        """Return cached set of field names for the Avro schema, or None."""
        if not ctx or not ctx.topic:
            return None
        entity_type = self._get_entity_type(ctx.topic)
        if not entity_type:
            return None
        return self._avro_field_cache.get(entity_type)

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Handle delivery reports."""
        if err:
            self.stats.failed += 1
            logger.error("Delivery failed: %s", err)
        else:
            self.stats.delivered += 1
            logger.debug("Delivered to %s[%d]@%d", msg.topic(), msg.partition(), msg.offset())

    def _error_only_callback(self, err: Any, msg: Any) -> None:
        """Handle only error delivery reports (used in bulk mode)."""
        if err:
            self.stats.failed += 1
            logger.error("Delivery failed: %s", err)

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
        """Extract entity type from topic name (cached)."""
        cached = self._entity_type_cache.get(topic)
        if cached is not None:
            return cached
        # banking.transactions -> transactions
        # banking.card-transactions -> card_transactions
        if "." in topic:
            entity = topic.split(".")[-1].replace("-", "_")
        else:
            entity = topic
        self._entity_type_cache[topic] = entity
        return entity

    def _get_ce_timestamp_bytes(self) -> bytes:
        """Return cached CloudEvents timestamp, updating every ~1ms."""
        now = time.time()
        if now - self._ce_timestamp_updated > 0.001:
            dt = datetime.fromtimestamp(now, tz=timezone.utc)
            ts = dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"
            self._ce_timestamp_bytes = ts.encode("utf-8")
            self._ce_timestamp_updated = now
        return self._ce_timestamp_bytes

    def _build_cloudevent_headers(
        self, topic: str, record: Any
    ) -> list[tuple[str, bytes]]:
        """Build CloudEvents headers for binary content mode.

        Parameters
        ----------
        topic : str
            Kafka topic name.
        record : Any
            The record being sent.

        Returns
        -------
        list[tuple[str, bytes]]
            List of header tuples (name, value) for Kafka.
        """
        entity_type = self._get_entity_type(topic)

        # Use pre-computed static headers per topic
        static = self._ce_static_cache.get(entity_type)
        if static is None:
            static = _STATIC_CE_HEADERS.get(entity_type)
            if static is None:
                event_type = EVENT_TYPES.get(
                    entity_type, f"com.financial.{entity_type}.created.v1"
                )
                source = SOURCE_URIS.get(
                    entity_type, f"/financial/banking/{entity_type}"
                )
                static = [
                    ("ce_specversion", _CE_SPECVERSION_BYTES),
                    ("ce_type", event_type.encode("utf-8")),
                    ("ce_source", source.encode("utf-8")),
                    ("content-type", _CE_CONTENT_TYPE_BYTES),
                ]
            self._ce_static_cache[entity_type] = static

        # Clone static headers and append variable parts
        headers = list(static)
        headers.append(("ce_id", _fast_uuid_bytes()))
        headers.append(("ce_time", self._get_ce_timestamp_bytes()))

        subject = self._get_key(topic, record)
        if subject:
            headers.append(("ce_subject", subject.encode("utf-8")))

        return headers

    def _build_cloudevent_headers_fast(
        self, entity_type: str, key_bytes: bytes | None
    ) -> list[tuple[str, bytes]]:
        """Build CloudEvents headers without record inspection (for send_fast)."""
        static = self._ce_static_cache.get(entity_type)
        if static is None:
            static = _STATIC_CE_HEADERS.get(entity_type)
            if static is None:
                event_type = EVENT_TYPES.get(
                    entity_type, f"com.financial.{entity_type}.created.v1"
                )
                source = SOURCE_URIS.get(
                    entity_type, f"/financial/banking/{entity_type}"
                )
                static = [
                    ("ce_specversion", _CE_SPECVERSION_BYTES),
                    ("ce_type", event_type.encode("utf-8")),
                    ("ce_source", source.encode("utf-8")),
                    ("content-type", _CE_CONTENT_TYPE_BYTES),
                ]
            self._ce_static_cache[entity_type] = static

        headers = list(static)
        headers.append(("ce_id", _fast_uuid_bytes()))
        headers.append(("ce_time", self._get_ce_timestamp_bytes()))

        if key_bytes:
            headers.append(("ce_subject", key_bytes))

        return headers

    def send(self, topic: str, record: Any, key: str | None = None) -> None:
        """Send a single record to Kafka topic."""
        entity_type = self._get_entity_type(topic)

        # Use Avro serialization if available
        if entity_type and entity_type in self._avro_serializers:
            serializer = self._avro_serializers[entity_type]
            ctx = SerializationContext(topic, MessageField.VALUE)
            value = serializer(record, ctx)
        else:
            # Fall back to JSON — use fast path for flat event dataclasses
            if is_dataclass(record) and entity_type in AVRO_SCHEMAS:
                data = to_dict_fast(record)
            else:
                data = to_dict(record)
            value = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")

        # Extract key if not provided
        if key is None:
            key = self._get_key(topic, record)

        # Build CloudEvents headers if enabled
        headers = None
        if self.use_cloudevents:
            headers = self._build_cloudevent_headers(topic, record)

        produce_kwargs = {
            "topic": topic,
            "key": key.encode("utf-8") if key else None,
            "value": value,
            "headers": headers,
            "callback": self._delivery_callback,
        }
        try:
            self.producer.produce(**produce_kwargs)
        except BufferError:
            # Internal queue full — drain delivery callbacks and retry once
            self.producer.poll(1.0)
            self.producer.produce(**produce_kwargs)

        self.stats.sent += 1
        self._since_last_poll += 1
        if self._since_last_poll >= self._poll_interval:
            self.producer.poll(0)
            self._since_last_poll = 0

    def send_fast(self, topic: str, record: Any) -> None:
        """Send a single record with minimal overhead (bulk mode).

        Uses FastAvroSerializer, pre-computed headers, and error-only callbacks.
        Falls back to regular send() if fast serializer is not available.
        """
        entity_type = self._get_entity_type(topic)

        # Try fast path: FastAvroSerializer + error-only callback
        fast_ser = self._fast_serializers.get(entity_type) if entity_type else None
        if fast_ser is None:
            return self.send(topic, record)

        # Serialize with FastAvroSerializer
        avro_dict = self._to_avro_dict_direct(record, fast_ser._field_set)
        value = fast_ser.serialize(avro_dict)

        # Extract key — encode once, reuse for both Kafka key and CE header
        key_field = self.KEY_FIELDS.get(topic)
        key = getattr(record, key_field, None) if key_field and is_dataclass(record) else None
        key_bytes = key.encode("utf-8") if key else None

        # Build CloudEvents headers (pass pre-encoded key_bytes)
        headers = None
        if self.use_cloudevents:
            headers = self._build_cloudevent_headers_fast(entity_type, key_bytes)

        try:
            self.producer.produce(
                topic=topic,
                key=key_bytes,
                value=value,
                headers=headers,
                callback=self._error_only_callback,
            )
        except BufferError:
            self.producer.poll(1.0)
            self.producer.produce(
                topic=topic,
                key=key_bytes,
                value=value,
                headers=headers,
                callback=self._error_only_callback,
            )

        self.stats.sent += 1
        self._since_last_poll += 1
        if self._since_last_poll >= self._poll_interval:
            self.producer.poll(0)
            self._since_last_poll = 0

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
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning(
                "Flush timeout: %d messages still in queue after %.0fs",
                remaining, timeout,
            )

    def close(self) -> None:
        """Flush and close the producer.

        Uses a scaled timeout: 30s base + 1s per 10K messages sent,
        capped at 300s. This prevents silent data loss on large loads.
        """
        timeout = min(300.0, 30.0 + self.stats.sent / 10000)
        self.flush(timeout)
        # Compute delivered for error-only callback mode
        delivered = self.stats.delivered
        if delivered == 0 and self.stats.sent > 0 and self.stats.failed < self.stats.sent:
            delivered = self.stats.sent - self.stats.failed
        logger.info(
            "Kafka sink closed: sent=%d, delivered=%d, failed=%d",
            self.stats.sent,
            delivered,
            self.stats.failed,
        )
