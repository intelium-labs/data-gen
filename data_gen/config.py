"""Configuration management for data-gen."""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class KafkaConfig:
    """Kafka producer configuration."""

    bootstrap_servers: str = "localhost:9092"
    acks: str = "all"
    batch_size: int = 16384
    linger_ms: int = 5
    compression: str = "snappy"
    retries: int = 3

    def to_dict(self) -> dict[str, Any]:
        """Convert to confluent-kafka config dict."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": self.acks,
            "batch.size": self.batch_size,
            "linger.ms": self.linger_ms,
            "compression.type": self.compression,
            "retries": self.retries,
        }


@dataclass
class PostgresConfig:
    """PostgreSQL connection configuration."""

    host: str = "localhost"
    port: int = 5432
    database: str = "datagen"
    user: str = "postgres"
    password: str = "postgres"

    @property
    def connection_string(self) -> str:
        """Get connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class OutputConfig:
    """Output configuration."""

    json_output_dir: Path = field(default_factory=lambda: Path("output"))
    pretty_json: bool = False


@dataclass
class StreamConfig:
    """Streaming configuration."""

    rate_per_second: float = 100.0
    duration_seconds: float = 60.0
    topic_prefix: str = "dev.financial"


@dataclass
class ScenarioConfig:
    """Configuration for scenario execution."""

    name: str
    num_customers: int = 100
    transactions_per_customer: int = 50
    start_date: datetime | None = None
    end_date: datetime | None = None
    enable_fraud_patterns: bool = False
    fraud_rate: float = 0.05
    enable_poison_pills: bool = False
    poison_pill_rate: float = 0.01
    labels: dict[str, Any] = field(default_factory=dict)


@dataclass
class DataGenConfig:
    """Main configuration for data-gen."""

    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    stream: StreamConfig = field(default_factory=StreamConfig)
    scenario: ScenarioConfig | None = None
    seed: int | None = None
    log_level: str = "INFO"
    country_weights: dict[str, float] | None = None

    @classmethod
    def from_env(cls) -> "DataGenConfig":
        """Create config from environment variables."""
        import json
        import os

        kafka = KafkaConfig(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            acks=os.getenv("KAFKA_ACKS", "all"),
        )

        postgres = PostgresConfig(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "datagen"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        )

        output = OutputConfig(
            json_output_dir=Path(os.getenv("OUTPUT_DIR", "output")),
            pretty_json=os.getenv("PRETTY_JSON", "false").lower() == "true",
        )

        stream = StreamConfig(
            rate_per_second=float(os.getenv("STREAM_RATE", "100")),
            duration_seconds=float(os.getenv("STREAM_DURATION", "60")),
            topic_prefix=os.getenv("TOPIC_PREFIX", "dev.financial"),
        )

        country_weights_str = os.getenv("COUNTRY_WEIGHTS")
        country_weights = json.loads(country_weights_str) if country_weights_str else None

        return cls(
            kafka=kafka,
            postgres=postgres,
            output=output,
            stream=stream,
            seed=int(os.getenv("SEED")) if os.getenv("SEED") else None,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            country_weights=country_weights,
        )
