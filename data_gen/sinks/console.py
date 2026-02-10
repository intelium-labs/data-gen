"""Console sink for debugging and development."""

import json
from typing import Any, Iterator

from data_gen.sinks.serialization import to_dict


class ConsoleSink:
    """Output data to console (stdout) for debugging."""

    def __init__(self, pretty: bool = True, max_records: int | None = None) -> None:
        """Initialize console sink.

        Parameters
        ----------
        pretty : bool
            Pretty-print JSON output.
        max_records : int | None
            Maximum records to print per batch (None for all).
        """
        self.pretty = pretty
        self.max_records = max_records
        self._counts: dict[str, int] = {}

    def write_batch(self, entity_type: str, records: list[Any]) -> None:
        """Write a batch of records to console."""
        print(f"\n{'='*60}")
        print(f"Entity: {entity_type} ({len(records)} records)")
        print("=" * 60)

        display_records = records[: self.max_records] if self.max_records else records

        for record in display_records:
            data = to_dict(record)
            if self.pretty:
                print(json.dumps(data, indent=2, ensure_ascii=False, default=str))
            else:
                print(json.dumps(data, ensure_ascii=False, default=str))

        if self.max_records and len(records) > self.max_records:
            print(f"... and {len(records) - self.max_records} more records")

        self._counts[entity_type] = self._counts.get(entity_type, 0) + len(records)

    def write_stream(
        self,
        topic: str,
        generator: Iterator[Any],
        rate_per_second: float,
        duration_seconds: float,
    ) -> None:
        """Stream records to console."""
        import time

        print(f"\n{'='*60}")
        print(f"Streaming to: {topic}")
        print(f"Rate: {rate_per_second}/sec, Duration: {duration_seconds}s")
        print("=" * 60)

        interval = 1.0 / rate_per_second if rate_per_second > 0 else 0
        start_time = time.time()
        count = 0

        for record in generator:
            if time.time() - start_time >= duration_seconds:
                break

            data = to_dict(record)
            if self.pretty:
                print(json.dumps(data, indent=2, ensure_ascii=False, default=str))
            else:
                print(json.dumps(data, ensure_ascii=False, default=str))

            count += 1

            if interval > 0:
                time.sleep(interval)

        print(f"\nStreamed {count} records in {time.time() - start_time:.2f}s")

    def close(self) -> None:
        """Print summary and close."""
        print(f"\n{'='*60}")
        print("Console Sink Summary")
        print("=" * 60)
        for entity_type, count in self._counts.items():
            print(f"  {entity_type}: {count} records")

