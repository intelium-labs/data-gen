"""JSON file sink for exporting data to files."""

import json
from pathlib import Path
from typing import Any, Iterator

from data_gen.sinks.serialization import to_dict


class JsonFileSink:
    """Output data to JSON files."""

    def __init__(self, output_dir: str | Path, pretty: bool = False) -> None:
        """Initialize JSON file sink.

        Parameters
        ----------
        output_dir : str | Path
            Directory to write JSON files.
        pretty : bool
            Pretty-print JSON output.
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.pretty = pretty
        self._counts: dict[str, int] = {}

    def write_batch(self, entity_type: str, records: list[Any]) -> None:
        """Write a batch of records to a JSON file."""
        file_path = self.output_dir / f"{entity_type}.json"

        data = [to_dict(record) for record in records]

        with open(file_path, "w", encoding="utf-8") as f:
            if self.pretty:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            else:
                json.dump(data, f, ensure_ascii=False, default=str)

        self._counts[entity_type] = len(records)

    def write_stream(
        self,
        topic: str,
        generator: Iterator[Any],
        rate_per_second: float,
        duration_seconds: float,
    ) -> None:
        """Stream records to a JSON Lines file."""
        import time

        # Use topic name as filename (replace dots with underscores)
        filename = topic.replace(".", "_") + ".jsonl"
        file_path = self.output_dir / filename

        interval = 1.0 / rate_per_second if rate_per_second > 0 else 0
        start_time = time.time()
        count = 0

        with open(file_path, "w", encoding="utf-8") as f:
            for record in generator:
                if time.time() - start_time >= duration_seconds:
                    break

                data = to_dict(record)
                f.write(json.dumps(data, ensure_ascii=False, default=str) + "\n")
                count += 1

                if interval > 0:
                    time.sleep(interval)

        self._counts[topic] = count

    def close(self) -> None:
        """Print summary."""
        print(f"JSON files written to: {self.output_dir}")
        for entity_type, count in self._counts.items():
            print(f"  {entity_type}: {count} records")

