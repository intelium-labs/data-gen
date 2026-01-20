"""Output sinks for exporting generated data."""

from data_gen.sinks.console import ConsoleSink
from data_gen.sinks.json_file import JsonFileSink
from data_gen.sinks.kafka import KafkaSink
from data_gen.sinks.postgres import PostgresSink

__all__ = ["ConsoleSink", "JsonFileSink", "KafkaSink", "PostgresSink"]
