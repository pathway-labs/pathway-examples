# Copyright © 2024 Pathway

import time

import pathway as pw

rdkafka_settings = {
    "bootstrap.servers": "kafka:9092",
    "security.protocol": "plaintext",
    "group.id": "0",
    "session.timeout.ms": "6000",
    "auto.offset.reset": "earliest",
}

# timezone1 = "America/New_York"
# timezone2 = "Europe/Paris"

str_repr = "%Y-%m-%d %H:%M:%S.%f %z"


class inputStreamSchema(pw.Schema):
    date: str
    message: str


timestamps_timezone_1 = pw.io.kafka.read(
    rdkafka_settings,
    topic="timezone1",
    format="json",
    schema=inputStreamSchema,
    autocommit_duration_ms=100,
)

timestamps_timezone_2 = pw.io.kafka.read(
    rdkafka_settings,
    topic="timezone2",
    format="json",
    schema=inputStreamSchema,
    autocommit_duration_ms=100,
)

pw.io.csv.write(timestamps_timezone_1, "./raw_t1.csv")
pw.io.csv.write(timestamps_timezone_2, "./raw_t2.csv")


def convert_to_UTC(table):
    table = table.select(
        date=pw.this.date.dt.strptime(fmt=str_repr, contains_timezone=True),
        message=pw.this.message,
    )
    table_timestamp = table.select(
        timestamp=pw.this.date.dt.timestamp(unit="ms"),
        message=pw.this.message,
    )
    return table_timestamp


timestamps_timezone_1 = convert_to_UTC(timestamps_timezone_1)
timestamps_timezone_2 = convert_to_UTC(timestamps_timezone_2)

timestamps_unified = timestamps_timezone_1.concat_reindex(timestamps_timezone_2)

pw.io.csv.write(timestamps_timezone_1, "./t1.csv")
pw.io.csv.write(timestamps_timezone_2, "./t2.csv")
pw.io.csv.write(timestamps_unified, "./unified_timestamps.csv")
pw.io.kafka.write(
    timestamps_unified, rdkafka_settings, topic_name="unified_timestamps", format="json"
)

# We wait for Kafka to be ready.
time.sleep(20)

# We launch the computation.
pw.run()
