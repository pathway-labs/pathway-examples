# Copyright © 2023 Pathway

import time

import pathway as pw

alert_threshold = 5
sliding_window_duration = 1

rdkafka_settings = {
    "bootstrap.servers": "kafka:9092",
    "security.protocol": "plaintext",
    "group.id": "0",
    "session.timeout.ms": "6000",
}

inputSchema = pw.schema_builder(
    columns={
        "@timestamp": pw.column_definition(dtype=str),
        "message": pw.column_definition(dtype=str),
    }
)

# We use the Kafka connector to listen to the "logs" topic
# We only need the timestamp and the message
t_logs = pw.io.kafka.read(
    rdkafka_settings,
    topic="logs",
    format="json",
    schema=inputSchema,
    autocommit_duration_ms=100,
)
t_logs = t_logs.select(timestamp=pw.this["@timestamp"], log=pw.this.message)
t_logs = t_logs.select(
    pw.this.log,
    timestamp=pw.this.timestamp.dt.strptime("%Y-%m-%dT%H:%M:%S.%fZ").dt.timestamp(),
)

t_latest_log = t_logs.reduce(last_log=pw.reducers.max(pw.this.timestamp))


t_sliding_window = t_logs.filter(
    pw.this.timestamp >= t_latest_log.ix_ref().last_log - sliding_window_duration
)
t_alert = t_sliding_window.reduce(count=pw.reducers.count())
t_alert = t_alert.select(
    alert=pw.this.count >= alert_threshold, latest_update=t_latest_log.ix_ref().last_log
)
t_alert = t_alert.select(pw.this.alert)

time.sleep(10)

pw.io.elasticsearch.write(
    t_alert,
    "http://elasticsearch:9200",
    auth=pw.io.elasticsearch.ElasticSearchAuth.basic("elastic", "password"),
    index_name="alerts",
)

# We launch the computation.
pw.run()
