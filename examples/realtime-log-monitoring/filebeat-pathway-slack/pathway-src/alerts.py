# Copyright © 2024 Pathway

import time

import requests

import pathway as pw

alert_threshold = 5
sliding_window_duration = 1


SLACK_ALERT_CHANNEL_ID = "XXX"
SLACK_ALERT_TOKEN = "XXX"

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
log_table = pw.io.kafka.read(
    rdkafka_settings,
    topic="logs",
    format="json",
    schema=inputSchema,
    autocommit_duration_ms=100,
)
log_table = log_table.select(timestamp=pw.this["@timestamp"], log=pw.this.message)
log_table = log_table.select(
    pw.this.log,
    timestamp=pw.this.timestamp.dt.strptime("%Y-%m-%dT%H:%M:%S.%fZ").dt.timestamp(),
)

t_latest_log = log_table.reduce(last_log=pw.reducers.max(pw.this.timestamp))


t_sliding_window = log_table.filter(
    pw.this.timestamp >= t_latest_log.ix_ref().last_log - sliding_window_duration
)
t_alert = t_sliding_window.reduce(count=pw.reducers.count())
t_alert = t_alert.select(
    alert=pw.this.count >= alert_threshold, latest_update=t_latest_log.ix_ref().last_log
)
t_alert = t_alert.select(pw.this.alert)


def on_alert_event(key, row, time, is_addition):
    alert_message = "Alert '{}' changed state to {}".format(
        row["alert"],
        "ACTIVE" if is_addition else "INACTIVE",
    )
    requests.post(
        "https://slack.com/api/chat.postMessage",
        data="text={}&channel={}".format(alert_message, SLACK_ALERT_CHANNEL_ID),
        headers={
            "Authorization": "Bearer {}".format(SLACK_ALERT_TOKEN),
            "Content-Type": "application/x-www-form-urlencoded",
        },
    ).raise_for_status()


pw.io.subscribe(t_alert, on_alert_event)

time.sleep(10)
# We launch the computation.
pw.run()
