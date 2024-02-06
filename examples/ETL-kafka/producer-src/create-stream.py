# Copyright © 2024 Pathway

import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer
from pytz import timezone

input_size = 100
random.seed(0)

topic1 = "timezone1"
topic2 = "timezone2"

timezone1 = timezone("America/New_York")
timezone2 = timezone("Europe/Paris")

str_repr = "%Y-%m-%d %H:%M:%S.%f %z"


def generate_stream():
    time.sleep(30)
    producer1 = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        security_protocol="PLAINTEXT",
        api_version=(0, 10, 2),
    )
    producer2 = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        security_protocol="PLAINTEXT",
        api_version=(0, 10, 2),
    )

    for i in range(input_size):
        if random.choice([True, False]):
            timestamp = datetime.now(timezone1)
            message_json = {"date": timestamp.strftime(str_repr), "message": str(i)}
            producer1.send(topic1, (json.dumps(message_json)).encode("utf-8"))
        else:
            timestamp = datetime.now(timezone2)
            message_json = {"date": timestamp.strftime(str_repr), "message": str(i)}
            producer2.send(topic2, (json.dumps(message_json)).encode("utf-8"))
        time.sleep(1)

    time.sleep(2)
    producer1.close()
    producer2.close()


if __name__ == "__main__":
    generate_stream()
