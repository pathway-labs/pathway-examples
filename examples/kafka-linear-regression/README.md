# Linear Regression with Kafka and Pathway

This example demonstrates simple linear regression on streaming data from Kafka.

You can read the
[linear regression on data from Kafka tutorial](https://pathway.com/developers/tutorials/linear_regression_with_kafka/)
for more details.

## Before you begin

You'll need Python installed and access to a Kafka instance. If you don't have
access to a Kafka instance you can signup for an [Upstash](https://upstash.com)
account and use a free Kafka instance.

## Setup the runtime environment

Create a virtual environment, activate it, and install the example dependencies:

```sh
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

Create a `.env` file with the following contents. Update the environment
variable values with credentials for your Kafka instance.

```
UPSTASH_KAFKA_USER=
UPSTASH_KAFKA_PASS=
UPSTASH_KAFKA_ENDPOINT=
```

## Run the examples

Run the Pathway script:

```sh
python realtime_regression.py
```

Run the Kafka script:

```sh
python generate_kafka_stream.py
```
