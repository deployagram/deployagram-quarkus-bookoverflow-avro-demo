# Deployagram with Quarkus, Kafka Avro, and Confluent Schema Registry - Demo App

Using Maven.

## Purpose

This repo demonstrates how to instrument a [Quarkus](https://quarkus.io/) application
with [Deployagram](https://deployagram.com), JUnit 5, Kafka Avro messages, and
Confluent Schema Registry.

The application itself is simple: it listens to HTTP requests for a Book Recommendation, asks another app over HTTP for
a book, then writes the suggestion to a Kafka topic as an Avro message and responds to the original HTTP Request.

The HTTP request and response contract intentionally matches the JSON-based Quarkus demo. The Kafka payload differs:
`DontForgetMessage` is generated from `src/main/avro/dont_forget_message.avsc` and is serialized through Confluent's
Avro serializer.

## Running tests

The JUnit 5 test starts Kafka and Confluent Schema Registry with Testcontainers, then verifies the emitted Avro Kafka
message through Schema Registry while preserving the Deployagram HTTP and Kafka instrumentation.

To run the tests from the command line, you need:

* Docker running locally, because the tests use Testcontainers for Kafka, Confluent Schema Registry, and Deployagram.
* A Deployagram collector and proxy already running outside the test process.
* A local `.deployagram` license file in this project directory.
* Deployagram environment variables exported in the shell. The required variable names are listed in `.env.example`.

If you keep those variables in a local `.env` file, run the tests with:

```shell script
set -a; source .env; set +a; ./mvnw test
```

If the variables are already exported in your shell, run:

```shell script
./mvnw test
```

When running the test from an IDE, configure the same working directory and
environment variables. The test does not start the Deployagram collector; the
Kafka producer interceptor expects the collector at the configured test
`logger.hostname` and `logger.port`.

## History

If you inspect the commits, you will find:

* Creation of a vanilla Quarkus application
* Service test and application functionality
* Deployagram instrumentation for HTTP (both server and client)
* Deployagram instrumentation for Kafka Producer
* Kafka payload serialization using Avro and Confluent Schema Registry

## [License](./LICENSE)
