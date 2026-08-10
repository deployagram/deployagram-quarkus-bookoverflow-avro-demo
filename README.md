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
message through Schema Registry.

To run the tests from the command line, you need:

* Docker running locally, because the tests use Testcontainers for Kafka, Confluent Schema Registry.

## History

If you inspect the commits, you will find:

* Creation of a vanilla Quarkus application
* Service test and application functionality
* Kafka payload serialization using Avro and Confluent Schema Registry

## [License](./LICENSE)
