# Kafka Message Audit

A toolset for auditing Kafka messages, featuring producers, consumers, and interceptors for detailed message tracking and validation.

## Features

- `producer` & `producerInterceptor`: Send messages with auditing metadata.
- `consumer` & `consumerInterceptor`: Receive and audit messages.
- `audit-common`: Shared utilities and models.
- `integrationTests`: Validate end-to-end message flow.

## Getting Started

### Prerequisites

- Java 17 (or your specific version)
- Maven 3.8.0+
- Apache Kafka cluster (tested on Embedded Kafka provided by spring-kafka-test)


