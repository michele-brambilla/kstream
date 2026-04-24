# Kafka Code Map (Detailed)

Generated: 2026-04-23 — detailed map produced by automated scan

## Repo snapshot (top-level relevant tree)
- kafka/
  - adaptors/
    - franz/
      - admin.go
      - consumer.go
      - producer.go
      - record.go

(Full project tree captured during scan — see repository root for complete layout)

## Kafka-related files (paths + short role)
- kafka/adaptors/franz/admin.go — admin wrapper and helpers (kAdmin, adminOptions)
- kafka/adaptors/franz/consumer.go — group/partition consumer implementations and builders (GroupConsumerConfig, groupConsumer, partitionConsumer, groupSession, partitionClaim, franzPartition)
- kafka/adaptors/franz/producer.go — producer provider and franzProducer implementation (ProducerConfig, producerProvider, franzProducer)
- kafka/adaptors/franz/record.go — internal record shapes used by franz adaptor (record, producerRecord)

## go.mod summary (scanned)
- Module: github.com/twmb/franz-go
- Notable requires: github.com/gmbyapa/kstream/v2 v2.2.0 (this repository depends on kstream), franz-go internal packages (kadm, kmsg)
- No references to sarama or librd in this repo's top-level go.mod; kafka adaptors here are franz only.

## Code flow (text/ASCII)
Producers (client code) -> kafka producer API (kstream/kafka wrapper) -> kafka/adaptors/franz/franzProducer -> franz-go client -> Kafka brokers

Kafka brokers -> franz-go client -> kafka/adaptors/franz/partitionConsumer / franzPartition -> groupConsumer / groupSession -> streams/task manager -> processors -> state stores

## Main structs & interfaces (name + file + role)
- ProducerConfig — kafka/adaptors/franz/producer.go — configuration for franz producer provider
- producerProvider — kafka/adaptors/franz/producer.go — provider that constructs franzProducer instances
- franzProducer — kafka/adaptors/franz/producer.go — adaptor implementation for producing (transactional support where applicable)
- record — kafka/adaptors/franz/record.go — internal message shape
- producerRecord — kafka/adaptors/franz/record.go — adaptor-level record used when sending
- adminOptions, kAdmin — kafka/adaptors/franz/admin.go — admin client wrapper types
- GroupConsumerConfig — kafka/adaptors/franz/consumer.go — config for group consumer provider
- groupConsumerProvider — kafka/adaptors/franz/consumer.go — factory/provider for group consumer
- groupConsumer — kafka/adaptors/franz/consumer.go — group consumer implementation (Subscribe/Unsubscribe/Errors)
- partitionClaim — kafka/adaptors/franz/consumer.go — claim abstraction exposing TopicPartition and Records()
- assignment — kafka/adaptors/franz/consumer.go — assignment metadata
- groupSession — kafka/adaptors/franz/consumer.go — session object for a rebalance cycle (Assignment, MarkOffset, CommitOffset)
- ConsumerConfig, consumerProvider, partitionConsumer, franzPartition — kafka/adaptors/franz/consumer.go — partition-level consumer types and helpers

## Main functions (high level signatures / responsibilities)
- NewGroupConsumerConfig(), NewGroupConsumerProvider(), newGroupConsumer(), Subscribe(), Unsubscribe(), Errors(), consumeLoop() — (consumer.go) group consumer lifecycle and event loop
- NewConsumerConfig(), NewConsumerProvider(), newPartitionConsumer(), Partitions(), ConsumeTopic() — (consumer.go) partition discovery and consumption
- NewConsumer/Producer providers and builder entrypoints are present in adaptors/franz to integrate with higher-level kafka interfaces

---
Notes & next steps
- Scan found only franz adaptor in kafka/adaptors — no sarama or librd adaptors in this checkout (matches go.mod).
- If you want a deeper per-file function list (line numbers, signatures) or a diagram including stream components (streams/ directory), I can append that to this document.

