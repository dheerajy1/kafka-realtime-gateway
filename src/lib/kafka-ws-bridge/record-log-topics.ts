/**
 * Record Log Kafka topic names from validated environment configuration.
 * DLQ topics must not be consumed by the Gateway consumer.
 */

import { env } from "@/lib/env";

export function getRecordLogWriteTopic(): string {
  return env.RECORD_LOG_WRITE_TOPIC;
}

export function getRecordLogReadTopic(): string {
  return env.RECORD_LOG_READ_TOPIC;
}

export function getRecordLogStatusTopic(): string {
  return env.RECORD_LOG_STATUS_TOPIC;
}

export function getRecordLogWriteDlqTopic(): string {
  return env.RECORD_LOG_WRITE_DLQ_TOPIC;
}

export function getRecordLogReadDlqTopic(): string {
  return env.RECORD_LOG_READ_DLQ_TOPIC;
}

/** Topics that require subscriber ACK before Kafka offset commit. */
export function getAckRequiredTopics(): Set<string> {
  return new Set([
    env.RECORD_LOG_WRITE_TOPIC,
    env.RECORD_LOG_READ_TOPIC,
    env.RECORD_LOG_STATUS_TOPIC,
  ]);
}

/** Stage DLQ topics — Kafka-retained; Gateway consumer must not subscribe. */
export function getConsumerExcludedTopics(): Set<string> {
  return new Set([
    env.RECORD_LOG_WRITE_DLQ_TOPIC,
    env.RECORD_LOG_READ_DLQ_TOPIC,
  ]);
}

/** Topics subscribers may publish via the authenticated subscribe WS. */
export function getSubscriberPublishAllowlist(): Set<string> {
  return new Set([
    env.RECORD_LOG_WRITE_TOPIC,
    env.RECORD_LOG_WRITE_DLQ_TOPIC,
    env.RECORD_LOG_READ_TOPIC,
    env.RECORD_LOG_READ_DLQ_TOPIC,
    env.RECORD_LOG_STATUS_TOPIC,
  ]);
}

export function isAckRequiredTopic(topic: string): boolean {
  return getAckRequiredTopics().has(topic);
}

export function isConsumerExcludedTopic(topic: string): boolean {
  return getConsumerExcludedTopics().has(topic);
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * KafkaJS subscribe pattern: non-internal topics except configured DLQs.
 * Built from env so topic names are never hardcoded in the consumer path.
 */
export function getConsumerTopicPattern(): RegExp {
  const excluded = [...getConsumerExcludedTopics()].map(
    (t) => `${escapeRegex(t)}$`,
  );
  const inner = ["__", ...excluded].join("|");
  return new RegExp(`^(?!${inner}).+`);
}
