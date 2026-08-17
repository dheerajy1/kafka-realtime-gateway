/**
 * Record Log Kafka topic groupings and consumer filters.
 * Backed by validated environment configuration.
 */

import { env } from "@/lib/env";

/** Topics that require subscriber ACK before Kafka offset commit. */
export const ackRequiredTopics = new Set<string>([
  env.RECORD_LOG_WRITE_TOPIC,
  env.RECORD_LOG_READ_TOPIC,
  env.RECORD_LOG_STATUS_TOPIC,
]);

/** Stage DLQ topics — Kafka-retained; Gateway consumer must not subscribe. */
export const consumerExcludedTopics = new Set<string>([
  env.RECORD_LOG_WRITE_DLQ_TOPIC,
  env.RECORD_LOG_READ_DLQ_TOPIC,
]);

/** Topics subscribers may publish via the authenticated subscribe WS. */
export const subscriberPublishAllowlist = new Set<string>([
  env.RECORD_LOG_WRITE_TOPIC,
  env.RECORD_LOG_WRITE_DLQ_TOPIC,
  env.RECORD_LOG_READ_TOPIC,
  env.RECORD_LOG_READ_DLQ_TOPIC,
  env.RECORD_LOG_STATUS_TOPIC,
]);

export function isAckRequiredTopic(topic: string): boolean {
  return ackRequiredTopics.has(topic);
}

export function isConsumerExcludedTopic(topic: string): boolean {
  return consumerExcludedTopics.has(topic);
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Precompiled KafkaJS consumer topic regex (excludes internal and DLQ topics). */
export const consumerTopicPattern: RegExp = (() => {
  const excluded = [...consumerExcludedTopics].map((t) => `${escapeRegex(t)}$`);
  const inner = ["__", ...excluded].join("|");
  return new RegExp(`^(?!${inner}).+`);
})();