/**
 * Generic topic policy builder and helpers (DB-backed, domain-neutral).
 */

import { describe, expect, test } from "bun:test";
import {
  buildTopicPolicy,
  isAckRequiredTopic,
  isConsumerExcludedTopic,
  isSubscriberPublishAllowed,
  setTopicPolicy,
  type TopicPolicyRow,
} from "@/lib/kafka-ws-bridge/topic-policy";

const sampleRows: TopicPolicyRow[] = [
  {
    topic_name: "record-log-ingest-write-model",
    gateway_consume: true,
    ack_required: true,
    subscriber_publish_allowed: true,
    enabled: true,
    description: "rl write",
  },
  {
    topic_name: "record-log-ingest-read-model",
    gateway_consume: true,
    ack_required: true,
    subscriber_publish_allowed: true,
    enabled: true,
    description: "rl read",
  },
  {
    topic_name: "record-log-status",
    gateway_consume: true,
    ack_required: true,
    subscriber_publish_allowed: true,
    enabled: true,
    description: "rl status",
  },
  {
    topic_name: "record-log-ingest-write-model-dlq",
    gateway_consume: false,
    ack_required: false,
    subscriber_publish_allowed: true,
    enabled: true,
    description: "rl write dlq",
  },
  {
    topic_name: "record-log-ingest-read-model-dlq",
    gateway_consume: false,
    ack_required: false,
    subscriber_publish_allowed: true,
    enabled: true,
    description: "rl read dlq",
  },
  {
    topic_name: "jobs-ingest-write-model",
    gateway_consume: true,
    ack_required: true,
    subscriber_publish_allowed: true,
    enabled: true,
    description: "jobs write",
  },
  {
    topic_name: "jobs-ingest-read-model",
    gateway_consume: true,
    ack_required: true,
    subscriber_publish_allowed: true,
    enabled: true,
    description: "jobs read",
  },
  {
    topic_name: "jobs-status",
    gateway_consume: true,
    ack_required: true,
    subscriber_publish_allowed: true,
    enabled: true,
    description: "jobs status",
  },
  {
    topic_name: "jobs-ingest-write-model-dlq",
    gateway_consume: false,
    ack_required: false,
    subscriber_publish_allowed: true,
    enabled: true,
    description: "jobs write dlq",
  },
  {
    topic_name: "jobs-ingest-read-model-dlq",
    gateway_consume: false,
    ack_required: false,
    subscriber_publish_allowed: true,
    enabled: true,
    description: "jobs read dlq",
  },
  {
    topic_name: "disabled-topic",
    gateway_consume: true,
    ack_required: true,
    subscriber_publish_allowed: true,
    enabled: false,
    description: "should be ignored",
  },
];

describe("topic policy", () => {
  test("Record Log topics are represented correctly", () => {
    const policy = buildTopicPolicy(sampleRows);
    expect(policy.enabledTopics.has("record-log-ingest-write-model")).toBe(true);
    expect(policy.enabledTopics.has("record-log-status")).toBe(true);
    expect(policy.consumeTopics.has("record-log-ingest-write-model")).toBe(true);
    expect(policy.consumeTopics.has("record-log-ingest-write-model-dlq")).toBe(false);
  });

  test("Jobs topics are represented correctly", () => {
    const policy = buildTopicPolicy(sampleRows);
    expect(policy.enabledTopics.has("jobs-ingest-write-model")).toBe(true);
    expect(policy.enabledTopics.has("jobs-status")).toBe(true);
    expect(policy.consumeTopics.has("jobs-ingest-write-model")).toBe(true);
    expect(policy.consumeTopics.has("jobs-ingest-write-model-dlq")).toBe(false);
    expect(policy.enabledTopics.has("jobs-status-dlq")).toBe(false);
  });

  test("ACK policy is loaded correctly", () => {
    const policy = buildTopicPolicy(sampleRows);
    setTopicPolicy(policy);
    expect(isAckRequiredTopic("record-log-status")).toBe(true);
    expect(isAckRequiredTopic("jobs-status")).toBe(true);
    expect(isAckRequiredTopic("record-log-ingest-write-model-dlq")).toBe(false);
    expect(isAckRequiredTopic("jobs-ingest-read-model-dlq")).toBe(false);
  });

  test("gateway-consume policy is loaded correctly", () => {
    const policy = buildTopicPolicy(sampleRows);
    setTopicPolicy(policy);
    expect(isConsumerExcludedTopic("record-log-ingest-write-model-dlq")).toBe(true);
    expect(isConsumerExcludedTopic("jobs-ingest-write-model")).toBe(false);
  });

  test("subscriber-publish policy is loaded correctly", () => {
    const policy = buildTopicPolicy(sampleRows);
    setTopicPolicy(policy);
    expect(isSubscriberPublishAllowed("jobs-ingest-write-model-dlq")).toBe(true);
    expect(isSubscriberPublishAllowed("record-log-ingest-read-model")).toBe(true);
  });

  test("disabled topics are not treated as active configuration", () => {
    const policy = buildTopicPolicy(sampleRows);
    expect(policy.enabledTopics.has("disabled-topic")).toBe(false);
    expect(policy.consumeTopics.has("disabled-topic")).toBe(false);
    expect(policy.ackRequiredTopics.has("disabled-topic")).toBe(false);
  });
});
