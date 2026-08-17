/**
 * Phase 7A — topic config from env; DLQs excluded from consumption.
 */

import { describe, expect, test } from "bun:test";
import { env } from "@/lib/env";
import {
  getAckRequiredTopics,
  // getConsumerExcludedTopics,
  getConsumerTopicPattern,
  getSubscriberPublishAllowlist,
  isAckRequiredTopic,
  isConsumerExcludedTopic,
} from "@/lib/kafka-ws-bridge/record-log-topics";

describe("record-log topic configuration", () => {
  test("ack-required topics match env write/read/status", () => {
    const ack = getAckRequiredTopics();
    expect(ack.has(env.RECORD_LOG_WRITE_TOPIC)).toBe(true);
    expect(ack.has(env.RECORD_LOG_READ_TOPIC)).toBe(true);
    expect(ack.has(env.RECORD_LOG_STATUS_TOPIC)).toBe(true);
    expect(ack.has(env.RECORD_LOG_WRITE_DLQ_TOPIC)).toBe(false);
    expect(ack.has(env.RECORD_LOG_READ_DLQ_TOPIC)).toBe(false);
  });

  test("DLQ topics are consumer-excluded", () => {
    expect(isConsumerExcludedTopic(env.RECORD_LOG_WRITE_DLQ_TOPIC)).toBe(true);
    expect(isConsumerExcludedTopic(env.RECORD_LOG_READ_DLQ_TOPIC)).toBe(true);
    expect(isConsumerExcludedTopic(env.RECORD_LOG_WRITE_TOPIC)).toBe(false);
  });

  test("consumer topic pattern excludes DLQs and internal topics", () => {
    const pat = getConsumerTopicPattern();
    expect(pat.test(env.RECORD_LOG_WRITE_TOPIC)).toBe(true);
    expect(pat.test(env.RECORD_LOG_READ_TOPIC)).toBe(true);
    expect(pat.test(env.RECORD_LOG_STATUS_TOPIC)).toBe(true);
    expect(pat.test(env.RECORD_LOG_WRITE_DLQ_TOPIC)).toBe(false);
    expect(pat.test(env.RECORD_LOG_READ_DLQ_TOPIC)).toBe(false);
    expect(pat.test("__consumer_offsets")).toBe(false);
  });

  test("subscriber publish allowlist includes DLQs", () => {
    const allow = getSubscriberPublishAllowlist();
    expect(allow.has(env.RECORD_LOG_WRITE_DLQ_TOPIC)).toBe(true);
    expect(allow.has(env.RECORD_LOG_READ_DLQ_TOPIC)).toBe(true);
  });

  test("isAckRequiredTopic uses env-backed set", () => {
    expect(isAckRequiredTopic(env.RECORD_LOG_STATUS_TOPIC)).toBe(true);
    expect(isAckRequiredTopic(env.RECORD_LOG_WRITE_DLQ_TOPIC)).toBe(false);
  });
});
