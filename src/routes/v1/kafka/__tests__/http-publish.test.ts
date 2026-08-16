import { describe, expect, test, beforeEach } from "bun:test";
import {
  httpPublishRequestSchema,
  httpPublishResponseSchema,
} from "@/schemas/http-publish.schema";
import { setProducer, type KafkaProducerLike } from "@/lib/producer.kafka";

const validUuidV7 = "01936c1a-7b2e-7b2e-8b2e-7b2e7b2e7b2e";

describe("httpPublishRequestSchema", () => {
  test("accepts valid topic, UUIDv7, and arbitrary value", () => {
    const result = httpPublishRequestSchema.safeParse({
      topic: "any-topic",
      correlationId: validUuidV7,
      value: { foo: "bar", nested: [1, 2, 3] },
    });
    expect(result.success).toBe(true);
  });

  test("accepts null/primitive value", () => {
    expect(
      httpPublishRequestSchema.safeParse({
        topic: "t",
        correlationId: validUuidV7,
        value: null,
      }).success,
    ).toBe(true);
    expect(
      httpPublishRequestSchema.safeParse({
        topic: "t",
        correlationId: validUuidV7,
        value: "string",
      }).success,
    ).toBe(true);
  });

  test("rejects empty topic", () => {
    const result = httpPublishRequestSchema.safeParse({
      topic: "",
      correlationId: validUuidV7,
      value: {},
    });
    expect(result.success).toBe(false);
  });

  test("rejects invalid UUID", () => {
    const result = httpPublishRequestSchema.safeParse({
      topic: "t",
      correlationId: "not-a-uuid",
      value: {},
    });
    expect(result.success).toBe(false);
  });

  test("does not require Record Log-specific fields", () => {
    const result = httpPublishRequestSchema.safeParse({
      topic: "record-log-ingest-write-model",
      correlationId: validUuidV7,
      value: { only: "whatever" },
    });
    expect(result.success).toBe(true);
  });

  test("rejects missing correlationId", () => {
    const result = httpPublishRequestSchema.safeParse({
      topic: "t",
      value: {},
    });
    expect(result.success).toBe(false);
  });
});

describe("httpPublishResponseSchema", () => {
  test("accepts defined success response", () => {
    const result = httpPublishResponseSchema.safeParse({
      success: true,
      statusCode: 202,
      message: "Event accepted",
      data: {
        topic: "any-topic",
        correlationId: validUuidV7,
      },
    });
    expect(result.success).toBe(true);
  });

  test("rejects success:false", () => {
    const result = httpPublishResponseSchema.safeParse({
      success: false,
      statusCode: 202,
      message: "x",
      data: { topic: "t", correlationId: validUuidV7 },
    });
    expect(result.success).toBe(false);
  });
});

describe("Gateway publishing contract (mock producer)", () => {
  let sent: Array<{
    topic: string;
    messages: Array<{ key?: string | null; value: string }>;
  }>;

  beforeEach(() => {
    sent = [];
    const mock: KafkaProducerLike = {
      send: async (payload) => {
        sent.push(payload);
        return {};
      },
    };
    setProducer(mock);
  });

  test("publishes to supplied topic with key=correlationId and stringified value", async () => {
    const producer = await import("@/lib/producer.kafka").then((m) =>
      m.getProducer(),
    );
    const topic = "my-custom-topic";
    const correlationId = validUuidV7;
    const value = {
      correlationId,
      job: { writeModel: "a", readModel: "b" },
      sourceClientId: "record-log-api",
      records: ["2024-01-01,A,1"],
    };

    await producer.send({
      topic,
      messages: [
        {
          key: correlationId,
          value: JSON.stringify(value),
        },
      ],
    });

    expect(sent).toHaveLength(1);
    expect(sent[0].topic).toBe(topic);
    expect(sent[0].messages[0].key).toBe(correlationId);
    expect(JSON.parse(sent[0].messages[0].value)).toEqual(value);
  });

  test("does not publish to record-log-status", async () => {
    const producer = await import("@/lib/producer.kafka").then((m) =>
      m.getProducer(),
    );
    await producer.send({
      topic: "record-log-ingest-write-model",
      messages: [{ key: validUuidV7, value: "{}" }],
    });
    expect(sent.every((s) => s.topic !== "record-log-status")).toBe(true);
    expect(sent).toHaveLength(1);
  });
});
