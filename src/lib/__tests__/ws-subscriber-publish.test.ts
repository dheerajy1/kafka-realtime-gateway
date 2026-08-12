import { describe, expect, test, beforeEach } from "bun:test";
import {
  handleSubscriberPublish,
  isSubscriberPublishAllowed,
  WsPublishCommandSchema,
  SUBSCRIBER_PUBLISH_ALLOWLIST,
} from "@/lib/ws-subscriber-publish";
import { setProducer } from "@/lib/producer.kafka";

const REQ = "01901234-5678-7abc-8def-0123456789aa";

beforeEach(() => {
  setProducer(null);
});

describe("subscriber publish allowlist", () => {
  test("allows committed and status only", () => {
    expect(isSubscriberPublishAllowed("record-log-committed")).toBe(true);
    expect(isSubscriberPublishAllowed("record-log-status")).toBe(true);
    expect(isSubscriberPublishAllowed("record-log-ingest")).toBe(false);
    expect(isSubscriberPublishAllowed("random-topic")).toBe(false);
    expect(SUBSCRIBER_PUBLISH_ALLOWLIST.size).toBe(2);
  });
});

describe("WsPublishCommandSchema", () => {
  test("accepts valid publish command", () => {
    const r = WsPublishCommandSchema.safeParse({
      type: "publish",
      requestId: REQ,
      topic: "record-log-status",
      key: "01901234-5678-7abc-8def-0123456789ab",
      value: {
        correlationId: "01901234-5678-7abc-8def-0123456789ab",
        stage: "AZURE_COMMITTED",
        status: "SUCCEEDED",
      },
    });
    expect(r.success).toBe(true);
  });

  test("rejects missing requestId", () => {
    const r = WsPublishCommandSchema.safeParse({
      type: "publish",
      topic: "record-log-status",
      value: {},
    });
    expect(r.success).toBe(false);
  });
});

describe("handleSubscriberPublish", () => {
  test("success returns published metadata with same requestId", async () => {
    setProducer({
      send: async () => [{ topicName: "record-log-status", partition: 2, offset: "99" }],
    });

    const result = await handleSubscriberPublish(
      {
        type: "publish",
        requestId: REQ,
        topic: "record-log-status",
        key: "k1",
        value: { correlationId: "01901234-5678-7abc-8def-0123456789ab", stage: "X" },
      },
      { subscriberId: 1, clientId: "azure-sql-writer-consumer" },
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.requestId).toBe(REQ);
      expect(result.topic).toBe("record-log-status");
      expect(result.partition).toBe(2);
      expect(result.offset).toBe("99");
    }
  });

  test("unauthorized topic is rejected", async () => {
    let sent = false;
    setProducer({
      send: async () => {
        sent = true;
        return [];
      },
    });

    const result = await handleSubscriberPublish(
      {
        type: "publish",
        requestId: REQ,
        topic: "record-log-ingest",
        value: { foo: 1 },
      },
      { subscriberId: 1 },
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toBe("topic-not-allowed");
      expect(result.requestId).toBe(REQ);
    }
    expect(sent).toBe(false);
  });

  test("producer failure returns error, not published", async () => {
    setProducer({
      send: async () => {
        throw new Error("broker unavailable");
      },
    });

    const result = await handleSubscriberPublish(
      {
        type: "publish",
        requestId: REQ,
        topic: "record-log-committed",
        key: "k",
        value: { correlationId: "01901234-5678-7abc-8def-0123456789ab" },
      },
      { subscriberId: 3, clientId: "azure-sql-writer-consumer" },
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toBe("Kafka publish failed");
      expect(result.reason).toContain("broker unavailable");
      expect(result.requestId).toBe(REQ);
    }
  });
});
