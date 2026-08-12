/**
 * Phase 3 — real Elysia route tests for POST /http-publish
 * Mocks Kafka producer and API-key verification (no DB / brokers required).
 */

const TEST_ENV: Record<string, string> = {
  JWT_SECRET: "test-jwt-secret-key-32chars!!",
  CLIENT_ID: "test-client-id",
  CLIENT_SECRET: "test-client-secret",
  ONPREM_DB_SERVER: "127.0.0.1",
  ONPREM_DB_PORT: "5432",
  ONPREM_DB_DATABASE: "test-pg",
  ONPREM_DB_USER_PUBLIC: "pg-public",
  ONPREM_DB_PASSWORD_PUBLIC: "pg-public-pass",
  ONPREM_DB_USER_SERVICE: "pg-service",
  ONPREM_DB_PASSWORD_SERVICE: "pg-service-pass",
  KAFKA_BROKERS: "localhost:9092",
  KAFKA_GROUP_ID: "test-group",
  APP_NAME: "kafka-api-gateway-test",
  KAFKA_CLIENT_ID: "test-client",
  PIPELINE_STATE_CONSUMER_PG_DATABASE: "test-pg",
  PIPELINE_STATE_CONSUMER_PG_USER: "pipeline_state_consumer",
  PIPELINE_STATE_CONSUMER_PG_PASSWORD: "test-pass",
};

for (const [k, v] of Object.entries(TEST_ENV)) {
  process.env[k] = v;
}

import { afterEach, beforeEach, describe, expect, test, mock } from "bun:test";

const UUID_V7_A = "01901234-5678-7abc-8def-0123456789ab";

type SentMessage = {
  topic: string;
  key: string | null | undefined;
  value: string;
};

function createMockProducer() {
  const sent: SentMessage[] = [];
  let failNext = false;
  const producer = {
    async send(payload: {
      topic: string;
      messages: Array<{ key?: string | null; value: string }>;
    }) {
      if (failNext) {
        failNext = false;
        throw new Error("mock kafka send failure");
      }
      for (const m of payload.messages) {
        sent.push({
          topic: payload.topic,
          key: m.key,
          value: m.value,
        });
      }
      return [];
    },
  };
  return {
    producer,
    sent,
    failOnce() {
      failNext = true;
    },
  };
}

function authHeaders(extra?: Record<string, string>): Record<string, string> {
  return {
    "content-type": "application/json",
    "x-client-id": TEST_ENV.CLIENT_ID!,
    "x-client-secret": TEST_ENV.CLIENT_SECRET!,
    "x-api-key": "valid-api-key",
    "x-api-secret": "valid-api-secret",
    ...extra,
  };
}

describe("POST /http-publish — event mode (Phase 3)", () => {
  let mockKafka: ReturnType<typeof createMockProducer>;
  let app: { handle: (req: Request) => Promise<Response> };
  let setProducer: (p: unknown) => void;
  let isUuidV7: (v: string) => boolean;

  beforeEach(async () => {
    // Mock API-key verification before loading the route module
    mock.module("@/lib/db-scripts/fn-verify-api-key", () => ({
      fnVerifyApiKey: async ({ xApiKey }: { xApiKey: string }) => {
        if (xApiKey === "valid-api-key") {
          return {
            success: true as const,
            data: {
              apiKeyId: "valid-api-key",
              apiUserId: 42,
              apiKeySecretHash: await Bun.password.hash("valid-api-secret"),
            },
          };
        }
        return { success: false as const, data: null };
      },
    }));

    const producerMod = await import("@/lib/producer.kafka");
    setProducer = producerMod.setProducer;
    mockKafka = createMockProducer();
    setProducer(mockKafka.producer);

    const uuidMod = await import("@/pipeline-state-consumer/lib/events");
    isUuidV7 = uuidMod.isUuidV7;

    const { MyError, errors } = await import("@/lib/errors");
    const { Elysia } = await import("elysia");
    const httpPublish = (await import("@/routes/v1/kafka/http-publish")).default;

    app = new Elysia()
      .error({ MyError })
      .onError(({ error, status, code }) => {
        if (error instanceof MyError) {
          return status(error.httpCode, {
            success: false,
            httpCode: error.httpCode,
            code: error.code,
            message: error.message,
            error: error.error,
          });
        }
        if (code === "VALIDATION") {
          return status(400, {
            success: false,
            httpCode: 400,
            code: "VALIDATION",
            message: "Validation failed",
            error: String(error),
          });
        }
        return status(500, {
          success: false,
          httpCode: 500,
          code: "INTERNAL_SERVER_ERROR",
          message: errors.INTERNAL_SERVER_ERROR.SERVER_ERROR.message,
          error: errors.INTERNAL_SERVER_ERROR.SERVER_ERROR.error,
        });
      })
      .use(httpPublish);
  });

  afterEach(() => {
    setProducer(null);
  });

  async function postPublish(
    body: unknown,
    headers?: Record<string, string>
  ): Promise<Response> {
    return app.handle(
      new Request("http://localhost/http-publish", {
        method: "POST",
        headers: headers ?? authHeaders(),
        body: JSON.stringify(body),
      })
    );
  }

  test("accepts valid UUIDv7 and returns same correlationId with 202", async () => {
    const event = {
      correlationId: UUID_V7_A,
      records: ["2026-01-01,temp,21.5"],
      metadata: {
        source: "recordlog-api",
        receivedAt: "2026-08-08T12:00:00.000Z",
        username: "alice",
        role: "admin",
        clientRequestId: null,
      },
    };

    const res = await postPublish({
      topic: "record-log-ingest",
      correlationId: UUID_V7_A,
      event,
      eventType: "ingest",
    });

    expect(res.status).toBe(202);
    const json = (await res.json()) as {
      success: boolean;
      statusCode: number;
      data: { topic: string; correlationId: string };
    };
    expect(json.success).toBe(true);
    expect(json.statusCode).toBe(202);
    expect(json.data.topic).toBe("record-log-ingest");
    expect(json.data.correlationId).toBe(UUID_V7_A);
    expect(isUuidV7(json.data.correlationId)).toBe(true);
  });

  test("uses correlationId as Kafka key and exact event as value", async () => {
    const event = {
      correlationId: UUID_V7_A,
      stage: "RECEIVED",
      status: "SUCCEEDED",
      timestamp: "2026-08-08T12:00:00.000Z",
      sequence: null,
    };

    const res = await postPublish({
      topic: "record-log-status",
      correlationId: UUID_V7_A,
      event,
      eventType: "status",
      jobName: "ignored-for-event-mode",
    });

    expect(res.status).toBe(202);
    expect(mockKafka.sent.length).toBe(1);
    expect(mockKafka.sent[0]!.topic).toBe("record-log-status");
    expect(mockKafka.sent[0]!.key).toBe(UUID_V7_A);
    expect(JSON.parse(mockKafka.sent[0]!.value)).toEqual(event);
    const parsed = JSON.parse(mockKafka.sent[0]!.value);
    expect(parsed).not.toHaveProperty("apiUserId");
    expect(parsed).not.toHaveProperty("jobName");
  });

  test("rejects mismatched event.correlationId vs top-level correlationId", async () => {
    const res = await postPublish({
      topic: "record-log-ingest",
      correlationId: UUID_V7_A,
      event: {
        correlationId: "01ARZ3NDEKTSV4RRFFQ69G5FAW",
        records: [],
      },
      eventType: "ingest",
    });

    expect(res.status).toBe(400);
    expect(mockKafka.sent.length).toBe(0);
  });

  test("rejects invalid UUIDv7", async () => {
    const res = await postPublish({
      topic: "record-log-ingest",
      correlationId: "not-a-uuidv7",
      event: { correlationId: "not-a-uuidv7", records: [] },
      eventType: "ingest",
    });

    expect(res.status).toBe(400);
    expect(mockKafka.sent.length).toBe(0);
  });

  test("rejects missing event in event mode shape", async () => {
    const res = await postPublish({
      topic: "record-log-ingest",
      correlationId: UUID_V7_A,
      eventType: "ingest",
    });

    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(mockKafka.sent.length).toBe(0);
  });

  test("rejects missing eventType when event fields present", async () => {
    const res = await postPublish({
      topic: "record-log-ingest",
      correlationId: UUID_V7_A,
      event: { correlationId: UUID_V7_A, records: [] },
    });

    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(mockKafka.sent.length).toBe(0);
  });

  test("Kafka publish failure does not return 202", async () => {
    mockKafka.failOnce();
    const res = await postPublish({
      topic: "record-log-ingest",
      correlationId: UUID_V7_A,
      event: { correlationId: UUID_V7_A, records: ["a,b,c"] },
      eventType: "ingest",
    });

    expect(res.status).not.toBe(202);
    expect(res.status).toBeGreaterThanOrEqual(400);
  });

  test("does not generate a replacement correlationId for event mode", async () => {
    const res = await postPublish({
      topic: "record-log-ingest",
      correlationId: UUID_V7_A,
      event: { correlationId: UUID_V7_A, records: ["1,2,3"] },
      eventType: "ingest",
    });
    const json = (await res.json()) as { data: { correlationId: string } };
    expect(json.data.correlationId).toBe(UUID_V7_A);
    expect(mockKafka.sent[0]!.key).toBe(UUID_V7_A);
  });
});

describe("POST /http-publish — legacy mode", () => {
  let mockKafka: ReturnType<typeof createMockProducer>;
  let app: { handle: (req: Request) => Promise<Response> };
  let setProducer: (p: unknown) => void;

  beforeEach(async () => {
    mock.module("@/lib/db-scripts/fn-verify-api-key", () => ({
      fnVerifyApiKey: async ({ xApiKey }: { xApiKey: string }) => {
        if (xApiKey === "valid-api-key") {
          return {
            success: true as const,
            data: {
              apiKeyId: "valid-api-key",
              apiUserId: 42,
              apiKeySecretHash: await Bun.password.hash("valid-api-secret"),
            },
          };
        }
        return { success: false as const, data: null };
      },
    }));

    const producerMod = await import("@/lib/producer.kafka");
    setProducer = producerMod.setProducer;
    mockKafka = createMockProducer();
    setProducer(mockKafka.producer);

    const { MyError, errors } = await import("@/lib/errors");
    const { Elysia } = await import("elysia");
    const httpPublish = (await import("@/routes/v1/kafka/http-publish")).default;

    app = new Elysia()
      .error({ MyError })
      .onError(({ error, status, code }) => {
        if (error instanceof MyError) {
          return status(error.httpCode, {
            success: false,
            httpCode: error.httpCode,
            code: error.code,
            message: error.message,
            error: error.error,
          });
        }
        if (code === "VALIDATION") {
          return status(400, {
            success: false,
            httpCode: 400,
            code: "VALIDATION",
            message: "Validation failed",
            error: String(error),
          });
        }
        return status(500, {
          success: false,
          httpCode: 500,
          code: "INTERNAL_SERVER_ERROR",
          message: errors.INTERNAL_SERVER_ERROR.SERVER_ERROR.message,
          error: errors.INTERNAL_SERVER_ERROR.SERVER_ERROR.error,
        });
      })
      .use(httpPublish);
  });

  afterEach(() => {
    setProducer(null);
  });

  test("legacy {topic, jobName} still returns 202 and publishes", async () => {
    const res = await app.handle(
      new Request("http://localhost/http-publish", {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({
          topic: "legacy-topic",
          jobName: "SomeSqlJob",
        }),
      })
    );

    expect(res.status).toBe(202);
    const json = (await res.json()) as {
      data: { topic: string; correlationId: string };
    };
    expect(json.data.topic).toBe("legacy-topic");
    expect(json.data.correlationId).toBeTruthy();
    expect(json.data.correlationId).not.toBe(UUID_V7_A);
    expect(mockKafka.sent.length).toBe(1);
    expect(mockKafka.sent[0]!.key).toBe("valid-api-key");
    const value = JSON.parse(mockKafka.sent[0]!.value);
    expect(value.jobName).toBe("SomeSqlJob");
    expect(value.correlationId).toBe(json.data.correlationId);
  });
});

describe("POST /http-publish — authentication", () => {
  let mockKafka: ReturnType<typeof createMockProducer>;
  let app: { handle: (req: Request) => Promise<Response> };
  let setProducer: (p: unknown) => void;

  beforeEach(async () => {
    mock.module("@/lib/db-scripts/fn-verify-api-key", () => ({
      fnVerifyApiKey: async ({ xApiKey }: { xApiKey: string }) => {
        if (xApiKey === "valid-api-key") {
          return {
            success: true as const,
            data: {
              apiKeyId: "valid-api-key",
              apiUserId: 42,
              apiKeySecretHash: await Bun.password.hash("valid-api-secret"),
            },
          };
        }
        return { success: false as const, data: null };
      },
    }));

    const producerMod = await import("@/lib/producer.kafka");
    setProducer = producerMod.setProducer;
    mockKafka = createMockProducer();
    setProducer(mockKafka.producer);

    const { MyError, errors } = await import("@/lib/errors");
    const { Elysia } = await import("elysia");
    const httpPublish = (await import("@/routes/v1/kafka/http-publish")).default;

    app = new Elysia()
      .error({ MyError })
      .onError(({ error, status, code }) => {
        if (error instanceof MyError) {
          return status(error.httpCode, {
            success: false,
            httpCode: error.httpCode,
            code: error.code,
            message: error.message,
            error: error.error,
          });
        }
        return status(500, {
          success: false,
          httpCode: 500,
          code: "INTERNAL_SERVER_ERROR",
          message: errors.INTERNAL_SERVER_ERROR.SERVER_ERROR.message,
          error: errors.INTERNAL_SERVER_ERROR.SERVER_ERROR.error,
        });
      })
      .use(httpPublish);
  });

  afterEach(() => {
    setProducer(null);
  });

  test("rejects request without API key headers", async () => {
    const res = await app.handle(
      new Request("http://localhost/http-publish", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-client-id": TEST_ENV.CLIENT_ID!,
          "x-client-secret": TEST_ENV.CLIENT_SECRET!,
        },
        body: JSON.stringify({
          topic: "record-log-ingest",
          correlationId: UUID_V7_A,
          event: { correlationId: UUID_V7_A, records: [] },
          eventType: "ingest",
        }),
      })
    );

    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(mockKafka.sent.length).toBe(0);
  });
});
