/**
 * Phase-2 → Phase-3 contract test
 *
 * Uses the exact HTTP body shape produced by recordlog-api HttpKafkaPublisher
 * (Phase 2) against the real Elysia POST /http-publish route (Phase 3).
 *
 * Does not import recordlog-api. Does not require a live Kafka broker.
 */

import type { KafkaProducerLike } from "@/lib/producer.kafka";

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

/** UUIDv7 correlation id used by pipeline-state / Phase-2 contract tests. */
const PHASE2_CORRELATION_ID = "01901234-5678-7abc-8def-0123456789ab";

/**
 * Exact request body shape from Phase-2 HttpKafkaPublisher.postToGateway:
 *   JSON.stringify({ topic, jobName, correlationId, event, eventType })
 */
function phase2AdapterBody(input: {
  topic: string;
  jobName: string;
  correlationId: string;
  event: Record<string, unknown>;
  eventType: "ingest" | "status";
}) {
  return {
    topic: input.topic,
    jobName: input.jobName,
    correlationId: input.correlationId,
    event: input.event,
    eventType: input.eventType,
  };
}

type SentMessage = {
  topic: string;
  key: string | null | undefined;
  value: string;
};

function createMockProducer() {
  const sent: SentMessage[] = [];
  const producer = {
    async send(payload: {
      topic: string;
      messages: Array<{ key?: string | null; value: string }>;
    }) {
      for (const m of payload.messages) {
        sent.push({ topic: payload.topic, key: m.key, value: m.value });
      }
      return [];
    },
  };
  return { producer, sent };
}

describe("Phase-2 HttpKafkaPublisher → Gateway /http-publish contract", () => {
  let mockKafka: ReturnType<typeof createMockProducer>;
  let app: { handle: (req: Request) => Promise<Response> };
  let setProducer: (mock: KafkaProducerLike | null) => void;

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
    const httpPublish = (await import("@/routes/v1/kafka/http-publish"))
      .default;

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

  test("ingest: Phase-2 adapter body → 202, same UUIDv7 key, exact event value, no jobName in value", async () => {
    // Exact ingest event Phase-2 buildIngestEvent produces
    const ingestEvent = {
      correlationId: PHASE2_CORRELATION_ID,
      records: ["2026-01-01,temp,21.5", "2026-01-01,hum,40"],
      metadata: {
        source: "recordlog-api",
        receivedAt: "2026-08-08T12:00:00.000Z",
        username: "alice",
        role: "admin",
        clientRequestId: null,
      },
    };

    // Exact body Phase-2 postToGateway sends
    const body = phase2AdapterBody({
      topic: "record-log-ingest",
      jobName: "legacy-sql-job-name", // Phase-2 always sends SQL_JOB_NAME
      correlationId: PHASE2_CORRELATION_ID,
      event: ingestEvent,
      eventType: "ingest",
    });

    const res = await app.handle(
      new Request("http://localhost/http-publish", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-client-id": TEST_ENV.CLIENT_ID!,
          "x-client-secret": TEST_ENV.CLIENT_SECRET!,
          "x-api-key": "valid-api-key",
          "x-api-secret": "valid-api-secret",
        },
        body: JSON.stringify(body),
      }),
    );

    expect(res.status).toBe(202);
    const json = (await res.json()) as {
      success: boolean;
      statusCode: number;
      data: { topic: string; correlationId: string };
    };
    expect(json.success).toBe(true);
    expect(json.statusCode).toBe(202);
    expect(json.data.correlationId).toBe(PHASE2_CORRELATION_ID);
    expect(json.data.topic).toBe("record-log-ingest");

    expect(mockKafka.sent.length).toBe(1);
    expect(mockKafka.sent[0]!.key).toBe(PHASE2_CORRELATION_ID);
    expect(mockKafka.sent[0]!.topic).toBe("record-log-ingest");

    const value = JSON.parse(mockKafka.sent[0]!.value);
    expect(value).toEqual(ingestEvent);
    expect(value).not.toHaveProperty("jobName");
    expect(value).not.toHaveProperty("apiUserId");
    expect(value).not.toHaveProperty("eventType");
  });

  test("status: Phase-2 adapter body → 202, same UUIDv7 key, exact status event value", async () => {
    const statusEvent = {
      correlationId: PHASE2_CORRELATION_ID,
      stage: "RECEIVED",
      status: "SUCCEEDED",
      timestamp: "2026-08-08T12:00:01.000Z",
      sequence: null,
      details: { message: "Request accepted by recordlog-api" },
    };

    const body = phase2AdapterBody({
      topic: "record-log-status",
      jobName: "legacy-sql-job-name",
      correlationId: PHASE2_CORRELATION_ID,
      event: statusEvent,
      eventType: "status",
    });

    const res = await app.handle(
      new Request("http://localhost/http-publish", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-client-id": TEST_ENV.CLIENT_ID!,
          "x-client-secret": TEST_ENV.CLIENT_SECRET!,
          "x-api-key": "valid-api-key",
          "x-api-secret": "valid-api-secret",
        },
        body: JSON.stringify(body),
      }),
    );

    expect(res.status).toBe(202);
    const json = (await res.json()) as {
      data: { correlationId: string; topic: string };
    };
    expect(json.data.correlationId).toBe(PHASE2_CORRELATION_ID);
    expect(json.data.topic).toBe("record-log-status");

    expect(mockKafka.sent.length).toBe(1);
    expect(mockKafka.sent[0]!.key).toBe(PHASE2_CORRELATION_ID);
    expect(JSON.parse(mockKafka.sent[0]!.value)).toEqual(statusEvent);
    expect(JSON.parse(mockKafka.sent[0]!.value)).not.toHaveProperty("jobName");
  });
});
