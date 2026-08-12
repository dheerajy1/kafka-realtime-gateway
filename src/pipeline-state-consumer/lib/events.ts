/** Status event for pipeline state persistence. Correlation ID is UUIDv7. */

export const STAGES = [
  "RECEIVED",
  "KAFKA_INGESTED",
  "AZURE_WRITE_STARTED",
  "AZURE_COMMITTED",
  "READ_MODEL_SYNC_STARTED",
  "POSTGRES_COMMITTED",
  "COMPLETED",
  "FAILED",
] as const;

export type Stage = (typeof STAGES)[number];

export const STATUSES = ["STARTED", "SUCCEEDED", "FAILED"] as const;
export type StatusValue = (typeof STATUSES)[number];

export type StatusEvent = {
  correlationId: string;
  stage: Stage;
  status: StatusValue;
  timestamp: string;
  sequence: number | null;
  details?: Record<string, unknown>;
};

const UUID_V7_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const STAGE_SET = new Set<string>(STAGES);
const STATUS_SET = new Set<string>(STATUSES);

export function isUuidV7(value: string): boolean {
  return typeof value === "string" && UUID_V7_RE.test(value);
}

export function parseStatusEvent(raw: unknown): StatusEvent {
  if (!raw || typeof raw !== "object") {
    throw new Error("Status event must be a JSON object");
  }
  const obj = raw as Record<string, unknown>;

  if (typeof obj.correlationId !== "string" || !isUuidV7(obj.correlationId)) {
    throw new Error("Status event requires a valid UUIDv7 correlationId");
  }
  if (typeof obj.stage !== "string" || !STAGE_SET.has(obj.stage)) {
    throw new Error(`Status event has unknown stage: ${String(obj.stage)}`);
  }
  if (typeof obj.status !== "string" || !STATUS_SET.has(obj.status)) {
    throw new Error(`Status event has unknown status: ${String(obj.status)}`);
  }
  if (typeof obj.timestamp !== "string" || obj.timestamp.length < 10) {
    throw new Error("Status event requires a timestamp");
  }

  return {
    correlationId: obj.correlationId,
    stage: obj.stage as Stage,
    status: obj.status as StatusValue,
    timestamp: obj.timestamp,
    sequence: typeof obj.sequence === "number" ? obj.sequence : null,
    details:
      obj.details && typeof obj.details === "object"
        ? (obj.details as Record<string, unknown>)
        : undefined,
  };
}
