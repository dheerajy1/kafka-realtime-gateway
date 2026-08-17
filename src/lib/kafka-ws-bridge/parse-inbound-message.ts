/**
 * Parse one Kafka message value into correlationId + payload.
 */

import { KafkaMsgValSchema } from "@/schemas/kafka-ws-bridge.schema";

export type ParsedInboundMessage =
  | { ok: true; correlationId: string; eventPayload: Record<string, unknown> }
  | { ok: false; reason: "empty" | "malformed-json" | "invalid-message"; issues?: unknown };

export function parseInboundMessage(
  value: string | undefined,
): ParsedInboundMessage {
  if (!value) {
    return { ok: false, reason: "empty" };
  }

  let parsedJson: unknown;

  try {
    parsedJson = JSON.parse(value);
  } catch {
    return { ok: false, reason: "malformed-json" };
  }

  const result = KafkaMsgValSchema.safeParse(parsedJson);

  if (result.success) {
    return {
      ok: true,
      correlationId: result.data.correlationId,
      eventPayload: { ...result.data },
    };
  }

  const loose = parsedJson as Record<string, unknown> | null;
  const cid =
    loose &&
    typeof loose === "object" &&
    typeof loose.correlationId === "string"
      ? loose.correlationId
      : null;

  const uuidV7 =
    /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

  if (!cid || !uuidV7.test(cid)) {
    return {
      ok: false,
      reason: "invalid-message",
      issues: result.error.issues,
    };
  }

  return {
    ok: true,
    correlationId: cid,
    eventPayload: { ...loose },
  };
}