/**
 * Parse one Kafka message value into correlationId + payload.
 */

import { MyError } from "@/lib/errors";
import { getFilePath } from "@/lib/get-file-path";
import { KafkaMsgValSchema } from "@/schemas/kafka-ws-bridge.schema";
import { ParsedInboundMessage } from "@/types/global.type";

export function parseInboundMessage(
  value: string | undefined,
): ParsedInboundMessage {
  try {
    if (!value) {
      throw new Error("EMPTY_MESSAGE");
    }

    const parsedJson = JSON.parse(value);
    const result = KafkaMsgValSchema.safeParse(parsedJson);

    if (!result.success) {
      return {
        ok: false,
        reason: "invalid-message",
        issues: result.error.issues,
      };
    }

    return {
      ok: true,
      correlationId: result.data.correlationId,
      eventPayload: result.data as Record<string, unknown>,
    };
  } catch (error: unknown) {
    if (error instanceof MyError) {
      throw error;
    }

    if (error instanceof SyntaxError) {
      return {
        ok: false,
        reason: "malformed-json",
      };
    }

    if (error instanceof Error && error.message === "EMPTY_MESSAGE") {
      return {
        ok: false,
        reason: "empty",
      };
    }

    console.log(`${getFilePath()} - unexpected parse error`, error);

    return {
      ok: false,
      reason: "invalid-message",
    };
  }
}