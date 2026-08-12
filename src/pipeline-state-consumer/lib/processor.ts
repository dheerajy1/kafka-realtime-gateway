/**
 * Pipeline State Consumer — passive persistence only.
 * Transport-independent (no WS / Kafka knowledge).
 */

import type { PostgresDb } from "@/pipeline-state-consumer/lib/postgres";
import { parseStatusEvent } from "@/pipeline-state-consumer/lib/events";

export type ProcessorDeps = {
  db: PostgresDb;
};

export type ProcessResult =
  | {
      ok: true;
      correlationId: string;
      stage: string;
      replayed: boolean;
      sequence: number;
    }
  | { ok: false; correlationId?: string; error: string; retryable: boolean };

export async function processStatusMessage(
  rawValue: string,
  deps: ProcessorDeps,
): Promise<ProcessResult> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(rawValue);
  } catch {
    return {
      ok: false,
      error: "Malformed JSON status payload",
      retryable: false,
    };
  }

  let event;
  try {
    event = parseStatusEvent(parsed);
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Invalid status event";
    return { ok: false, error: msg, retryable: false };
  }

  try {
    const applied = await deps.db.applyStatus({
      correlationId: event.correlationId,
      stage: event.stage,
      status: event.status,
      timestamp: event.timestamp,
      details: event.details,
    });

    return {
      ok: true,
      correlationId: event.correlationId,
      stage: event.stage,
      replayed: applied.replayed,
      sequence: applied.assignedSequence,
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "PostgreSQL apply failed";
    return {
      ok: false,
      correlationId: event.correlationId,
      error: msg,
      retryable: true,
    };
  }
}
