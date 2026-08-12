
import { describe, expect, test } from "bun:test";
import { processStatusMessage } from "@/pipeline-state-consumer/lib/processor";
import type { PostgresDb, ApplyStatusResult } from "@/pipeline-state-consumer/lib/postgres";

const VALID_V7 = "01901234-5678-7abc-8def-0123456789ab";

function mockDb(opts: { fail?: boolean; replayed?: boolean }): PostgresDb {
  return {
    async applyStatus() {
      if (opts.fail) throw new Error("db down");
      return {
        result: "SUCCESS",
        replayed: opts.replayed ?? false,
        assignedSequence: 1,
        currentStage: "RECEIVED",
        currentStatus: "STARTED",
      } satisfies ApplyStatusResult;
    },
    async close() {},
  };
}

describe("processStatusMessage", () => {
  test("malformed JSON", async () => {
    const r = await processStatusMessage("{", { db: mockDb({}) });
    expect(r.ok).toBe(false);
  });
  test("PG success => ok", async () => {
    const r = await processStatusMessage(
      JSON.stringify({
        correlationId: VALID_V7,
        stage: "RECEIVED",
        status: "STARTED",
        timestamp: "2026-08-10T10:00:00.000Z",
      }),
      { db: mockDb({}) },
    );
    expect(r.ok).toBe(true);
  });
  test("PG failure => retryable", async () => {
    const r = await processStatusMessage(
      JSON.stringify({
        correlationId: VALID_V7,
        stage: "RECEIVED",
        status: "STARTED",
        timestamp: "2026-08-10T10:00:00.000Z",
      }),
      { db: mockDb({ fail: true }) },
    );
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.retryable).toBe(true);
  });
  test("replayed", async () => {
    const r = await processStatusMessage(
      JSON.stringify({
        correlationId: VALID_V7,
        stage: "RECEIVED",
        status: "STARTED",
        timestamp: "2026-08-10T10:00:00.000Z",
      }),
      { db: mockDb({ replayed: true }) },
    );
    expect(r.ok).toBe(true);
    if (r.ok) expect(r.replayed).toBe(true);
  });
});
