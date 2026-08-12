import { describe, expect, test } from "bun:test";
import {
  isUuidV7,
  parseStatusEvent,
} from "@/pipeline-state-consumer/lib/events";

const VALID_V7 = "01901234-5678-7abc-8def-0123456789ab";
const VALID_V4 = "550e8400-e29b-41d4-a716-446655440000";
const SAMPLE_ULID = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

describe("isUuidV7", () => {
  test("accepts UUIDv7", () => expect(isUuidV7(VALID_V7)).toBe(true));
  test("rejects UUIDv4", () => expect(isUuidV7(VALID_V4)).toBe(false));
  test("rejects ULID", () => expect(isUuidV7(SAMPLE_ULID)).toBe(false));
  test("rejects malformed", () => expect(isUuidV7("not-a-uuid")).toBe(false));
});

describe("parseStatusEvent", () => {
  test("parses valid UUIDv7 event", () => {
    const ev = parseStatusEvent({
      correlationId: VALID_V7,
      stage: "RECEIVED",
      status: "STARTED",
      timestamp: "2026-08-10T10:00:00.000Z",
    });
    expect(ev.correlationId).toBe(VALID_V7);
  });
  test("rejects ULID", () => {
    expect(() =>
      parseStatusEvent({
        correlationId: SAMPLE_ULID,
        stage: "RECEIVED",
        status: "STARTED",
        timestamp: "2026-08-10T10:00:00.000Z",
      }),
    ).toThrow(/UUIDv7/);
  });
  test("rejects UUIDv4", () => {
    expect(() =>
      parseStatusEvent({
        correlationId: VALID_V4,
        stage: "RECEIVED",
        status: "STARTED",
        timestamp: "2026-08-10T10:00:00.000Z",
      }),
    ).toThrow(/UUIDv7/);
  });
  test("rejects non-object", () => {
    expect(() => parseStatusEvent("nope")).toThrow(/JSON object/);
  });
});
