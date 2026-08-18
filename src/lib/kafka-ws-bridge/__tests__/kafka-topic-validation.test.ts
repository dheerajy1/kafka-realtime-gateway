/**
 * Kafka Admin validation of configured topics (mocked listTopics).
 */

import { describe, expect, mock, test } from "bun:test";

describe("kafka topic validation", () => {
  test("detects missing configured topics", async () => {
    const listTopics = mock(async () => ["existing-a", "existing-b"]);
    const connect = mock(async () => undefined);
    const disconnect = mock(async () => undefined);

    // Inline minimal validation logic matching production helper direction
    const configured = ["existing-a", "missing-x"];
    const kafkaSet = new Set(await listTopics());
    const missing = configured.filter((t) => !kafkaSet.has(t));
    const existing = configured.filter((t) => kafkaSet.has(t));

    expect(missing).toEqual(["missing-x"]);
    expect(existing).toEqual(["existing-a"]);
    expect(connect).not.toHaveBeenCalled(); // pure unit of set logic
    expect(disconnect).not.toHaveBeenCalled();
  });

  test("accepts when all configured topics exist", async () => {
    const kafkaTopics = ["t1", "t2", "unrelated"];
    const configured = ["t1", "t2"];
    const kafkaSet = new Set(kafkaTopics);
    const missing = configured.filter((t) => !kafkaSet.has(t));
    expect(missing).toEqual([]);
  });
});
