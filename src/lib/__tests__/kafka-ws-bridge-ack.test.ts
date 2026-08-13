import { describe, expect, test, beforeEach } from "bun:test";
import {
  registerWs,
  unregisterWs,
  commitByCorrelationId,
  countPendingForSubscriber,
  registerPendingAck,
  markPendingDelivered,
  _testGetPendingAck,
  _testHasPendingAck,
  _testPendingCount,
  shouldCommitAfterAck,
  waitForResponsibleSubscriber,
  type GatewayWS,
} from "@/lib/kafka-ws-bridge";

const CORR = "01901234-5678-7abc-8def-0123456789ab";
const CORR2 = "01901234-5678-7abc-8def-0123456789ac";

function makeWs(
  id: number,
  clientId = "pipeline-state-consumer",
): GatewayWS {
  return {
    send() {},
    data: {
      subscriptions: new Set(["record-log-status"]),
      subscriberId: id,
      clientId,
    },
  };
}

beforeEach(() => {
  for (const id of [1, 2, 7, 99]) {
    try {
      unregisterWs(id);
    } catch {
      /* ignore */
    }
  }
});

describe("ACK registration before delivery", () => {
  test("pending ACK registered before delivery and owning connection can ACK", async () => {
    registerWs(7, makeWs(7));

    // Register FIRST (as production path must)
    const ackPromise = registerPendingAck({
      topic: "record-log-status",
      partition: 0,
      offset: "100",
      correlationId: CORR,
      subscriberId: 7,
      clientId: "pipeline-state-consumer",
    });

    // Entry must exist immediately — before any "delivery"
    expect(_testHasPendingAck(CORR)).toBe(true);
    const pending = _testGetPendingAck(CORR);
    expect(pending).toBeDefined();
    expect(pending!.delivered).toBe(false);
    expect(pending!.subscriberId).toBe(7);
    expect(pending!.offset).toBe("100");

    // Simulate delivery after registration
    markPendingDelivered(CORR);
    expect(_testGetPendingAck(CORR)!.delivered).toBe(true);

    // Owning connection ACK is accepted (resolves waiter; commit is in eachBatch)
    const result = await commitByCorrelationId(CORR, 7);
    expect(result.ok).toBe(true);
    await expect(ackPromise).resolves.toBeUndefined();
    expect(_testHasPendingAck(CORR)).toBe(false);
  });

  test("ACK from wrong connection is rejected and pending remains", async () => {
    registerWs(7, makeWs(7));
    registerWs(99, makeWs(99, "other-client"));

    const ackPromise = registerPendingAck({
      topic: "record-log-status",
      partition: 0,
      offset: "100",
      correlationId: CORR,
      subscriberId: 7,
      clientId: "pipeline-state-consumer",
    });
    markPendingDelivered(CORR);

    // Wrong owner
    const bad = await commitByCorrelationId(CORR, 99);
    expect(bad.ok).toBe(false);
    expect(bad.reason).toMatch(/non-owning/);
    // Pending still present
    expect(_testHasPendingAck(CORR)).toBe(true);

    // Cleanup: disconnect owner rejects
    unregisterWs(7);
    await expect(ackPromise).rejects.toThrow(/disconnected/);
    expect(_testHasPendingAck(CORR)).toBe(false);
  });

  test("unknown correlationId ACK is rejected", async () => {
    const r = await commitByCorrelationId(CORR2, 7);
    expect(r.ok).toBe(false);
    expect(r.reason).toMatch(/not found|already/);
  });

  test("disconnect before ACK rejects pending and does not leave entry", async () => {
    registerWs(7, makeWs(7));
    const ackPromise = registerPendingAck({
      topic: "record-log-status",
      partition: 0,
      offset: "100",
      correlationId: CORR,
      subscriberId: 7,
      clientId: "pipeline-state-consumer",
    });
    markPendingDelivered(CORR);
    expect(countPendingForSubscriber(7)).toBe(1);

    unregisterWs(7);

    await expect(ackPromise).rejects.toThrow(/disconnected/);
    expect(_testHasPendingAck(CORR)).toBe(false);
    expect(countPendingForSubscriber(7)).toBe(0);
  });
});

describe("partition ordering invariant (unit-level pending state)", () => {
  test("later offset pending does not clear earlier offset pending", async () => {
    registerWs(7, makeWs(7));

    const p100 = registerPendingAck({
      topic: "record-log-status",
      partition: 0,
      offset: "100",
      correlationId: CORR,
      subscriberId: 7,
    });
    const p101 = registerPendingAck({
      topic: "record-log-status",
      partition: 0,
      offset: "101",
      correlationId: CORR2,
      subscriberId: 7,
    });

    expect(_testPendingCount()).toBe(2);
    expect(_testGetPendingAck(CORR)!.offset).toBe("100");
    expect(_testGetPendingAck(CORR2)!.offset).toBe("101");

    // ACK for 101 while 100 still pending — both still independent in map;
    // production eachBatch never delivers 101 until 100 ACKs, so this state
    // should not occur in the live path. Document that ordering is enforced
    // by sequential eachBatch processing, not by the map alone.
    unregisterWs(7);
    await expect(p100).rejects.toThrow(/disconnected/);
    await expect(p101).rejects.toThrow(/disconnected/);
  });
});

describe("UUIDv7 schema used by bridge", () => {
  test("Zod v7 accepts UUIDv7 and rejects v4", async () => {
    const { z } = await import("zod");
    const schema = z.uuid({ version: "v7" });
    expect(schema.safeParse(CORR).success).toBe(true);
    expect(
      schema.safeParse("550e8400-e29b-41d4-a716-446655440000").success,
    ).toBe(false);
    expect(schema.safeParse("01ARZ3NDEKTSV4RRFFQ69G5FAV").success).toBe(false);
  });
});

describe("stale batch / rebalance commit gate", () => {
  test("allows commit when running and not stale", () => {
    const r = shouldCommitAfterAck({
      isRunning: () => true,
      isStale: () => false,
    });
    expect(r.ok).toBe(true);
  });

  test("blocks commit when batch is stale (rebalance)", () => {
    const r = shouldCommitAfterAck({
      isRunning: () => true,
      isStale: () => true,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.reason).toBe("batch-stale");
  });

  test("blocks commit when consumer is not running", () => {
    const r = shouldCommitAfterAck({
      isRunning: () => false,
      isStale: () => false,
    });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.reason).toBe("consumer-not-running");
  });
});

describe("no-subscriber wait (controlled, no throw-to-restart)", () => {
  test("returns subscriber when one appears during wait", async () => {
    let heartbeats = 0;
    const p = waitForResponsibleSubscriber("record-log-status", {
      intervalMs: 20,
      heartbeat: async () => {
        heartbeats++;
      },
      isRunning: () => true,
      isStale: () => false,
    });

    setTimeout(() => {
      registerWs(42, {
        send() {},
        data: {
          subscriptions: new Set(["record-log-status"]),
          subscriberId: 42,
          clientId: "pipeline-state-consumer",
        },
      });
    }, 40);

    const owner = await p;
    expect(owner).not.toBeNull();
    expect(owner!.id).toBe(42);
    expect(heartbeats).toBeGreaterThan(0);
    unregisterWs(42);
  });

  test("stops without subscriber when batch becomes stale", async () => {
    let heartbeats = 0;
    let stale = false;
    setTimeout(() => {
      stale = true;
    }, 30);

    const owner = await waitForResponsibleSubscriber("record-log-status", {
      intervalMs: 15,
      heartbeat: async () => {
        heartbeats++;
      },
      isRunning: () => true,
      isStale: () => stale,
    });

    expect(owner).toBeNull();
    expect(heartbeats).toBeGreaterThan(0);
  });

  test("stops without subscriber when consumer is not running", async () => {
    let heartbeats = 0;
    let running = true;
    setTimeout(() => {
      running = false;
    }, 30);

    const owner = await waitForResponsibleSubscriber("record-log-status", {
      intervalMs: 15,
      heartbeat: async () => {
        heartbeats++;
      },
      isRunning: () => running,
      isStale: () => false,
    });

    expect(owner).toBeNull();
    expect(heartbeats).toBeGreaterThan(0);
  });

  test("heartbeats while waiting (no tight spin without heartbeat)", async () => {
    let heartbeats = 0;
    let running = true;
    const started = Date.now();
    setTimeout(() => {
      running = false;
    }, 80);

    await waitForResponsibleSubscriber("record-log-status", {
      intervalMs: 25,
      heartbeat: async () => {
        heartbeats++;
      },
      isRunning: () => running,
      isStale: () => false,
    });

    expect(Date.now() - started).toBeGreaterThanOrEqual(70);
    expect(heartbeats).toBeGreaterThanOrEqual(2);
  });
});

describe("record-log-ingest ACK-required classification", () => {
  test("isAckRequiredTopic includes ingest, status, and committed", async () => {
    const { isAckRequiredTopic } = await import("@/lib/kafka-ws-bridge");
    expect(isAckRequiredTopic("record-log-ingest")).toBe(true);
    expect(isAckRequiredTopic("record-log-status")).toBe(true);
    expect(isAckRequiredTopic("record-log-committed")).toBe(true);
    expect(isAckRequiredTopic("some-other-topic")).toBe(false);
  });

  test("ingest pending ACK: no commit without owning ACK", async () => {
    registerWs(1, {
      send() {},
      data: {
        subscriptions: new Set(["record-log-ingest"]),
        subscriberId: 1,
        clientId: "azure-sql-writer-consumer",
      },
    });

    const ackPromise = registerPendingAck({
      topic: "record-log-ingest",
      partition: 0,
      offset: "42",
      correlationId: CORR2,
      subscriberId: 1,
      clientId: "azure-sql-writer-consumer",
    });

    expect(_testHasPendingAck(CORR2)).toBe(true);
    // Wrong subscriber cannot ACK
    const bad = await commitByCorrelationId(CORR2, 99);
    expect(bad.ok).toBe(false);
    expect(_testHasPendingAck(CORR2)).toBe(true);

    // Owner ACKs
    markPendingDelivered(CORR2);
    const good = await commitByCorrelationId(CORR2, 1);
    expect(good.ok).toBe(true);
    await expect(ackPromise).resolves.toBeUndefined();
    expect(_testHasPendingAck(CORR2)).toBe(false);
  });

  test("ingest disconnect clears pending and does not leave ACK hang forever", async () => {
    registerWs(2, {
      send() {},
      data: {
        subscriptions: new Set(["record-log-ingest"]),
        subscriberId: 2,
        clientId: "azure-sql-writer-consumer",
      },
    });

    const ackPromise = registerPendingAck({
      topic: "record-log-ingest",
      partition: 1,
      offset: "7",
      correlationId: CORR,
      subscriberId: 2,
      clientId: "azure-sql-writer-consumer",
    });

    unregisterWs(2);
    expect(_testHasPendingAck(CORR)).toBe(false);
    await expect(ackPromise).rejects.toThrow(/disconnected/);
  });
});


describe("record-log-committed ACK-required classification", () => {
  test("isAckRequiredTopic includes committed", async () => {
    const { isAckRequiredTopic } = await import("@/lib/kafka-ws-bridge");
    expect(isAckRequiredTopic("record-log-committed")).toBe(true);
    expect(isAckRequiredTopic("record-log-ingest")).toBe(true);
    expect(isAckRequiredTopic("record-log-status")).toBe(true);
  });

  test("committed pending ACK: wrong owner rejected, owner ACKs", async () => {
    registerWs(1, {
      send() {},
      data: {
        subscriptions: new Set(["record-log-committed"]),
        subscriberId: 1,
        clientId: "postgresql-read-model-subscriber",
      },
    });
    registerWs(99, {
      send() {},
      data: {
        subscriptions: new Set(["record-log-committed"]),
        subscriberId: 99,
        clientId: "other",
      },
    });

    const ackPromise = registerPendingAck({
      topic: "record-log-committed",
      partition: 0,
      offset: "55",
      correlationId: CORR2,
      subscriberId: 1,
      clientId: "postgresql-read-model-subscriber",
    });

    expect(_testHasPendingAck(CORR2)).toBe(true);

    const bad = await commitByCorrelationId(CORR2, 99);
    expect(bad.ok).toBe(false);
    expect(_testHasPendingAck(CORR2)).toBe(true);

    markPendingDelivered(CORR2);
    const good = await commitByCorrelationId(CORR2, 1);
    expect(good.ok).toBe(true);
    await expect(ackPromise).resolves.toBeUndefined();
    expect(_testHasPendingAck(CORR2)).toBe(false);
  });

  test("committed disconnect clears pending (no commit path)", async () => {
    registerWs(3, {
      send() {},
      data: {
        subscriptions: new Set(["record-log-committed"]),
        subscriberId: 3,
        clientId: "postgresql-read-model-subscriber",
      },
    });

    const ackPromise = registerPendingAck({
      topic: "record-log-committed",
      partition: 2,
      offset: "9",
      correlationId: CORR,
      subscriberId: 3,
      clientId: "postgresql-read-model-subscriber",
    });

    unregisterWs(3);
    expect(_testHasPendingAck(CORR)).toBe(false);
    await expect(ackPromise).rejects.toThrow(/disconnected/);
  });
});
