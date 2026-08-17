/**
 * Phase 7C — registered WS must be visible to subscriber-selection
 * via the shared registry (same Map instance).
 */

import { describe, expect, test, beforeEach } from "bun:test";
import { registerWs, unregisterWs } from "@/lib/kafka-ws-bridge/ws-registry";
import { pickResponsibleSubscriber } from "@/lib/kafka-ws-bridge/subscriber-selection";
import { wsClients } from "@/lib/kafka-ws-bridge/state";
import { wsSubscriberState } from "@/lib/ws-subscriber/ws-subscriber-state";
import type { GatewayWS } from "@/schemas/kafka-ws-bridge.schema";

function makeFakeWs(subscriberId: number): GatewayWS {
  const subscriptions = new Set<string>();
  return {
    send() {},
    data: {
      ctx: {
        subscriberId,
        subscriptions,
        xSubscriberId: "record-log-api",
      },
      clientId: "test-client",
    },
  };
}

describe("ws registry ↔ subscriber selection", () => {
  beforeEach(() => {
    wsClients.clear();
    wsSubscriberState.subscribers.clear();
    wsSubscriberState.topicSubscribers.clear();
    wsSubscriberState.subscriberCounter = 0;
  });

  test("registerWs populates both wsClients and wsSubscriberState.subscribers", () => {
    const ws = makeFakeWs(1);
    registerWs(1, ws);
    expect(wsClients.has(1)).toBe(true);
    expect(wsSubscriberState.subscribers.has(1)).toBe(true);
    expect(wsSubscriberState.subscribers.get(1)!.ws).toBe(ws);
  });

  test("pickResponsibleSubscriber finds registered subscriber for topic", () => {
    const ws = makeFakeWs(1);
    registerWs(1, ws);
    ws.data.ctx.subscriptions.add("record-log-ingest-write-model");
    wsSubscriberState.topicSubscribers.set(
      "record-log-ingest-write-model",
      new Set([1]),
    );

    const picked = pickResponsibleSubscriber("record-log-ingest-write-model");
    expect(picked).not.toBeNull();
    expect(picked!.id).toBe(1);
    expect(picked!.ws).toBe(ws);
  });

  test("unregister removes subscriber from selection", () => {
    const ws = makeFakeWs(1);
    registerWs(1, ws);
    wsSubscriberState.topicSubscribers.set(
      "record-log-ingest-write-model",
      new Set([1]),
    );
    unregisterWs(1);
    wsSubscriberState.topicSubscribers
      .get("record-log-ingest-write-model")
      ?.delete(1);

    expect(pickResponsibleSubscriber("record-log-ingest-write-model")).toBeNull();
    expect(wsClients.has(1)).toBe(false);
    expect(wsSubscriberState.subscribers.has(1)).toBe(false);
  });

  test("wsClients and subscribers Maps stay aligned after register", () => {
    registerWs(7, makeFakeWs(7));
    expect([...wsClients.keys()]).toEqual([7]);
    expect([...wsSubscriberState.subscribers.keys()]).toEqual([7]);
  });
});
