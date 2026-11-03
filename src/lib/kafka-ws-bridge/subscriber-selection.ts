/**
 * Pick / wait for a responsible WS subscriber for a Kafka topic.
 * Resolves the live socket from the shared wsSubscriberState.subscribers
 * registry (same singleton as topicSubscribers), with wsClients fallback.
 */

import { wsClients } from "@/lib/kafka-ws-bridge/state";
import { wsSubscriberState } from "@/lib/ws-subscriber/ws-subscriber-state";
import type {
  GatewayWS,
  SubscriberWaitCtx,
} from "@/schemas/kafka-ws-bridge.schema";
import { sleep } from "bun";

function resolveSubscriberWs(subscriberId: number): GatewayWS | undefined {
  const session = wsSubscriberState.subscribers.get(subscriberId);
  if (session?.ws) return session.ws;
  return wsClients.get(subscriberId);
}

export function pickResponsibleSubscriber(
  topic: string,
): { id: number; ws: GatewayWS } | null {
  const subscriberIds = wsSubscriberState.topicSubscribers.get(topic);

  if (!subscriberIds || subscriberIds.size === 0) {
    return null;
  }

  const candidates: { id: number; ws: GatewayWS }[] = [];

  for (const subscriberId of subscriberIds) {
    const ws = resolveSubscriberWs(subscriberId);
    if (ws) {
      candidates.push({ id: subscriberId, ws });
    }
  }

  if (candidates.length === 0) {
    return null;
  }

  candidates.sort((a, b) => a.id - b.id);
  return candidates[0]!;
}

export async function waitForResponsibleSubscriber(
  topic: string,
  ctx: SubscriberWaitCtx,
): Promise<{ id: number; ws: GatewayWS } | null> {
  const intervalMs = ctx.intervalMs ?? 500;

  while (true) {
    const subscriber = pickResponsibleSubscriber(topic);

    if (subscriber) {
      return subscriber;
    }

    if (!ctx.isRunning()) {
      return null;
    }

    if (ctx.isStale()) {
      return null;
    }

    try {
      await ctx.heartbeat();
    } catch {
      /* heartbeat errors surface via KafkaJS session handling */
    }

    await sleep(intervalMs);
  }
}
