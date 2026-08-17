/**
 * Fan-out one Kafka event frame to WS sessions subscribed to the topic.
 */

import { decodeKafkaHeaders } from "./headers";
import { wsSubscriberState } from "@/lib/ws-subscriber/ws-subscriber-state";

export function deliverEventToSubscribers(args: {
  topic: string;
  partition: number;
  offset: string;
  headers: unknown;
  eventPayload: Record<string, unknown>;
  /** When set, only this subscriber is marked as the delivery owner for logging. */
  ownerId?: number;
  onOwnerDelivered?: (id: number) => void;
}): number {
  const {
    topic,
    partition,
    offset,
    headers,
    eventPayload,
    ownerId,
    onOwnerDelivered,
  } = args;

  let delivered = 0;

  for (const [id, session] of wsSubscriberState.subscribers.entries()) {
    const subscribed =
      wsSubscriberState.topicSubscribers.get(topic)?.has(id) ||
      session.subscriptions.has(topic) ||
      session.ws.data.ctx.subscriptions.has(topic);

    if (!subscribed) continue;

    const ws = session.ws;

    ws.send(
      JSON.stringify({
        type: "event",
        topic,
        partition,
        offset,
        headers: decodeKafkaHeaders(
          headers as Record<string, unknown> | undefined,
        ),
        ...eventPayload,
      }),
    );

    delivered++;

    if (ownerId != null && id === ownerId) {
      onOwnerDelivered?.(id);
    }
  }

  return delivered;
}