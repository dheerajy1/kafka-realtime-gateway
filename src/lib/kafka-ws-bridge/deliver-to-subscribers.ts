/**
 * Fan-out one Kafka event frame to WS sessions subscribed to the topic.
 */

import { wsSubscriberState } from "@/lib/ws-subscriber/ws-subscriber-state";
import { DeliverEventArgs, RawKafkaHeaders } from "@/types/global.type";

/**
 * Normalizes a single Kafka header value into a UTF-8 string.
 */
function decodeHeaderValue(val: unknown): string {
  if (Buffer.isBuffer(val)) {
    return val.toString("utf8");
  }
  if (typeof val === "string") {
    return val;
  }
  if (Array.isArray(val) && val.length > 0) {
    const first = val[0];
    return Buffer.isBuffer(first) ? first.toString("utf8") : String(first);
  }
  return String(val);
}

/**
 * Decodes KafkaJS message headers into a clean string map for WS event frames.
 */
export function decodeKafkaHeaders(
  headers: RawKafkaHeaders | unknown,
): Record<string, string> {
  if (!headers || typeof headers !== "object") return {};

  const decoded: Record<string, string> = {};

  for (const [key, value] of Object.entries(headers)) {
    if (value != null) {
      decoded[key] = decodeHeaderValue(value);
    }
  }

  return decoded;
}

export function deliverEventToSubscribers(args: DeliverEventArgs): number {
  const {
    topic,
    partition,
    offset,
    headers,
    eventPayload,
    ownerId,
    onOwnerDelivered,
  } = args;

  // Decode headers once per event instead of on every iteration
  const decodedHeaders = decodeKafkaHeaders(headers);

  const serializedMessage = JSON.stringify({
    type: "event",
    topic,
    partition,
    offset,
    headers: decodedHeaders,
    ...eventPayload,
  });

  const topicSubscribers = wsSubscriberState.topicSubscribers.get(topic);
  let delivered = 0;

  for (const [id, session] of wsSubscriberState.subscribers.entries()) {
    const isSubscribed =
      topicSubscribers?.has(id) ||
      session.subscriptions.has(topic) ||
      session.ws.data.ctx.subscriptions.has(topic);

    if (!isSubscribed) continue;

    session.ws.send(serializedMessage);
    delivered++;

    if (ownerId != null && id === ownerId) {
      onOwnerDelivered?.(id);
    }
  }

  return delivered;
}
