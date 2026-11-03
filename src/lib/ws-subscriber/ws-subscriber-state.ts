import type { GatewayWS } from "@/schemas/kafka-ws-bridge.schema";

export interface SubscriberSession {
  id: number;
  ws: GatewayWS;
  subscriptions: Set<string>;
}

export const wsSubscriberState = {
  subscriberCounter: 0,
  subscribers: new Map<number, SubscriberSession>(),
  topicSubscribers: new Map<string, Set<number>>(),
};