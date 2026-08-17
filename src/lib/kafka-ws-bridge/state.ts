/**
 * Shared runtime state for the Kafka ↔ WebSocket bridge.
 */

import { kafka } from "@/lib/kafka.config";
import type { GatewayWS, PendingAck } from "@/schemas/kafka-ws-bridge.schema";

export const pendingAcks = new Map<string, PendingAck>();

export const wsClients = new Map<number, GatewayWS>();

export let consumerRef: ReturnType<typeof kafka.consumer> | null = null;

export function setConsumerRef(
  ref: ReturnType<typeof kafka.consumer> | null,
): void {
  consumerRef = ref;
}
