/**
 * WebSocket client registry for the Kafka bridge.
 * Writes to BOTH:
 * - kafka-ws-bridge/state.wsClients (bridge Map)
 * - wsSubscriberState.subscribers (single shared routing state)
 * so selector and delivery always see the same live connection.
 */

import { isoNowIST } from "@/lib/isoNowIST";
import { pendingAcks, wsClients } from "@/lib/kafka-ws-bridge/state";
import { wsSubscriberState } from "@/lib/ws-subscriber/ws-subscriber-state";
import type { GatewayWS } from "@/schemas/kafka-ws-bridge.schema";

export function registerWs(id: number, ws: GatewayWS): void {
  wsClients.set(id, ws);
  wsSubscriberState.subscribers.set(id, {
    id,
    ws,
    subscriptions: ws.data.ctx.subscriptions,
  });
}

export function unregisterWs(id: number): void {
  wsClients.delete(id);
  wsSubscriberState.subscribers.delete(id);

  for (const [corr, pending] of pendingAcks.entries()) {
    if (pending.subscriberId === id) {
      console.log(
        `${isoNowIST()}\t[WsBridge:Disconnect]\tsubscriberId=${id}\tclientId=${pending.subscriber ?? "?"}\tpendingAcks=1\tcorr=${corr}\ttopic=${pending.topic}\tpartition=${pending.partition}\toffset=${pending.offset}`,
      );
      pendingAcks.delete(corr);
      pending.reject(
        new Error(`subscriber ${id} disconnected before ACK for corr=${corr}`),
      );
    }
  }
}
