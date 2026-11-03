/**
 * Pending ACK registration and commit-by-correlationId.
 */

import { isoNowIST } from "@/lib/isoNowIST";
import type { PendingAck } from "@/schemas/kafka-ws-bridge.schema";
import { pendingAcks } from "@/lib/kafka-ws-bridge/state";

export function registerPendingAck(args: {
  topic: string;
  partition: number;
  offset: string;
  correlationId: string;
  subscriberId: number;
  subscriber: string;
}): Promise<void> {
  const { topic, partition, offset, correlationId, subscriberId, subscriber } =
    args;

  if (pendingAcks.has(correlationId)) {
    const prev = pendingAcks.get(correlationId)!;
    prev.reject(new Error(`superseded pending ACK for corr=${correlationId}`));
    pendingAcks.delete(correlationId);
  }

  return new Promise<void>((resolve, reject) => {
    pendingAcks.set(correlationId, {
      topic,
      partition,
      offset,
      correlationId,
      subscriberId,
      subscriber,
      resolve,
      reject,
      delivered: false,
    });
  });
}

export function markPendingDelivered(correlationId: string): void {
  const p = pendingAcks.get(correlationId);
  if (p) p.delivered = true;
}

export function countPendingForSubscriber(subscriberId: number): number {
  let n = 0;
  for (const p of pendingAcks.values()) {
    if (p.subscriberId === subscriberId) n++;
  }
  return n;
}

export async function commitByCorrelationId({
  correlationId,
  fromSubscriberId,
  subscriber
}:{
  correlationId: string;
  fromSubscriberId: number;
  subscriber: string;
}): Promise<{ ok: boolean; reason?: string }> {
  const info = pendingAcks.get(correlationId);

  if (!info) {
    console.log(
      `${isoNowIST()}\t[WsBridge:ACK]\tREJECTED corr=${correlationId}\tSubscriber=${subscriber}subscriberId=${fromSubscriberId}\treason=not-found`,
    );
    return { ok: false, reason: "offset not found or already committed" };
  }

  if (info.subscriberId !== fromSubscriberId) {
    console.log(
      `${isoNowIST()}\t[WsBridge:ACK]\tREJECTED corr=${correlationId}\tsubscriberId=${fromSubscriberId}\townerSubscriberId=${info.subscriberId}\treason=wrong-owner`,
    );
    return { ok: false, reason: "ACK from non-owning connection" };
  }

  console.log(
    `${isoNowIST()}\t[WsBridge:ACK]\ttopic=${info.topic}\tpartition=${info.partition}\toffset=${info.offset}\tcorr=${correlationId}\tsubscriberId=${info.subscriberId}\tsubscriber=${info.subscriber ?? "?"}`,
  );

  pendingAcks.delete(correlationId);
  info.resolve();
  return { ok: true };
}

export function _testGetPendingAck(
  correlationId: string,
): PendingAck | undefined {
  return pendingAcks.get(correlationId);
}

export function _testHasPendingAck(correlationId: string): boolean {
  return pendingAcks.has(correlationId);
}

export function _testPendingCount(): number {
  return pendingAcks.size;
}
