/**
 * ACK wait with Kafka consumer heartbeats, and commit safety gate.
 */

import { sleep } from "bun";

export function shouldCommitAfterAck(ctx: {
  isRunning: () => boolean;
  isStale: () => boolean;
}): { ok: true } | { ok: false; reason: string } {
  if (!ctx.isRunning()) {
    return { ok: false, reason: "consumer-not-running" };
  }
  if (ctx.isStale()) {
    return { ok: false, reason: "batch-stale" };
  }
  return { ok: true };
}

export async function waitForAckWithHeartbeat(
  ackPromise: Promise<void>,
  heartbeat: () => Promise<void>,
  timeoutMs: number,
): Promise<void> {
  let settled = false;
  const tracked = ackPromise.then(
    () => {
      settled = true;
    },
    (err) => {
      settled = true;
      throw err;
    },
  );

  const deadline = Date.now() + timeoutMs;
  while (!settled && Date.now() < deadline) {
    try {
      await heartbeat();
    } catch {
      /* heartbeat failure will surface via KafkaJS session handling */
    }
    await Promise.race([tracked, sleep(3000).then(() => undefined)]);
  }

  if (!settled) {
    throw new Error("ACK timeout while heartbeating");
  }
  await tracked;
}
