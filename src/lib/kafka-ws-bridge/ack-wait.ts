/**
 * ACK wait with Kafka consumer heartbeats, and commit safety gate.
 */

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
  heartbeatIntervalMs = 3000,
): Promise<void> {
  let heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  let timeoutTimer: ReturnType<typeof setTimeout> | undefined;

  const cleanup = () => {
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    if (timeoutTimer) clearTimeout(timeoutTimer);
  };

  const timeoutPromise = new Promise<never>((_, reject) => {
    timeoutTimer = setTimeout(() => {
      reject(new Error("ACK timeout while heartbeating"));
    }, timeoutMs);
  });

  // Periodically send heartbeats in background without blocking immediate ACK
  heartbeatTimer = setInterval(() => {
    heartbeat().catch(() => {
      /* heartbeat failure will surface via KafkaJS session handling */
    });
  }, heartbeatIntervalMs);

  try {
    // Prevent unhandled rejections if timeout triggers first
    ackPromise.catch(() => {});
    await Promise.race([ackPromise, timeoutPromise]);
  } finally {
    cleanup();
  }
}
