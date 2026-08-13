import { kafka } from "@/lib/kafka.config";
import { z } from "zod";
import { isoNowIST } from "@/lib/isoNowIST";
import { env } from "@/lib/env";

const ACK_REQUIRED_TOPICS = new Set<string>([
  "record-log-ingest",
  "record-log-status",
  "record-log-committed",
]);

export function isAckRequiredTopic(topic: string): boolean {
  return ACK_REQUIRED_TOPICS.has(topic);
}

const baseMsgSchema = z.object({
  correlationId: z.uuid({ version: "v7" }).min(1, "Correlation ID is required"),
  topic: z.string().min(1, "Topic is required"),
  jobName: z.string().min(1, "Job name is required"),
  clientRequestId: z.string().min(1, "Client Request ID is required"),
  timestamp: z.string().min(1, "Timestamp is required"),
  apiUserId: z.number().int().positive().optional(),
  sequence: z.number().nullable().optional(),
});

const ingestMsgSchema = baseMsgSchema.extend({
  eventType: z.literal("ingest"),
  records: z.array(z.string()).min(1, "At least one record is required"),
  username: z.string().nullable().optional(),
  role: z.string().nullable().optional(),
});

const statusMsgSchema = baseMsgSchema.extend({
  eventType: z.literal("status"),
  stage: z.string().optional(),
  status: z.string().optional(),
  details: z.record(z.string(), z.unknown()).optional(),
});

const committedMsgSchema = baseMsgSchema.extend({
  eventType: z.literal("committed"),
  records: z.array(z.string()).min(1, "At least one record is required").optional(),
  username: z.string().nullable().optional(),
  role: z.string().nullable().optional(),
  details: z.record(z.string(), z.unknown()).optional(),
});

const KafkaMsgValSchema = z.discriminatedUnion("eventType", [
  ingestMsgSchema,
  statusMsgSchema,
  committedMsgSchema,
]);

export interface GatewayWS {
  send(data: string | Record<string, unknown>): void;
  data: {
    subscriptions: Set<string>;
    subscriberId: number;
    clientId?: string;
  };
}

export const wsClients = new Map<number, GatewayWS>();

type PendingAck = {
  topic: string;
  partition: number;
  offset: string;
  correlationId: string;
  subscriberId: number;
  clientId?: string;
  resolve: () => void;
  reject: (err: Error) => void;
  delivered: boolean;
};

const pendingAcks = new Map<string, PendingAck>();

let _consumerRef: ReturnType<typeof kafka.consumer> | null = null;

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

export function registerPendingAck(args: {
  topic: string;
  partition: number;
  offset: string;
  correlationId: string;
  subscriberId: number;
  clientId?: string;
}): Promise<void> {
  const { topic, partition, offset, correlationId, subscriberId, clientId } =
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
      clientId,
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

export function registerWs(id: number, ws: GatewayWS) {
  wsClients.set(id, ws);
}

export function unregisterWs(id: number) {
  wsClients.delete(id);
  for (const [corr, pending] of pendingAcks.entries()) {
    if (pending.subscriberId === id) {
      console.log(
        `${isoNowIST()}\t[WsBridge:Disconnect]\tsubscriberId=${id}\tclientId=${pending.clientId ?? "?"}\tpendingAcks=1\tcorr=${corr}\ttopic=${pending.topic}\tpartition=${pending.partition}\toffset=${pending.offset}`,
      );
      pendingAcks.delete(corr);
      pending.reject(
        new Error(`subscriber ${id} disconnected before ACK for corr=${corr}`),
      );
    }
  }
}

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

export function countPendingForSubscriber(subscriberId: number): number {
  let n = 0;
  for (const p of pendingAcks.values()) {
    if (p.subscriberId === subscriberId) n++;
  }
  return n;
}

export async function commitByCorrelationId(
  correlationId: string,
  fromSubscriberId: number,
): Promise<{ ok: boolean; reason?: string }> {
  const info = pendingAcks.get(correlationId);

  if (!info) {
    console.log(
      `${isoNowIST()}\t[WsBridge:ACK]\tREJECTED corr=${correlationId}\tsubscriberId=${fromSubscriberId}\treason=not-found`,
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
    `${isoNowIST()}\t[WsBridge:ACK]\ttopic=${info.topic}\tpartition=${info.partition}\toffset=${info.offset}\tcorr=${correlationId}\tsubscriberId=${info.subscriberId}\tclientId=${info.clientId ?? "?"}`,
  );

  pendingAcks.delete(correlationId);
  info.resolve();
  return { ok: true };
}

function pickResponsibleSubscriber(
  topic: string,
): { id: number; ws: GatewayWS } | null {
  const candidates: { id: number; ws: GatewayWS }[] = [];
  for (const [id, ws] of wsClients.entries()) {
    if (ws.data.subscriptions.has(topic)) {
      candidates.push({ id, ws });
    }
  }
  if (candidates.length === 0) return null;
  const preferred = candidates.find(
    (c) => c.ws.data.clientId === "pipeline-state-consumer",
  );
  if (preferred) return preferred;
  candidates.sort((a, b) => a.id - b.id);
  return candidates[0]!;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export type SubscriberWaitCtx = {
  heartbeat: () => Promise<void>;
  isRunning: () => boolean;
  isStale: () => boolean;
  intervalMs?: number;
};

export async function waitForResponsibleSubscriber(
  topic: string,
  ctx: SubscriberWaitCtx,
): Promise<{ id: number; ws: GatewayWS } | null> {
  const intervalMs = ctx.intervalMs ?? 500;
  while (true) {
    const sub = pickResponsibleSubscriber(topic);
    if (sub) return sub;

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

async function waitForAckWithHeartbeat(
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

export async function startKafkaWsBridge() {
  const consumer = kafka.consumer({
    groupId: env.KAFKA_GROUP_ID,
    sessionTimeout: 60_000,
    heartbeatInterval: 3_000,
  });

  _consumerRef = consumer;

  await consumer.connect();

  await consumer.subscribe({
    topic: /^(?!__).*$/,
  });

  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
    }) => {
      const { topic, partition, messages } = batch;

      for (const message of messages) {
        if (!isRunning() || isStale()) break;

        const value = message.value?.toString();

        if (!value) {
          resolveOffset(message.offset);

          await consumer.commitOffsets([
            {
              topic,
              partition,
              offset: (BigInt(message.offset) + 1n).toString(),
            },
          ]);

          await heartbeat();

          continue;
        }

        let parsedJson: unknown;

        try {
          parsedJson = JSON.parse(value);
        } catch {
          console.log(
            `${isoNowIST()}\t[WsBridge:Error]\tMALFORMED JSON topic=${topic} partition=${partition} offset=${message.offset}`,
          );
          resolveOffset(message.offset);

          await consumer.commitOffsets([
            {
              topic,
              partition,
              offset: (BigInt(message.offset) + 1n).toString(),
            },
          ]);
          await heartbeat();
          continue;
        }

        const result = KafkaMsgValSchema.safeParse(parsedJson);

        if (!result.success) {
          console.log(
            `${isoNowIST()}\t[WsBridge:Error]\tINVALID MESSAGE topic=${topic} partition=${partition} offset=${message.offset} issues=${JSON.stringify(result.error.issues)}`,
          );

          resolveOffset(message.offset);

          await consumer.commitOffsets([
            {
              topic,
              partition,
              offset: (BigInt(message.offset) + 1n).toString(),
            },
          ]);

          await heartbeat();

          continue;
        }

        const { eventType, correlationId } = result.data;

        const requiresAck = isAckRequiredTopic(topic);

        let owner = pickResponsibleSubscriber(topic);

        if (!owner && requiresAck) {
          console.log(
            `${isoNowIST()}\t[WsBridge:Log]\tWAITING_FOR_SUBSCRIBER topic=${topic} corr=${correlationId} partition=${partition} offset=${message.offset}`,
          );

          owner = await waitForResponsibleSubscriber(topic, {
            heartbeat,
            isRunning,
            isStale,
          });
        }

        if (!owner) {
          if (requiresAck) {
            console.log(
              `${isoNowIST()}\t[WsBridge:Log]\tNO_SUBSCRIBER_STOP topic=${topic} corr=${correlationId} partition=${partition} offset=${message.offset} reason=stopped-or-stale`,
            );
            break;
          }
          console.log(
            `${isoNowIST()}\t[WsBridge:Log]\tDROP topic=${topic} corr=${correlationId} reason=no-subscribers`,
          );
          resolveOffset(message.offset);
          await consumer.commitOffsets([
            {
              topic,
              partition,
              offset: (BigInt(message.offset) + 1n).toString(),
            },
          ]);
          await heartbeat();
          continue;
        }

        if (!requiresAck) {
          let delivered = 0;

          for (const [, ws] of wsClients.entries()) {
            if (!ws.data.subscriptions.has(topic)) continue;
            ws.send(
              JSON.stringify({
                type: "event",
                ...result.data,
              }),
            );
            delivered++;
          }

          resolveOffset(message.offset);

          await consumer.commitOffsets([
            {
              topic,
              partition,
              offset: (BigInt(message.offset) + 1n).toString(),
            },
          ]);

          console.log(
            `${isoNowIST()}\t[WsBridge:Log]\tKafka → Gateway → Subscribers | topic=${topic} corr=${correlationId} delivered=${delivered} autoCommitted=true`,
          );

          await heartbeat();

          continue;
        }

        const ackPromise = registerPendingAck({
          topic,
          partition,
          offset: message.offset,
          correlationId,
          subscriberId: owner.id,
          clientId: owner.ws.data.clientId,
        });

        for (const [id, ws] of wsClients.entries()) {
          if (!ws.data.subscriptions.has(topic)) continue;

          ws.send(
            JSON.stringify({
              type: "event",
              ...result.data,
            }),
          );

          if (id === owner.id) {
            markPendingDelivered(correlationId);

            console.log(
              `${isoNowIST()}\t[WsBridge:Deliver]\ttopic=${topic}\tpartition=${partition}\toffset=${message.offset}\tcorr=${correlationId}\tsubscriberId=${id}\tclientId=${ws.data.clientId ?? "?"}`,
            );
          }
        }

        const timeoutMs = 5 * 60 * 1000;

        try {
          await waitForAckWithHeartbeat(ackPromise, heartbeat, timeoutMs);
        } catch (err) {
          const still = pendingAcks.get(correlationId);
          if (still) {
            pendingAcks.delete(correlationId);
          }

          console.log(
            `${isoNowIST()}\t[WsBridge:Log]\tACK_WAIT_FAILED topic=${topic} partition=${partition} offset=${message.offset} corr=${correlationId} err=${err instanceof Error ? err.message : String(err)}`,
          );

          throw err;
        }

        const commitGate = shouldCommitAfterAck({ isRunning, isStale });

        if (!commitGate.ok) {
          console.log(
            `${isoNowIST()}\t[WsBridge:Log]\tSKIP_COMMIT_STALE topic=${topic} partition=${partition} offset=${message.offset} corr=${correlationId} reason=${commitGate.reason}`,
          );

          throw new Error(
            `ACK arrived but batch is not safe to commit: ${commitGate.reason} corr=${correlationId}`,
          );
        }

        const nextOffset = (BigInt(message.offset) + 1n).toString();

        resolveOffset(message.offset);

        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: nextOffset,
          },
        ]);

        console.log(
          `${isoNowIST()}\t[WsBridge:Commit]\ttopic=${topic}\tpartition=${partition}\tcommittedOffset=${nextOffset}\tcorr=${correlationId}\tclientId=${owner.ws.data.clientId ?? "?"}`,
        );

        await heartbeat();
      }
    },
  });
}