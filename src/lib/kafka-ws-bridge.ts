import { MyError, errors } from "@/lib/errors";
import { kafka } from "@/lib/kafka.config";
import { z } from 'zod';

const KafkaMsgValSchema = z.object({
  correlationId: z.uuid(),           // or .regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i) if you want strict v4
  apiUserId: z.number().int().positive(),
  jobName: z.string().min(1),
  timestamp: z.string().min(1),
});

// type KafkaMsgVal = z.infer<typeof KafkaMsgValSchema>;

/**
 * Minimal WS shape
 */
export interface GatewayWS {
  send(data: string): void;
  data: {
    subscriptions: Set<string>;
  };
}

/**
 * WS clients
 */
const wsClients = new Set<GatewayWS>();

/**
 * correlationId → offset info
 */
const pendingOffsets = new Map<
  string,
  { topic: string; partition: number; offset: string }
>();

let consumerRef: ReturnType<typeof kafka.consumer> | null = null;

export function registerWs(ws: GatewayWS) {
  wsClients.add(ws);
}

export function unregisterWs(ws: GatewayWS) {
  wsClients.delete(ws);
}

/**
 * Called from ws-subscribe when client ACKs
 */
export async function commitByCorrelationId(correlationId: string) {
  const info = pendingOffsets.get(correlationId);
  if (!info || !consumerRef) return;

  await consumerRef.commitOffsets([
    {
      topic: info.topic,
      partition: info.partition,
      offset: (Number(info.offset) + 1).toString(),
    },
  ]);

  pendingOffsets.delete(correlationId);
}

export async function startKafkaWsBridge() {
  const consumer = kafka.consumer({
    groupId: "kafka-realtime-gateway-ws",
  });

  consumerRef = consumer;

  await consumer.connect();

  await consumer.subscribe({
    topic: /^(?!__).*$/,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value?.toString();
      if (!value) return;

      const result = KafkaMsgValSchema.safeParse(JSON.parse(value));

      if (!result.success) {
        throw new MyError({
          code: "BAD_REQUEST",
          message: errors.BAD_REQUEST.INVALID_INPUT.message,
          error: errors.BAD_REQUEST.INVALID_INPUT.error,
        });
      }

      const { correlationId } = result.data;

      // store offset for later commit
      pendingOffsets.set(correlationId, {
        topic,
        partition,
        offset: message.offset,
      });

      for (const ws of wsClients) {
        if (!ws.data.subscriptions.has(topic)) continue;

        ws.send(
          JSON.stringify({
            type: "event",
            topic,
            ...result.data,
          })
        );
      }
    },
  });
}
