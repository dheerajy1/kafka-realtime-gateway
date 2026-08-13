import { z } from "zod";

export const baseMsgSchema = z.object({
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

export const KafkaMsgValSchema = z.discriminatedUnion("eventType", [
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

export type PendingAck = {
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

export type SubscriberWaitCtx = {
  heartbeat: () => Promise<void>;
  isRunning: () => boolean;
  isStale: () => boolean;
  intervalMs?: number;
};