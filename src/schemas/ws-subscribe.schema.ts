import z from "zod";

export const SubscribeSchema = z.object({
  type: z.literal("subscribe"),
  topic: z.string().min(1),
});

export const UnsubscribeSchema = z.object({
  type: z.literal("unsubscribe"),
  topic: z.string().min(1),
});

export const ProcessedSchema = z.object({
  type: z.literal("processed"),
  correlationId: z.uuid({ version: "v7" }),
});

/**
 * WS publish request (subscriber → gateway).
 * requestId correlates the command; do not overload pipeline correlationId.
 * Optional headers are forwarded as Kafka message headers (retry/DLQ metadata).
 */
export const WsPublishCommandSchema = z.object({
  type: z.literal("publish"),
  requestId: z.uuid().min(1),
  topic: z.string().min(1),
  key: z.string().min(1).optional(),
  /** Opaque event payload — forwarded as Kafka message value JSON. */
  value: z.record(z.string(), z.unknown()),
  /** Optional Kafka headers (string values). */
  headers: z.record(z.string(), z.string()).optional(),
});

export type WsPublishCommand = z.infer<typeof WsPublishCommandSchema>;

export type PublishSuccess = {
  ok: true;
  requestId: string;
  topic: string;
  partition: number;
  offset: string;
};

export type PublishFailure = {
  ok: false;
  requestId: string;
  error: string;
  reason: string;
};

export const MessageSchema = z.union([
  SubscribeSchema,
  UnsubscribeSchema,
  ProcessedSchema,
  WsPublishCommandSchema,
]);
