import { WsSubscriberContext } from "@/schemas/auth.schema";
import { z } from "zod";

/** Kafka → WS delivery: only correlationId is required for routing/ACK. */
export const KafkaMsgValSchema = z
  .object({
    correlationId: z.uuid({ version: "v7" }),
  })
  .passthrough();

export interface GatewayWS<
  TContext extends WsSubscriberContext = WsSubscriberContext,
  TData extends { ctx: TContext } = { ctx: TContext; clientId?: string },
> {
  send(data?: unknown, compress?: boolean): unknown;
  close?(code?: number, reason?: string): void;
  data: TData;
}

export type PendingAck = {
  topic: string;
  partition: number;
  offset: string;
  correlationId: string;
  subscriberId: number;
  subscriber: string;
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