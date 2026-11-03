type KafkaConsumer = {
  commitOffsets: (
    offsets: { topic: string; partition: number; offset: string }[],
  ) => Promise<void>;
};

export type ProcessInboundMessageArgs = {
  consumer: KafkaConsumer;
  topic: string;
  partition: number;
  message: {
    offset: string;
    value: { toString(): string } | null | undefined;
    headers?: unknown;
  };
  resolveOffset: (offset: string) => void;
  heartbeat: () => Promise<void>;
  isRunning: () => boolean;
  isStale: () => boolean;
};

export type ParsedInboundMessage =
  | { ok: true; correlationId: string; eventPayload: Record<string, unknown> }
  | {
      ok: false;
      reason: "empty" | "malformed-json" | "invalid-message";
      issues?: unknown;
    };

type RawHeaderValue = Buffer | string | number | boolean | unknown[] | null | undefined;
export type RawKafkaHeaders = Record<string, RawHeaderValue> | null | undefined;

export interface DeliverEventArgs {
  topic: string;
  partition: number;
  offset: string;
  headers: RawKafkaHeaders | unknown;
  eventPayload: Record<string, unknown>;
  /** When set, only this subscriber is marked as the delivery owner for logging. */
  ownerId?: number;
  onOwnerDelivered?: (id: number) => void;
}
