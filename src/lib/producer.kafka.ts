import { kafka } from "@/lib/kafka.config";

export type KafkaProducerLike = {
  send: (payload: {
    topic: string;
    messages: Array<{
      key?: string | null;
      value: string;
      headers?: Record<string, Buffer | string>;
    }>;
  }) => Promise<unknown>;
  connect?: () => Promise<void>;
};

export const producer = kafka.producer();

let connected = false;
let overrideProducer: KafkaProducerLike | null = null;

export async function getProducer(): Promise<KafkaProducerLike> {
  if (overrideProducer) {
    return overrideProducer;
  }
  if (!connected) {
    await producer.connect();
    connected = true;
  }
  return producer;
}

/** Test helper — inject a mock producer without connecting to brokers. */
export function setProducer(mock: KafkaProducerLike | null): void {
  overrideProducer = mock;
}
