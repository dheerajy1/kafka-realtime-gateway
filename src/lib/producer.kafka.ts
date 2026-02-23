import { kafka } from "@/lib/kafka.config";

export const producer = kafka.producer();

let connected = false;

export async function getProducer() {
  if (!connected) {
    await producer.connect();
    connected = true;
  }
  return producer;
}
