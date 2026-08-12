import { env } from "@/lib/env";
import { Kafka } from "kafkajs";

export const kafka = new Kafka({
  clientId: env.KAFKA_CLIENT_ID,
  brokers: [env.KAFKA_BROKERS],
   requestTimeout: 30000,
});
