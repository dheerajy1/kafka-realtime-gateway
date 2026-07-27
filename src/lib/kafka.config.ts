import { env } from "@/lib/env";
import { Kafka } from "kafkajs";

export const kafka = new Kafka({
  clientId: "kafka-api-gateway",
  brokers: [env.KAFKA_BROKERS],
   requestTimeout: 30000,
});
