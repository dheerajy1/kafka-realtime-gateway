import { Kafka } from "kafkajs";

export const kafka = new Kafka({
  clientId: "kafka-realtime-gateway",
  brokers: ["yoga-node:9092"],
   requestTimeout: 30000,
});
