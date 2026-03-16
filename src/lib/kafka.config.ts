import { Kafka } from "kafkajs";

export const kafka = new Kafka({
  clientId: "kafka-api-gateway",
  brokers: ["yoga-node:9092"],
   requestTimeout: 30000,
});
