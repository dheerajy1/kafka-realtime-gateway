/**
 * Kafka consumer lifecycle: connect, subscribe (excluding DLQs), run eachBatch.
 */

import { env } from "@/lib/env";
import { isoNowIST } from "@/lib/isoNowIST";
import { processInboundMessage } from "@/lib/kafka-ws-bridge/process-inbound-message";
import {
  consumerTopicPattern,
  isConsumerExcludedTopic,
} from "@/lib/kafka-ws-bridge/record-log-topics";
import { setConsumerRef } from "@/lib/kafka-ws-bridge/state";
import { kafka } from "@/lib/kafka.config";

export async function startKafkaWsBridge() {
  const consumer = kafka.consumer({
    groupId: env.KAFKA_GROUP_ID,
    sessionTimeout: 60_000,
    heartbeatInterval: 3_000,
  });

  setConsumerRef(consumer);

  await consumer.connect();

  await consumer.subscribe({
    topic: consumerTopicPattern,
  });

  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
    }) => {
      const { topic, partition, messages } = batch;

      if (isConsumerExcludedTopic(topic)) {
        console.log(
          `${isoNowIST()}\t[WsBridge:Log]\tSKIP_EXCLUDED_TOPIC topic=${topic} partition=${partition} messages=${messages.length}`,
        );
        return;
      }

      for (const message of messages) {
        if (!isRunning() || isStale()) break;

        const result = await processInboundMessage({
          consumer,
          topic,
          partition,
          message,
          resolveOffset,
          heartbeat,
          isRunning,
          isStale,
        });

        if (result === "stop-batch") {
          break;
        }
      }
    },
  });
}
