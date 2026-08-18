/**
 * Kafka consumer lifecycle: load DB topic policy, validate against Kafka,
 * connect, subscribe to consume-eligible topics, run eachBatch, periodic refresh.
 */

import { loadGatewayKafkaTopics } from "@/lib/db-scripts/fn-get-gateway-kafka-topics";
import { env } from "@/lib/env";
import { isoNowIST } from "@/lib/isoNowIST";
import { processInboundMessage } from "@/lib/kafka-ws-bridge/process-inbound-message";
import {
  logMissingTopics,
  validateConfiguredTopicsExist,
} from "@/lib/kafka-ws-bridge/kafka-topic-validation";
import {
  getTopicPolicy,
  isConsumerExcludedTopic,
  setTopicPolicy,
  type TopicPolicy,
} from "@/lib/kafka-ws-bridge/topic-policy";
import { setConsumerRef } from "@/lib/kafka-ws-bridge/state";
import { kafka } from "@/lib/kafka.config";

/** How often to reload policy from PostgreSQL (ms). */
const TOPIC_REGISTRY_REFRESH_MS = 60_000;

let refreshTimer: ReturnType<typeof setInterval> | null = null;
/** Topics we have already asked the consumer to subscribe to this process lifetime. */
const subscribedConsumeTopics = new Set<string>();

async function loadAndApplyPolicy(options: {
  phase: "startup" | "refresh";
  consumer: ReturnType<typeof kafka.consumer> | null;
}): Promise<TopicPolicy> {
  const policy = await loadGatewayKafkaTopics();
  setTopicPolicy(policy);

  console.log(
    `${isoNowIST()}\t[TopicRegistry:Log]\tPOLICY_LOADED phase=${options.phase} enabled=${policy.enabledTopics.size} consume=${policy.consumeTopics.size} ack=${policy.ackRequiredTopics.size} publish=${policy.subscriberPublishAllowlist.size}`,
  );

  const validation = await validateConfiguredTopicsExist(policy.enabledTopics);

  if (!validation.ok) {
    logMissingTopics(options.phase, validation.missingTopics);

    if (options.phase === "startup") {
      throw new Error(
        `Startup topic validation failed: configured enabled topics missing from Kafka: ${validation.missingTopics.join(", ")}`,
      );
    }
    // Runtime: log and continue (do not crash the Gateway).
  } else {
    console.log(
      `${isoNowIST()}\t[TopicRegistry:Log]\tKAFKA_VALIDATION_OK phase=${options.phase} configured=${validation.existingConfiguredTopics.length} kafkaTopics=${validation.kafkaTopicCount}`,
    );
  }

  // Subscribe only to topics that exist in Kafka and are consume-eligible.
  if (options.consumer) {
    const toSubscribe: string[] = [];
    for (const topic of policy.consumeTopics) {
      if (subscribedConsumeTopics.has(topic)) continue;
      if (validation.missingTopics.includes(topic)) continue;
      toSubscribe.push(topic);
    }

    if (toSubscribe.length > 0) {
      await options.consumer.subscribe({ topics: toSubscribe });
      for (const t of toSubscribe) {
        subscribedConsumeTopics.add(t);
      }
      console.log(
        `${isoNowIST()}\t[TopicRegistry:Log]\tCONSUMER_SUBSCRIBED phase=${options.phase} topics=${JSON.stringify(toSubscribe)}`,
      );
    }
  }

  return policy;
}

export async function startKafkaWsBridge() {
  // 1. Load policy + strict startup validation before consumer starts.
  await loadAndApplyPolicy({ phase: "startup", consumer: null });

  const policy = getTopicPolicy();
  if (policy.consumeTopics.size === 0) {
    console.log(
      `${isoNowIST()}\t[TopicRegistry:Warn]\tNO_CONSUME_TOPICS Gateway will start without consumer subscriptions`,
    );
  }

  const consumer = kafka.consumer({
    groupId: env.KAFKA_GROUP_ID,
    sessionTimeout: 60_000,
    heartbeatInterval: 3_000,
  });

  setConsumerRef(consumer);

  await consumer.connect();

  // 2. Initial subscribe to consume-eligible topics that exist in Kafka.
  const startupValidation = await validateConfiguredTopicsExist(
    policy.enabledTopics,
  );
  const subscribeList = [...policy.consumeTopics].filter(
    (t) => !startupValidation.missingTopics.includes(t),
  );

  if (subscribeList.length > 0) {
    await consumer.subscribe({ topics: subscribeList });
    for (const t of subscribeList) {
      subscribedConsumeTopics.add(t);
    }
    console.log(
      `${isoNowIST()}\t[TopicRegistry:Log]\tCONSUMER_SUBSCRIBED phase=startup topics=${JSON.stringify(subscribeList)}`,
    );
  }

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

  // 3. Periodic registry refresh (additions only; removals/disablements need restart).
  if (refreshTimer) {
    clearInterval(refreshTimer);
  }
  refreshTimer = setInterval(() => {
    void (async () => {
      try {
        await loadAndApplyPolicy({ phase: "refresh", consumer });
      } catch (err) {
        console.log(
          `${isoNowIST()}\t[TopicRegistry:Error]\tREFRESH_FAILED err=${err instanceof Error ? err.message : String(err)}`,
        );
      }
    })();
  }, TOPIC_REGISTRY_REFRESH_MS);

  console.log(
    `${isoNowIST()}\t[TopicRegistry:Log]\tREFRESH_SCHEDULED intervalMs=${TOPIC_REGISTRY_REFRESH_MS}`,
  );
}
