/**
 * Validate DB-configured Gateway topics against the live Kafka cluster via KafkaJS Admin.
 * Direction: configured enabled topics must exist in Kafka (not the reverse).
 */

import { isoNowIST } from "@/lib/isoNowIST";
import { kafka } from "@/lib/kafka.config";

export type TopicValidationResult = {
  ok: boolean;
  missingTopics: string[];
  existingConfiguredTopics: string[];
  kafkaTopicCount: number;
};

/**
 * Lists Kafka topics and reports which configured names are missing.
 * Does not create topics. Caller owns connect/disconnect lifecycle of admin when reusing.
 */
export async function validateConfiguredTopicsExist(
  configuredTopicNames: Iterable<string>,
): Promise<TopicValidationResult> {
  const configured = [...new Set([...configuredTopicNames])];
  const admin = kafka.admin();

  try {
    await admin.connect();
    const kafkaTopics = await admin.listTopics();
    const kafkaSet = new Set(kafkaTopics);

    const missingTopics: string[] = [];
    const existingConfiguredTopics: string[] = [];

    for (const name of configured) {
      if (kafkaSet.has(name)) {
        existingConfiguredTopics.push(name);
      } else {
        missingTopics.push(name);
      }
    }

    return {
      ok: missingTopics.length === 0,
      missingTopics,
      existingConfiguredTopics,
      kafkaTopicCount: kafkaTopics.length,
    };
  } finally {
    try {
      await admin.disconnect();
    } catch {
      // ignore disconnect errors
    }
  }
}

export function logMissingTopics(
  context: "startup" | "refresh",
  missingTopics: string[],
): void {
  if (missingTopics.length === 0) return;
  console.log(
    `${isoNowIST()}\t[TopicRegistry:Error]\t${context.toUpperCase()}_MISSING_KAFKA_TOPICS count=${missingTopics.length} topics=${JSON.stringify(missingTopics)}`,
  );
}
