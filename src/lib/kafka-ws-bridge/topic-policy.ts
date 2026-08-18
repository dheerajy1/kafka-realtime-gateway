/**
 * Generic Gateway topic policy loaded from PostgreSQL via fn_get_gateway_kafka_topics().
 * Domain-neutral: no hard-coded Record Log or Jobs topic names.
 */

export type TopicPolicyRow = {
  topic_name: string;
  gateway_consume: boolean;
  ack_required: boolean;
  subscriber_publish_allowed: boolean;
  enabled: boolean;
  description: string | null;
};

export type TopicPolicy = {
  /** All enabled policy rows (source of truth after last load/refresh). */
  rows: TopicPolicyRow[];
  /** Topics the Gateway consumer should subscribe to (enabled + gateway_consume). */
  consumeTopics: Set<string>;
  /** Topics that require subscriber ACK before offset commit. */
  ackRequiredTopics: Set<string>;
  /** Topics subscribers may publish to over the authenticated WS. */
  subscriberPublishAllowlist: Set<string>;
  /** All enabled topic names (for Kafka existence validation). */
  enabledTopics: Set<string>;
};

export function buildTopicPolicy(rows: TopicPolicyRow[]): TopicPolicy {
  const enabled = rows.filter((r) => r.enabled);

  const consumeTopics = new Set<string>();
  const ackRequiredTopics = new Set<string>();
  const subscriberPublishAllowlist = new Set<string>();
  const enabledTopics = new Set<string>();

  for (const row of enabled) {
    enabledTopics.add(row.topic_name);
    if (row.gateway_consume) {
      consumeTopics.add(row.topic_name);
    }
    if (row.ack_required) {
      ackRequiredTopics.add(row.topic_name);
    }
    if (row.subscriber_publish_allowed) {
      subscriberPublishAllowlist.add(row.topic_name);
    }
  }

  return {
    rows: enabled,
    consumeTopics,
    ackRequiredTopics,
    subscriberPublishAllowlist,
    enabledTopics,
  };
}

/** Mutable in-memory policy; replaced on load/refresh. */
let currentPolicy: TopicPolicy = buildTopicPolicy([]);

export function getTopicPolicy(): TopicPolicy {
  return currentPolicy;
}

export function setTopicPolicy(policy: TopicPolicy): void {
  currentPolicy = policy;
}

export function isAckRequiredTopic(topic: string): boolean {
  return currentPolicy.ackRequiredTopics.has(topic);
}

export function isSubscriberPublishAllowed(topic: string): boolean {
  return currentPolicy.subscriberPublishAllowlist.has(topic);
}

/** True when the Gateway should not treat this topic as a consume target. */
export function isConsumerExcludedTopic(topic: string): boolean {
  return !currentPolicy.consumeTopics.has(topic);
}
