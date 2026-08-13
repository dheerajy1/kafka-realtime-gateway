/**
 * Subscriber → gateway Kafka publish (over the existing authenticated subscribe WS).
 * Gateway is the sole Kafka producer for these pipeline topics.
 */
import { getProducer } from "@/lib/producer.kafka";
import { isoNowIST } from "@/lib/isoNowIST";
import {
  PublishFailure,
  PublishSuccess,
  WsPublishCommand,
} from "@/types/ws-subscribe";

/** Topics subscribers may publish via the subscribe WS. */
export const SUBSCRIBER_PUBLISH_ALLOWLIST = new Set<string>([
  "record-log-committed",
  "record-log-status",
]);

export function isSubscriberPublishAllowed(topic: string): boolean {
  return SUBSCRIBER_PUBLISH_ALLOWLIST.has(topic);
}

/**
 * Validate and publish via the gateway Kafka producer.
 * Does not ACK inbound Kafka records — that remains subscriber-owned.
 */
export async function handleSubscriberPublish(
  cmd: WsPublishCommand,
  meta: { subscriberId: number; clientId?: string },
): Promise<PublishSuccess | PublishFailure> {
  const { requestId, topic, key, value } = cmd;

  if (!isSubscriberPublishAllowed(topic)) {
    console.log(
      `${isoNowIST()}\t[WsPublish:Reject]\tsubscriberId=${meta.subscriberId}\tclientId=${meta.clientId ?? "?"}\trequestId=${requestId}\ttopic=${topic}\treason=unauthorized-topic`,
    );
    return {
      ok: false,
      requestId,
      error: "Unauthorized topic",
      reason: "topic-not-allowed",
    };
  }

  console.log(
    `${isoNowIST()}\t[WsPublish:Request]\tsubscriberId=${meta.subscriberId}\tclientId=${meta.clientId ?? "?"}\trequestId=${requestId}\ttopic=${topic}\tkey=${key ?? ""}`,
  );

  try {
    const producer = await getProducer();
    const result = await producer.send({
      topic,
      messages: [
        {
          key: key ?? null,
          value: JSON.stringify(value),
        },
      ],
    });

    // KafkaJS returns RecordMetadata[] when available
    const meta0 =
      Array.isArray(result) && result.length > 0
        ? (result[0] as { partition?: number; offset?: string })
        : undefined;
    const partition =
      typeof meta0?.partition === "number" ? meta0.partition : 0;
    const offset =
      typeof meta0?.offset === "string"
        ? meta0.offset
        : String(meta0?.offset ?? "");

    console.log(
      `${isoNowIST()}\t[WsPublish:Success]\tsubscriberId=${meta.subscriberId}\trequestId=${requestId}\ttopic=${topic}\tpartition=${partition}\toffset=${offset}`,
    );

    return {
      ok: true,
      requestId,
      topic,
      partition,
      offset,
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.log(
      `${isoNowIST()}\t[WsPublish:Fail]\tsubscriberId=${meta.subscriberId}\trequestId=${requestId}\ttopic=${topic}\terr=${msg}`,
    );
    return {
      ok: false,
      requestId,
      error: "Kafka publish failed",
      reason: msg,
    };
  }
}
