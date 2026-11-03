/**
 * Subscriber → gateway Kafka publish (over the existing authenticated subscribe WS).
 * Gateway is the sole Kafka producer for these pipeline topics.
 */
import { isoNowIST } from "@/lib/isoNowIST";
import { isSubscriberPublishAllowed } from "@/lib/kafka-ws-bridge/topic-policy";
import { getProducer } from "@/lib/producer.kafka";
import {
  PublishFailure,
  PublishSuccess,
  WsPublishCommand,
} from "@/schemas/ws-subscribe.schema";

export { isSubscriberPublishAllowed } from "@/lib/kafka-ws-bridge/topic-policy";

/**
 * Validate and publish via the gateway Kafka producer.
 * Does not ACK inbound Kafka records — that remains subscriber-owned.
 */
export async function handleSubscriberPublish(
  cmd: WsPublishCommand,
  meta: { subscriberId: number; subscriber: string },
): Promise<PublishSuccess | PublishFailure> {
  const { requestId, topic, key, value, headers } = cmd;

  if (!isSubscriberPublishAllowed(topic)) {
    console.log(
      `${isoNowIST()}\t[WsPublish:Reject]\tsubscriberId=${meta.subscriberId}\tSubscriber=${meta.subscriber ?? "?"}\trequestId=${requestId}\ttopic=${topic}\treason=unauthorized-topic`,
    );
    return {
      ok: false,
      requestId,
      error: "Unauthorized topic",
      reason: "topic-not-allowed",
    };
  }

  console.log(
    `${isoNowIST()}\t[WsPublish:Request]\tsubscriberId=${meta.subscriberId}\tSubscriber=${meta.subscriber ?? "?"}\trequestId=${requestId}\ttopic=${topic}\tkey=${key ?? ""}`,
  );

  try {
    const producer = await getProducer();
    const kafkaHeaders =
      headers && Object.keys(headers).length > 0
        ? Object.fromEntries(
            Object.entries(headers).map(([k, v]) => [k, Buffer.from(v)]),
          )
        : undefined;

    const result = await producer.send({
      topic,
      messages: [
        {
          key: key ?? null,
          value: JSON.stringify(value),
          headers: kafkaHeaders,
        },
      ],
    });

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
      `${isoNowIST()}\t[WsPublish:Success]\tsubscriberId=${meta.subscriberId}\tSubscriber=${meta.subscriber ?? "?"}\trequestId=${requestId}\ttopic=${topic}\tpartition=${partition}\toffset=${offset}`,
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
      `${isoNowIST()}\t[WsPublish:Fail]\tsubscriberId=${meta.subscriberId}\tSubscriber=${meta.subscriber ?? "?"}\trequestId=${requestId}\ttopic=${topic}\terr=${msg}`,
    );
    return {
      ok: false,
      requestId,
      error: "Kafka publish failed",
      reason: msg,
    };
  }
}