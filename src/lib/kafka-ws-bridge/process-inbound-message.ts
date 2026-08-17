/**
 * One Kafka message lifecycle: parse → route → deliver → ACK/commit (if required).
 */

import { isoNowIST } from "@/lib/isoNowIST";
import {
  shouldCommitAfterAck,
  waitForAckWithHeartbeat,
} from "@/lib/kafka-ws-bridge/ack-wait";
import { commitMessageOffset } from "@/lib/kafka-ws-bridge/commit-offset";
import { deliverEventToSubscribers } from "@/lib/kafka-ws-bridge/deliver-to-subscribers";
import { parseInboundMessage } from "@/lib/kafka-ws-bridge/parse-inbound-message";
import {
  markPendingDelivered,
  registerPendingAck,
} from "@/lib/kafka-ws-bridge/pending-acks";
import { isAckRequiredTopic } from "@/lib/kafka-ws-bridge/record-log-topics";
import { pendingAcks } from "@/lib/kafka-ws-bridge/state";
import {
  pickResponsibleSubscriber,
  waitForResponsibleSubscriber,
} from "@/lib/kafka-ws-bridge/subscriber-selection";
import { ProcessInboundMessageArgs } from "@/types/global.type";

/**
 * @returns `"stop-batch"` when the batch loop should break (ACK-required, no subscriber).
 * @returns `"done"` after handling this message (caller may process more messages).
 */
export async function processInboundMessage(
  args: ProcessInboundMessageArgs,
): Promise<"done" | "stop-batch"> {
  const {
    consumer,
    topic,
    partition,
    message,
    resolveOffset,
    heartbeat,
    isRunning,
    isStale,
  } = args;

  const value = message.value?.toString();
  const parsed = parseInboundMessage(value);

  if (!parsed.ok) {
    if (parsed.reason === "empty") {
      await commitMessageOffset({
        consumer,
        topic,
        partition,
        offset: message.offset,
        resolveOffset,
        heartbeat,
      });
      return "done";
    }

    if (parsed.reason === "malformed-json") {
      console.log(
        `${isoNowIST()}\t[WsBridge:Error]\tMALFORMED JSON topic=${topic} partition=${partition} offset=${message.offset}`,
      );
      await commitMessageOffset({
        consumer,
        topic,
        partition,
        offset: message.offset,
        resolveOffset,
        heartbeat,
      });
      return "done";
    }

    console.log(
      `${isoNowIST()}\t[WsBridge:Error]\tINVALID MESSAGE topic=${topic} partition=${partition} offset=${message.offset} issues=${JSON.stringify(parsed.issues)}`,
    );
    await commitMessageOffset({
      consumer,
      topic,
      partition,
      offset: message.offset,
      resolveOffset,
      heartbeat,
    });
    return "done";
  }

  const { correlationId, eventPayload } = parsed;
  const requiresAck = isAckRequiredTopic(topic);

  let owner = pickResponsibleSubscriber(topic);

  if (!owner && requiresAck) {
    console.log(
      `${isoNowIST()}\t[WsBridge:Log]\tWAITING_FOR_SUBSCRIBER topic=${topic} corr=${correlationId} partition=${partition} offset=${message.offset}`,
    );

    owner = await waitForResponsibleSubscriber(topic, {
      heartbeat,
      isRunning,
      isStale,
    });
  }

  if (!owner) {
    if (requiresAck) {
      console.log(
        `${isoNowIST()}\t[WsBridge:Log]\tNO_SUBSCRIBER_STOP topic=${topic} corr=${correlationId} partition=${partition} offset=${message.offset} reason=stopped-or-stale`,
      );
      return "stop-batch";
    }

    console.log(
      `${isoNowIST()}\t[WsBridge:Log]\tDROP topic=${topic} corr=${correlationId} reason=no-subscribers`,
    );
    await commitMessageOffset({
      consumer,
      topic,
      partition,
      offset: message.offset,
      resolveOffset,
      heartbeat,
    });
    return "done";
  }

  // Non-ACK topics: fan-out and commit immediately
  if (!requiresAck) {
    const delivered = deliverEventToSubscribers({
      topic,
      partition,
      offset: message.offset,
      headers: message.headers,
      eventPayload,
    });

    await commitMessageOffset({
      consumer,
      topic,
      partition,
      offset: message.offset,
      resolveOffset,
      heartbeat,
    });

    console.log(
      `${isoNowIST()}\t[WsBridge:Log]\tKafka → Gateway → Subscribers | topic=${topic} corr=${correlationId} delivered=${delivered} autoCommitted=true`,
    );

    return "done";
  }

  // ACK-required: register pending ACK, deliver, wait, commit gate, commit
  const ackPromise = registerPendingAck({
    topic,
    partition,
    offset: message.offset,
    correlationId,
    subscriberId: owner.id,
    subscriber: owner.ws.data.ctx.xSubscriberId,
  });

  deliverEventToSubscribers({
    topic,
    partition,
    offset: message.offset,
    headers: message.headers,
    eventPayload,
    ownerId: owner.id,
    onOwnerDelivered: (id) => {
      markPendingDelivered(correlationId);
      console.log(
        `${isoNowIST()}\t[WsBridge:Deliver]\ttopic=${topic}\tpartition=${partition}\toffset=${message.offset}\tcorr=${correlationId}\tsubscriberId=${id}\tSubscriber=${owner.ws.data.ctx.xSubscriberId ?? "?"}`,
      );
    },
  });

  const timeoutMs = 5 * 60 * 1000;

  try {
    await waitForAckWithHeartbeat(ackPromise, heartbeat, timeoutMs);
  } catch (err) {
    const still = pendingAcks.get(correlationId);
    if (still) {
      pendingAcks.delete(correlationId);
    }

    console.log(
      `${isoNowIST()}\t[WsBridge:Log]\tACK_WAIT_FAILED topic=${topic} partition=${partition} offset=${message.offset} corr=${correlationId} err=${err instanceof Error ? err.message : String(err)}`,
    );

    throw err;
  }

  const commitGate = shouldCommitAfterAck({ isRunning, isStale });

  if (!commitGate.ok) {
    console.log(
      `${isoNowIST()}\t[WsBridge:Log]\tSKIP_COMMIT_STALE topic=${topic} partition=${partition} offset=${message.offset} corr=${correlationId} reason=${commitGate.reason}`,
    );

    throw new Error(
      `ACK arrived but batch is not safe to commit: ${commitGate.reason} corr=${correlationId}`,
    );
  }

  const nextOffset = (BigInt(message.offset) + 1n).toString();

  resolveOffset(message.offset);

  await consumer.commitOffsets([
    {
      topic,
      partition,
      offset: nextOffset,
    },
  ]);

  console.log(
    `${isoNowIST()}\t[WsBridge:Commit]\ttopic=${topic}\tpartition=${partition}\tcommittedOffset=${nextOffset}\tcorr=${correlationId}\tsubscriberId=${owner.ws.data.ctx.subscriberId}\tSubscriber=${owner.ws.data.ctx.xSubscriberId ?? "?"}`,
  );

  await heartbeat();

  return "done";
}
