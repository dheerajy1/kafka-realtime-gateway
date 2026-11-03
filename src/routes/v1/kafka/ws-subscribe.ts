import { isoNowIST } from "@/lib/isoNowIST";
import {
  commitByCorrelationId,
  countPendingForSubscriber,
} from "@/lib/kafka-ws-bridge/pending-acks";
import { wsClients } from "@/lib/kafka-ws-bridge/state";
import { registerWs, unregisterWs } from "@/lib/kafka-ws-bridge/ws-registry";
import { handleSubscriberPublish } from "@/lib/ws-subscriber/ws-subscriber-publish";
import { wsSubscriberState } from "@/lib/ws-subscriber/ws-subscriber-state";
import { apiKeyAuth, wsSubscriberAuth } from "@/middleware/auth";
import { MessageSchema } from "@/schemas/ws-subscribe.schema";
import { Elysia } from "elysia";

export default new Elysia()
  .use(apiKeyAuth)
  .use(wsSubscriberAuth)
  .guard({ apiKey: true, wsSubscriber: true })
  .ws("/ws-subscribe", {
    open: (ws) => {
      try {
        const { ctx } = ws.data;
        const { xSubscriberId, subscriberId } = ctx;

        if (ctx.authError?.code != null) {
          ws.send({ type: "error", ...ctx.authError });
          ws.close(4001, JSON.stringify(ctx.authError));
          return;
        }

        registerWs(subscriberId, ws);

        console.log(
          `${isoNowIST()}\t[WsSubscribe:Action]\tSubscriber : ${xSubscriberId} CONNECTED ID: ${subscriberId}`,
        );
        console.log(
          `${isoNowIST()}\t[WsSubscribe:Log]\tACTIVE WS CONNECTIONS: ${wsClients.size}`,
        );

        ws.send({ type: "ready" });
      } catch (err) {
        console.error(
          `${isoNowIST()}\t[WsSubscribe:Error]\topen failed`,
          err instanceof Error ? err.message : err,
        );
        try {
          ws.close(1011, "subscriber open failed");
        } catch {
          /* ignore close errors */
        }
      }
    },

    message: async (ws, raw) => {
      const { xSubscriberId, subscriberId, subscriptions } = ws.data.ctx;

      const { topicSubscribers } = wsSubscriberState;

      try {
        let data: unknown;

        data = typeof raw === "string" ? JSON.parse(raw) : raw;

        const result = MessageSchema.safeParse(data);

        if (!result.success) {
          console.log(
            `${isoNowIST()}\t[WsSubscribe:Error]\tINVALID MESSAGE FORMAT Subscriber : ${xSubscriberId} subscriberId=${subscriberId}`,
          );

          ws.send({
            type: "error",
            message: "Invalid message format",
            issues: result.error.issues,
          });

          return;
        }

        const parsed = result.data;

        if (parsed.type === "subscribe") {
          subscriptions.add(parsed.topic);

          if (!topicSubscribers.has(parsed.topic)) {
            topicSubscribers.set(parsed.topic, new Set());
          }

          topicSubscribers.get(parsed.topic)!.add(subscriberId);

          console.log(
            `${isoNowIST()}\t[WsSubscribe:Action]\tSUBSCRIBER ${xSubscriberId} ${subscriberId} JOINED Topic: ${parsed.topic}`,
          );

          ws.send({ type: "subscribed", topic: parsed.topic });
          return;
        }

        if (parsed.type === "unsubscribe") {
          subscriptions.delete(parsed.topic);

          const set = topicSubscribers.get(parsed.topic);
          if (set) {
            set.delete(subscriberId);
            if (set.size === 0) topicSubscribers.delete(parsed.topic);
          }

          ws.send({ type: "unsubscribed", topic: parsed.topic });
          return;
        }

        if (parsed.type === "publish") {
          const pub = await handleSubscriberPublish(parsed, {
            subscriberId,
            subscriber: xSubscriberId,
          });

          if (pub.ok) {
            ws.send({
              type: "published",
              requestId: pub.requestId,
              topic: pub.topic,
              partition: pub.partition,
              offset: pub.offset,
            });
          } else {
            ws.send({
              type: "error",
              message: pub.error,
              reason: pub.reason,
              requestId: pub.requestId,
            });
          }
          return;
        }

        if (parsed.type === "processed") {
          const ackResult = await commitByCorrelationId({
            correlationId: parsed.correlationId,
            fromSubscriberId: subscriberId,
            subscriber: xSubscriberId,
          });

          if (ackResult.ok) {
            ws.send({
              type: "ack-received",
              correlationId: parsed.correlationId,
            });
          } else {
            ws.send({
              type: "error",
              message: "ACK rejected",
              reason: ackResult.reason,
              correlationId: parsed.correlationId,
            });
          }

          return;
        }
      } catch {
        console.log(
          `${isoNowIST()}\t[WsSubscribe:Error]\tMALFORMED JSON Subscriber : ${xSubscriberId} subscriberId=${subscriberId}`,
        );

        ws.send({ type: "error", message: "Malformed JSON" });
        return;
      }
    },

    close: (ws) => {
      const { xSubscriberId, subscriberId, subscriptions } = ws.data.ctx;

      const { topicSubscribers } = wsSubscriberState;

      for (const topic of subscriptions) {
        const set = topicSubscribers.get(topic);
        if (set) {
          set.delete(subscriberId);
          if (set.size === 0) topicSubscribers.delete(topic);
        }
      }

      const pending = countPendingForSubscriber(subscriberId);
      unregisterWs(subscriberId);

      if (wsClients.size === 0) {
        wsSubscriberState.subscriberCounter = 0;
      }

      console.log(
        `${isoNowIST()}\t[WsBridge:Disconnect]\tSUBSCRIBER : ${xSubscriberId}\tsubscriberId=${subscriberId}\tpendingAcks=${pending}`,
      );

      subscriptions.clear();
    },
  });
