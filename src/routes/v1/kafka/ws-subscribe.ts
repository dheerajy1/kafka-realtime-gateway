import { isoNowIST } from "@/lib/isoNowIST";
import {
  commitByCorrelationId,
  countPendingForSubscriber,
  registerWs,
  unregisterWs,
  wsClients,
} from "@/lib/kafka-ws-bridge";
import { handleSubscriberPublish } from "@/lib/ws-subscriber-publish";
import { apiKeyAuth } from "@/middleware/auth";
import { GatewayWS } from "@/schemas/kafka-ws-bridge.schema";
import { MessageSchema } from "@/schemas/ws-subscribe.schema";
import { Elysia } from "elysia";

const topicSubscribers = new Map<string, Set<number>>();

let subscriberCounter = 0;

export default new Elysia()
  .use(apiKeyAuth)
  .guard({ apiKey: true })
  .derive(() => ({
    subscriptions: new Set<string>(),
    subscriberId: ++subscriberCounter,
  }))
  .ws("/ws-subscribe", {
    open: (ws) => {
      const { ctx, subscriberId } = ws.data;

      if (ctx.authError.code != null) {
        ws.send({ type: "error", ...ctx.authError });
        ws.close(4001, JSON.stringify(ctx.authError));
        return;
      }

      registerWs(subscriberId, ws as unknown as GatewayWS);

      console.log(
        `${isoNowIST()}\t[WsSubscribe:Action]\tSUBSCRIBER CONNECTED ID: ${subscriberId}`,
      );
      console.log(
        `${isoNowIST()}\t[WsSubscribe:Log]\tACTIVE WS CONNECTIONS: ${wsClients.size}`,
      );

      ws.send({ type: "ready" });
    },

    message: async (ws, raw) => {
      const { subscriberId, subscriptions } = ws.data;

      let data: unknown;

      try {
        data = typeof raw === "string" ? JSON.parse(raw) : raw;

        const result = MessageSchema.safeParse(data);

        if (!result.success) {
          console.log(
            `${isoNowIST()}\t[WsSubscribe:Error]\tINVALID MESSAGE FORMAT subscriberId=${subscriberId}`,
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
            `${isoNowIST()}\t[WsSubscribe:Action]\tSUBSCRIBER ${subscriberId} JOINED Topic: ${parsed.topic}`,
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
          const clientId = (ws as unknown as GatewayWS).data.clientId;
          const pub = await handleSubscriberPublish(parsed, {
            subscriberId,
            clientId,
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
          const ackResult = await commitByCorrelationId(
            parsed.correlationId,
            subscriberId,
          );

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
          `${isoNowIST()}\t[WsSubscribe:Error]\tMALFORMED JSON subscriberId=${subscriberId}`,
        );

        ws.send({ type: "error", message: "Malformed JSON" });
        return;
      }
    },

    close: (ws) => {
      const { subscriberId, subscriptions } = ws.data;

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
        subscriberCounter = 0;
      }

      console.log(
        `${isoNowIST()}\t[WsBridge:Disconnect]\tsubscriberId=${subscriberId}\tpendingAcks=${pending}`,
      );

      ws.data.subscriptions.clear();
    },
  });
