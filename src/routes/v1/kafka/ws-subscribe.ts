import { isoNowIST } from "@/lib/isoNowIST";
import { commitByCorrelationId, registerWs, unregisterWs, wsClients } from "@/lib/kafka-ws-bridge";
import { apiKeyAuth } from "@/middleware/auth";
import { Elysia } from "elysia";
import z from "zod";

const SubscribeSchema = z.object({
  type: z.literal("subscribe"),
  topic: z.string().min(1),
});

const UnsubscribeSchema = z.object({
  type: z.literal("unsubscribe"),
  topic: z.string().min(1),
});

const ProcessedSchema = z.object({
  type: z.literal("processed"),
  correlationId: z.uuid(),
});

const MessageSchema = z.union([
  SubscribeSchema,
  UnsubscribeSchema,
  ProcessedSchema,
]);

type WsConn = {
  data: {
    subscriptions: Set<string>;
    subscriberId: number;
  };
} & Record<string, unknown>;

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
      const sub = ws as unknown as WsConn;

      registerWs(sub.data.subscriberId, ws);

      console.log(`${isoNowIST()}\t[WsSubscribe:Action]\tSUBSCRIBER CONNECTED ID: ${sub.data.subscriberId}`);
      console.log(`${isoNowIST()}\t[WsSubscribe:Log]\tACTIVE WS CONNECTIONS: ${wsClients.size}`);

      ws.send({ type: "ready" });
    },

    message: async (ws, raw) => {

      const sub = ws as unknown as WsConn;

      let data: unknown;

      try {
        data = typeof raw === "string" ? JSON.parse(raw) : raw;

        const result = MessageSchema.safeParse(data);

        if (!result.success) {

          console.log(`${isoNowIST()}\t[WsSubscribe:Error]\tINVALID MESSAGE FORMAT`);

          ws.send({
            type: "error",
            message: "Invalid message format",
            issues: result.error.issues,
          });

          return;
        }

        const parsed = result.data;

        if (parsed.type === "subscribe") {

          sub.data.subscriptions.add(parsed.topic);

          if (!topicSubscribers.has(parsed.topic)) {
            topicSubscribers.set(parsed.topic, new Set());
          }

          topicSubscribers.get(parsed.topic)!.add(sub.data.subscriberId);

          console.log(`${isoNowIST()}\t[WsSubscribe:Action]\tSUBSCRIBER ${sub.data.subscriberId} JOINED Topic: ${parsed.topic}`);
          console.log(`${isoNowIST()}\t[WsSubscribe:Log]\tSUBSCRIBER TOPICS:`, [...sub.data.subscriptions]);
          console.log(`${isoNowIST()}\t[WsSubscribe:Log]\tTOPIC SUBSCRIBERS (${parsed.topic}): ${topicSubscribers.get(parsed.topic)!.size}`);
          console.log(`${isoNowIST()}\t[WsSubscribe:Log]\tACTIVE WS CONNECTIONS: ${wsClients.size}`);

          ws.send({ type: "subscribed", topic: parsed.topic });
          return;
        }

        if (parsed.type === "unsubscribe") {

          sub.data.subscriptions.delete(parsed.topic);

          const set = topicSubscribers.get(parsed.topic);
          if (set) {
            set.delete(sub.data.subscriberId);
            if (set.size === 0) topicSubscribers.delete(parsed.topic);
          }

          console.log(`${isoNowIST()}\t[WsSubscribe:Action]\tSUBSCRIBER ${sub.data.subscriberId} UNSUBSCRIBED Topic: ${parsed.topic}`);
          console.log(`${isoNowIST()}\t[WsSubscribe:Log]\tSUBSCRIBER TOPICS:`, [...sub.data.subscriptions]);
          console.log(`${isoNowIST()}\t[WsSubscribe:Log]\tTOPIC SUBSCRIBERS (${parsed.topic}): ${set?.size ?? 0}`);
          console.log(`${isoNowIST()}\t[WsSubscribe:Log]\tACTIVE WS CONNECTIONS: ${wsClients.size}`);

          ws.send({ type: "unsubscribed", topic: parsed.topic });
          return;
        }

        if (parsed.type === "processed") {

          await commitByCorrelationId(parsed.correlationId);

          ws.send({
            type: "ack-received",
            correlationId: parsed.correlationId,
          });

          return;
        }

      } catch {

        console.log(`${isoNowIST()}\t[WsSubscribe:Error]\tMALFORMED JSON`);

        ws.send({ type: "error", message: "Malformed JSON" });
        return;
      }
    },

    close: (ws) => {

      const sub = ws as unknown as WsConn;

      for (const topic of sub.data.subscriptions) {
        const set = topicSubscribers.get(topic);
        if (set) {
          set.delete(sub.data.subscriberId);
          if (set.size === 0) topicSubscribers.delete(topic);
        }
      }

      unregisterWs(sub.data.subscriberId);

      console.log(`${isoNowIST()}\t[WsSubscribe:Action]\tSUBSCRIBER DISCONNECTED ID: ${sub.data.subscriberId}`);
      console.log(`${isoNowIST()}\t[WsSubscribe:Log]\tSUBSCRIBER TOPICS:`, [...sub.data.subscriptions]);
      console.log(`${isoNowIST()}\t[WsSubscribe:Log]\tACTIVE WS CONNECTIONS: ${wsClients.size}`);

      ws.data.subscriptions.clear();
    },
  });
