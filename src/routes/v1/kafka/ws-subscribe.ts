import { commitByCorrelationId, registerWs, unregisterWs } from "@/lib/kafka-ws-bridge";
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

export default new Elysia()
  .use(apiKeyAuth)
  .guard({ apiKey: true })
  .derive(() => ({
    subscriptions: new Set<string>(),
  }))
  .ws("/ws-subscribe", {

    open: (ws) => {
      registerWs(ws);
      ws.send({ type: "ready" });
    },

    message: async (ws, raw) => {
      // const { ctx } = ws.data

      let data: unknown;

      try {
        data = typeof raw === "string" ? JSON.parse(raw) : raw;

        const result = MessageSchema.safeParse(data);

        if (!result.success) {
          ws.send({
            type: "error",
            message: "Invalid message format",
            issues: result.error.issues,
          });
          return;
        }

        const parsed = result.data;

        if (parsed.type === "subscribe") {
          ws.data.subscriptions.add(parsed.topic);
          ws.send({ type: "subscribed", topic: parsed.topic });
          return;
        }

        if (parsed.type === "unsubscribe") {
          ws.data.subscriptions.delete(parsed.topic);
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
        ws.send({ type: "error", message: "Malformed JSON" });
        return;
      }
    },

    close: (ws) => {
      ws.data.subscriptions.clear();
      unregisterWs(ws);
    },
  });