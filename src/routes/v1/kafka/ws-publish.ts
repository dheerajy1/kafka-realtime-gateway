import { getProducer } from "@/lib/producer.kafka";
import { apiKeyAuth } from "@/middleware/auth";
import crypto from "crypto";
import { Elysia } from "elysia";
import z from "zod";

const WsPublishSchema = z.object({
    // type: z.literal("publish"),
    topic: z.string().min(1),
    jobName: z.string().min(1),
});

export default new Elysia()
    .use(apiKeyAuth)
    .guard({ apiKey: true })
    .ws("/ws-publish", {
        open: (ws) => {
            ws.send(
                JSON.stringify({
                    type: "ready",
                    message: "WebSocket publish connection established",
                })
            );
        },

        message: async (ws, raw) => {
            try {
                const { ctx } = ws.data
                const { xApiKey, apiUserId } = ctx

                const data =
                    typeof raw === "string" ? JSON.parse(raw) : raw;

                const parsed = WsPublishSchema.safeParse(data);

                if (!parsed.success) {
                    ws.send(
                        JSON.stringify({
                            type: "error",
                            message: "Invalid publish message",
                        })
                    );
                    return;
                }

                const { topic, jobName } = parsed.data;

                const correlationId = crypto.randomUUID();
                const producer = await getProducer();

                await producer.send({
                    topic,
                    messages: [
                        {
                            key: xApiKey,
                            value: JSON.stringify({
                                correlationId,
                                apiUserId: apiUserId,
                                jobName,
                                timestamp: new Date().toISOString(),
                            }),
                        },
                    ],
                });

                ws.send(
                    JSON.stringify({
                        type: "ack",
                        topic,
                        correlationId,
                    })
                );
            } catch {
                ws.send(
                    JSON.stringify({
                        type: "error",
                        message: "Malformed JSON",
                    })
                );
            }
        },
        // NO .derive for subscriptions
        // NO subscriptions in ws.data
        close: () => {
            // No subscriptions here → nothing to clean
        },
    });

// How client uses it

/*
const ws = new WebSocket("ws://host/api/v1/ws-publish", {
  headers: {
    "X-API-KEY": "...",
    "X-API-SECRET": "..."
  }
});

ws.onopen = () => {
  ws.send(JSON.stringify({
    type: "publish",
    topic: "user.created",
    payload: { id: 123 }
  }));
};

*/