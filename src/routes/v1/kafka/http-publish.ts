import { isoNowIST } from "@/lib/isoNowIST";
import { getProducer } from "@/lib/producer.kafka";
import { apiKeyAuth } from "@/middleware/auth";
import crypto from "crypto";
import { Elysia } from "elysia";
import z from "zod";

export default new Elysia()
  .use(apiKeyAuth)
  .guard({ apiKey: true })
  .post(
    "/http-publish",
    async ({ ctx, body, status }) => {

      const { xApiKey, apiUserId } = ctx
      const { topic, jobName } = body;

      // correlation id for tracing (Kafka later)
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

      console.log(`${isoNowIST()}\t [Publisher:Action]\t EVENT Published Topic: ${topic}`);

      return status(202, {
        success: true,
        statusCode: 202,
        message: "Event accepted",
        data: {
          topic,
          correlationId,
        },
      });
    },
    {
      body: z.object({
        topic: z.string().min(1),
        jobName: z.string().min(1),
      }),
      // headers: apiKeyAuthHeadersSchema,
      response: {
        202: z.object({
          success: z.literal(true),
          statusCode: z.literal(202),
          message: z.string(),
          data: z.object({
            topic: z.string(),
            correlationId: z.uuid(),
          }),
        })
      },
      detail: {
        tags: ["Publish"],
        summary: "HTTP publish event",
        description:
          "Publish an event via HTTP using API key authentication",
      },
    }
  );
