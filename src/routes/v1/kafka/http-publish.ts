import { MyError, errors } from "@/lib/errors";
import { getProducer } from "@/lib/producer.kafka";
import { apiKeyAuth } from "@/middleware/auth";
import { requestBodySchema } from "@/types/http-publish.types";
import { Elysia } from "elysia";
import z from "zod";
/*
ONE HTTP request
       ↓
Kafka Gateway
       ↓
RECEIVED
       ↓
record-log-ingest
       ↓
KAFKA_INGESTED
*/
export default new Elysia()
  .use(apiKeyAuth)
  .guard({ apiKey: true })
  .post(
    "/http-publish",
    async ({ ctx, body, status }) => {
      if (ctx?.authError?.code != null) {
        throw new MyError({
          code: (ctx.authError.code as keyof typeof errors) || "UNAUTHORIZED",
          message:
            ctx.authError.message ||
            errors.UNAUTHORIZED.INVALID_API_KEY.message,
          error:
            ctx.authError.error || errors.UNAUTHORIZED.INVALID_API_KEY.error,
        });
      }

      const producer = await getProducer();

      await producer.send({
        topic: "record-log-status",
        messages: [
          {
            key: body.correlationId,
            value: JSON.stringify({
              correlationId: body.correlationId,
              stage: "RECEIVED",
              status: "SUCCEEDED",
              timestamp: body.timestamp,
              details: {
                message: "Request accepted by Kafka Gateway",
              },
            }),
          },
        ],
      });

      await producer.send({
        topic: body.topic,
        messages: [
          {
            key: body.correlationId,
            value: JSON.stringify(body),
          },
        ],
      });

      await producer.send({
        topic: "record-log-status",
        messages: [
          {
            key: body.correlationId,
            value: JSON.stringify({
              correlationId: body.correlationId,
              stage: "KAFKA_INGESTED",
              status: "SUCCEEDED",
              timestamp: body.timestamp,
              details: {
                message: "Ingest event accepted by Kafka",
              },
            }),
          },
        ],
      });

      return status(202, {
        success: true,
        statusCode: 202,
        message: "Event accepted",
        data: {
          topic: body.topic,
          correlationId: body.correlationId,
        },
      });
    },
    {
      body: requestBodySchema,
      response: {
        202: z.object({
          success: z.literal(true),
          statusCode: z.literal(202),
          message: z.string(),
          data: z.object({
            topic: z.string(),
            correlationId: z.string(),
          }),
        }),
      },
      detail: {
        tags: ["Publish"],
        summary: "Publish record log ingest event",
        description:
          "Accepts one record-log ingest request and publishes the ingest event plus its Kafka observability status events.",
      },
    },
  );
