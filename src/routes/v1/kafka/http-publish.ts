import { getProducer } from "@/lib/producer.kafka";
import { isUuidV7 } from "@/pipeline-state-consumer/lib/events";
import { MyError, errors } from "@/lib/errors";
import { apiKeyAuth } from "@/middleware/auth";
import { Elysia } from "elysia";
import z from "zod";
import { requestBodySchema } from "@/types/http-publish";

/**
 * HTTP publish is event-only.
 * Caller generates UUIDv7.
 * Gateway validates UUIDv7.
 * Gateway forwards it unchanged.
 *
 * Request body:
 *   { topic, correlationId, event, eventType, jobName? }
 *   → key = correlationId, value = exact event JSON, correlationId never regenerated
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

      const { correlationId, topic } = body;

      if (!isUuidV7(correlationId)) {
        throw new MyError({
          code: "BAD_REQUEST",
          message: errors.BAD_REQUEST.INVALID_INPUT.message,
          error: "correlationId must be a valid UUIDv7",
        });
      }

      const producer = await getProducer();

      await producer.send({
        topic,
        messages: [
          {
            key: correlationId,
            value: JSON.stringify(body),
          },
        ],
      });

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
        summary: "HTTP publish event",
        description:
          "Publish via HTTP using API key auth. Event-only mode. Validates and forwards event unchanged.",
      },
    },
  );
