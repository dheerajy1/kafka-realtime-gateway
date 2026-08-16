import { MyError, errors } from "@/lib/errors";
import { getProducer } from "@/lib/producer.kafka";
import { apiKeyAuth } from "@/middleware/auth";
import {
  httpPublishRequestSchema,
  httpPublishResponseSchema,
} from "@/schemas/http-publish.schema";
import { Elysia } from "elysia";

/**
 * Generic Kafka HTTP publish endpoint.
 *
 * Authenticates the request, validates the transport envelope,
 * publishes `value` to `topic` with Kafka key = correlationId,
 * and returns a defined 202 response.
 *
 * No domain-specific status events or topic hardcoding.
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
        topic: body.topic,
        messages: [
          {
            key: body.correlationId,
            value: JSON.stringify(body.value),
          },
        ],
      });

      return status(202, {
        success: true as const,
        statusCode: 202 as const,
        message: "Event accepted",
        data: {
          topic: body.topic,
          correlationId: body.correlationId,
        },
      });
    },
    {
      body: httpPublishRequestSchema,
      response: {
        202: httpPublishResponseSchema,
      },
      detail: {
        tags: ["Publish"],
        summary: "Publish a message to a Kafka topic",
        description:
          "Accepts a generic transport envelope (topic, correlationId, value). " +
          "Publishes the value to the supplied topic using correlationId as the Kafka message key. " +
          "Returns 202 after Kafka acceptance. No domain-specific processing.",
      },
    },
  );
