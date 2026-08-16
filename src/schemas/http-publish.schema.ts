import { z } from "zod";

/**
 * Generic HTTP → Kafka publish request.
 * Gateway has no domain knowledge of the `value` payload.
 */
export const httpPublishRequestSchema = z.object({
  topic: z.string().min(1),
  correlationId: z.uuid({ version: "v7" }),
  value: z.unknown(),
});

export type HttpPublishRequest = z.infer<typeof httpPublishRequestSchema>;

/**
 * Explicit success response from POST /http-publish.
 */
export const httpPublishResponseSchema = z.object({
  success: z.literal(true),
  statusCode: z.literal(202),
  message: z.string(),
  data: z.object({
    topic: z.string(),
    correlationId: z.uuid({ version: "v7" }),
  }),
});

export type HttpPublishResponse = z.infer<typeof httpPublishResponseSchema>;
