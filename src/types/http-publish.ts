import z from "zod";

const baseSchema = z.object({
  correlationId: z.uuid({ version: "v7" }).min(1, "Correlation ID is required"),
  topic: z.string().min(1, "Topic is required"),
  jobName: z.string().min(1, "Job name is required"),
  clientRequestId: z.string().min(1, "Client Request ID is required"),
  timestamp: z.string().min(1, "Timestamp is required"),
});

const ingestSchema = baseSchema.extend({
  eventType: z.literal("ingest"),
  records: z.array(z.string()).min(1, "At least one record is required"),
  username: z.string().nullable().optional(),
  role: z.string().nullable().optional(),
});

const statusSchema = baseSchema.extend({
  eventType: z.literal("status"),
  stage: z.string().min(1, "Stage is required"),
  status: z.enum(["STARTED", "SUCCEEDED", "FAILED"]),
  details: z
    .record(z.union([z.string(), z.number(), z.symbol()]), z.unknown())
    .optional(),
});

const committedSchema = baseSchema.extend({
  eventType: z.literal("committed"),
  records: z.array(z.string()).min(1).optional(),
  username: z.string().nullable().optional(),
  role: z.string().nullable().optional(),
  details: z.record(z.string(), z.unknown()).optional(),
});

export const requestBodySchema = z.discriminatedUnion("eventType", [
  ingestSchema,
  statusSchema,
  committedSchema,
]);