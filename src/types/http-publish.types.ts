import z from "zod";

export const requestBodySchema = z.object({
  eventType: z.literal("ingest"),
  topic: z.literal("record-log-ingest"),
  correlationId: z.uuid({ version: "v7" }),
  jobName: z.string().min(1, "Job name is required"),
  clientRequestId: z.string().min(1, "Client Request ID is required"),
  timestamp: z.string().min(1, "Timestamp is required"),
  records: z.array(z.string()).min(1, "At least one record is required"),
  username: z.string().nullable().optional(),
  role: z.string().nullable().optional(),
});