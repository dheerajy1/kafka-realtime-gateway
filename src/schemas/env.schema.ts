import { z } from "zod";

export const envSchema = z.object({
  JWT_SECRET: z
    .string()
    .trim()
    .min(1, { message: "JWT_SECRET is required and cannot be empty" })
    .max(50, {
      message: "JWT_SECRET must be under 50 characters long",
    })
    .describe("JWT Secret used for authentication"),

  // ============================
  // Client
  // ============================
  CLIENT_ID: z
    .string()
    .trim()
    .min(1, { message: "CLIENT_ID is required and cannot be empty" })
    .max(50, {
      message: "CLIENT_ID must be under 50 character long",
    })
    .describe("Public client identifier"),

  CLIENT_SECRET: z
    .string()
    .trim()
    .min(1, { message: "CLIENT_SECRET is required and cannot be empty" })
    .max(50, {
      message: "CLIENT_SECRET must be under 50 characters long",
    })
    .describe("Client secret used for authentication"),

  // ============================
  // Database
  // ============================

  // On-prem
  ONPREM_DB_SERVER: z
    .string()
    .trim()
    .min(1, {
      message: "ONPREM_DB_SERVER is required and cannot be empty",
    })
    .max(50, {
      message: "ONPREM_DB_SERVER must be under 50 characters long",
    })
    .describe("Server name for On-prem sql server"),

  ONPREM_DB_PORT: z.coerce
    .number()
    .int()
    .positive()
    .default(5432)
    .describe("Port for On-prem PostgreSQL"),

  ONPREM_DB_DATABASE: z
    .string()
    .trim()
    .min(1, {
      message: "ONPREM_DB_DATABASE is required and cannot be empty",
    })
    .max(50, {
      message: "ONPREM_DB_DATABASE must be under 50 characters long",
    })
    .describe("Database name for On-prem db sql login account"),

  ONPREM_DB_USER_PUBLIC: z
    .string()
    .trim()
    .min(1, {
      message: "ONPREM_DB_USER_PUBLIC is required and cannot be empty",
    })
    .max(50, {
      message: "ONPREM_DB_USER_PUBLIC must be under 50 characters long",
    })
    .describe("Username for On-prem db sql login public account"),

  ONPREM_DB_PASSWORD_PUBLIC: z
    .string()
    .trim()
    .min(1, {
      message: "ONPREM_DB_PASSWORD_PUBLIC is required and cannot be empty",
    })
    .max(50, {
      message: "ONPREM_DB_PASSWORD_PUBLIC must be under 50 characters long",
    })
    .describe("Password for On-prem db sql login public account"),

  ONPREM_DB_USER_SERVICE: z
    .string()
    .trim()
    .min(1, {
      message: "ONPREM_DB_USER_SERVICE is required and cannot be empty",
    })
    .max(50, {
      message: "ONPREM_DB_USER_SERVICE must be under 50 characters long",
    })
    .describe("Username for On-prem db sql login service account"),

  ONPREM_DB_PASSWORD_SERVICE: z
    .string()
    .trim()
    .min(1, {
      message: "ONPREM_DB_PASSWORD_SERVICE is required and cannot be empty",
    })
    .max(50, {
      message: "ONPREM_DB_PASSWORD_SERVICE must be under 50 characters long",
    })
    .describe("Password for On-prem db sql login service account"),

  APP_NAME: z
    .string()
    .trim()
    .min(1, { message: "APP_ENV is required and cannot be empty" })
    .max(50, {
      message: "APP_ENV must be under 50 characters long",
    })
    .describe("App environment"),
  // ============================
  // KAFKA
  // ============================
  KAFKA_BROKERS: z
    .string()
    .trim()
    .min(1, { message: "KAFKA_BROKERS is required and cannot be empty" })
    .describe("Comma-separated list of Kafka broker addresses"),

  KAFKA_CLIENT_ID: z
    .string()
    .trim()
    .min(1, { message: "KAFKA_CLIENT_ID is required and cannot be empty" })
    .max(100, {
      message: "KAFKA_CLIENT_ID must be under 100 characters long",
    })
    .describe(
      "Kafka client id for kafka-api-gateway (used to identify the producer/consumer application)",
    ),

  KAFKA_GROUP_ID: z
    .string()
    .trim()
    .min(1, { message: "KAFKA_GROUP_ID is required and cannot be empty" })
    .max(100, {
      message: "KAFKA_GROUP_ID must be under 100 characters long",
    })
    .describe(
      "Kafka consumer group id for kafka-api-gateway (unique per environment)",
    ),
});
