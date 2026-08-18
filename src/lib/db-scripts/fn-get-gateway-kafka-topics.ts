/**
 * Loads Gateway topic policy via PostgreSQL function fn_get_gateway_kafka_topics().
 * Application code must not SELECT from kafka_topic directly.
 */

import { getOnPremPool } from "@/lib/db";
import { MyError, errors } from "@/lib/errors";
import { getFilePath } from "@/lib/get-file-path";
import {
  buildTopicPolicy,
  type TopicPolicy,
  type TopicPolicyRow,
} from "@/lib/kafka-ws-bridge/topic-policy";

type PgError = {
  code?: string;
  message?: string;
};

type DbRow = {
  topic_name: string;
  gateway_consume: boolean;
  ack_required: boolean;
  subscriber_publish_allowed: boolean;
  enabled: boolean;
  description: string | null;
};

export async function loadGatewayKafkaTopics(): Promise<TopicPolicy> {
  const pool = await getOnPremPool({ poolKey: "service" });

  try {
    const { rows } = await pool.query<DbRow>(
      `
            SELECT
                topic_name,
                gateway_consume,
                ack_required,
                subscriber_publish_allowed,
                enabled,
                description
            FROM fn_get_gateway_kafka_topics()
            `,
    );

    const policyRows: TopicPolicyRow[] = rows.map((r) => ({
      topic_name: r.topic_name,
      gateway_consume: r.gateway_consume,
      ack_required: r.ack_required,
      subscriber_publish_allowed: r.subscriber_publish_allowed,
      enabled: r.enabled,
      description: r.description,
    }));

    return buildTopicPolicy(policyRows);
  } catch (error: unknown) {
    if (error instanceof MyError) {
      throw error;
    }

    console.log(`${getFilePath()} - error`, error);

    const err = error as PgError;
    const code = err?.code;

    if (code === "42501") {
      throw new MyError({
        code: "INTERNAL_SERVER_ERROR",
        message: errors.INTERNAL_SERVER_ERROR.DB_PERMISSION_DENIED.message,
        error: errors.INTERNAL_SERVER_ERROR.DB_PERMISSION_DENIED.error,
      });
    }

    if (code === "28P01" || code === "3D000" || code === "ECONNREFUSED") {
      throw new MyError({
        code: "INTERNAL_SERVER_ERROR",
        message: errors.INTERNAL_SERVER_ERROR.DB_CONNECTION_ERROR.message,
        error: errors.INTERNAL_SERVER_ERROR.DB_CONNECTION_ERROR.error,
      });
    }

    throw new MyError({
      code: "INTERNAL_SERVER_ERROR",
      message: errors.INTERNAL_SERVER_ERROR.DATABASE_ERROR.message,
      error: errors.INTERNAL_SERVER_ERROR.DATABASE_ERROR.error,
    });
  }
}
