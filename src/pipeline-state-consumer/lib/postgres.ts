import { getOnPremPool, closeAllPools } from "@/lib/db";
import type { Stage, StatusValue } from "@/pipeline-state-consumer/lib/events";

export type ApplyStatusResult = {
  result: string;
  replayed: boolean;
  assignedSequence: number;
  currentStage: string;
  currentStatus: string;
};

export type PostgresDb = {
  applyStatus: (args: {
    correlationId: string;
    stage: Stage;
    status: StatusValue;
    timestamp: string;
    details?: Record<string, unknown>;
  }) => Promise<ApplyStatusResult>;
  close: () => Promise<void>;
};

export function createPostgresDb(): PostgresDb {
  return {
    async applyStatus({ correlationId, stage, status, timestamp, details }) {
      const pool = await getOnPremPool({ poolKey: "pipelineStateConsumerDb" });
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const res = await client.query(
          `SELECT result, replayed, assigned_sequence, current_stage, current_status
             FROM fn_pipeline_apply_status(
               $1::uuid,
               $2::varchar,
               $3::varchar,
               $4::timestamptz,
               $5::jsonb
             )`,
          [
            correlationId,
            stage,
            status,
            timestamp,
            details ? JSON.stringify(details) : null,
          ],
        );
        await client.query("COMMIT");

        const row = res.rows[0] as
          | {
              result: string;
              replayed: boolean;
              assigned_sequence: number;
              current_stage: string;
              current_status: string;
            }
          | undefined;

        if (!row || row.result !== "SUCCESS") {
          throw new Error("fn_pipeline_apply_status did not return SUCCESS");
        }

        return {
          result: row.result,
          replayed: Boolean(row.replayed),
          assignedSequence: Number(row.assigned_sequence),
          currentStage: row.current_stage,
          currentStatus: row.current_status,
        };
      } catch (err) {
        try {
          await client.query("ROLLBACK");
        } catch {
          /* ignore */
        }
        throw err;
      } finally {
        client.release();
      }
    },

    async close() {
      await closeAllPools();
    },
  };
}
