import { getOnPremPool } from "@/lib/db";
import { getFilePath } from "@/lib/get-file-path";
import { MyError, errors } from "@/lib/errors";

type Notes = {
  apiKeyId: string;
  apiUserId: number;
  apiKeySecretHash: string;
};

type DbRow = {
  result: "SUCCESS";
  notes: Notes;
};

type PgError = {
  code?: string;
  message?: string;
};

export async function fnVerifyApiKey({ xApiKey }: { xApiKey: string }) {
  const pool = await getOnPremPool({ poolKey: "service" });

  try {
    const { rows } = await pool.query<DbRow>(
      `
            SELECT *
            FROM fn_verify_api_key($1)
            `,
      [xApiKey],
    );

    const row = rows[0];

    if (!row || row.result !== "SUCCESS") {
      throw new MyError({
        code: "UNAUTHORIZED",
        message: errors.UNAUTHORIZED.INVALID_API_KEY.message,
        error: errors.UNAUTHORIZED.INVALID_API_KEY.error,
      });
    }

    return {
      success: true as const,
      data: row.notes,
    };
  } catch (error: unknown) {
    if (error instanceof MyError) {
      throw error;
    }

    console.log(`${getFilePath()} - error`, error);

    const err = error as PgError;
    const code = err?.code;
    const message = err?.message ?? "Unknown database error";

    if (code === "42501") {
      throw new MyError({
        code: "INTERNAL_SERVER_ERROR",
        message: errors.INTERNAL_SERVER_ERROR.DB_PERMISSION_DENIED.message,
        error: errors.INTERNAL_SERVER_ERROR.DB_PERMISSION_DENIED.error,
      });
    }

    if (code === "P0001") {
      if (message.toLowerCase().includes("permission denied")) {
        throw new MyError({
          code: "INTERNAL_SERVER_ERROR",
          message: errors.INTERNAL_SERVER_ERROR.DB_PERMISSION_DENIED.message,
          error: errors.INTERNAL_SERVER_ERROR.DB_PERMISSION_DENIED.error,
        });
      }

      throw new MyError({
        code: "UNAUTHORIZED",
        message: errors.UNAUTHORIZED.INVALID_API_KEY.message,
        error: errors.UNAUTHORIZED.INVALID_API_KEY.error,
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
