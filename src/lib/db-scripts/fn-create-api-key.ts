import { getOnPremPool } from "@/lib/db";

type Notes = {
  apiKeyId: string;
};

type DbRow = {
  result: "SUCCESS";
  notes: Notes;
};

export async function fnCreateApiKey({
  username,
  apiKeyId,
  apiKeySecretHash,
}: {
  username: string;
  apiKeyId: string;
  apiKeySecretHash: string;
}) {
  const pool = await getOnPremPool({ poolKey: "service" });

  try {
    const { rows } = await pool.query<DbRow>(
      `
            SELECT *
            FROM fn_create_api_key($1, $2, $3)
            `,
      [username, apiKeyId, apiKeySecretHash],
    );

    const row = rows[0];

    if (!row || row.result !== "SUCCESS") {
      throw new Error("Function did not return SUCCESS");
    }

    return {
      success: true,
      data: row.notes,
    };
  } catch (error: unknown) {
    const msg =
      error instanceof Error ? error.message : "Internal server error";

    return {
      success: false,
      error: msg,
    };
  }
}
