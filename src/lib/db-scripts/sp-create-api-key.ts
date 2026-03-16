import { getOnPremPool } from "@/lib/db";
import sql from "mssql";

type Notes = {
    apiKeyId: string;
};

type SpOutput = {
    result: "SUCCESS";
    notes: string;
};
export async function spCreateApiKey({
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
        const result = await pool
            .request()
            .input("userName", sql.VarChar(100), username)
            .input("apiKeyId", sql.VarChar(100), apiKeyId)
            .input("apiKeySecretHash", sql.VarChar(255), apiKeySecretHash)
            .execute("[dbo].[01spcreateApiKey]");

        const row = result.recordset?.[0] as SpOutput;

        const notes = JSON.parse(row.notes) as Notes;

        if (!row || row.result !== "SUCCESS") {
            throw new Error("SP did not return SUCCESS");
        }

        return {
            success: true,
            data: notes,
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
