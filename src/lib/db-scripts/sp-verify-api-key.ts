import { getOnPremPool } from "@/lib/db";
import sql from "mssql";

type Notes = {
    apiKeyId: string;
    apiUserId: number;
    apiKeySecretHash: string;
};

type SpOutput = {
    result: "SUCCESS";
    notes: string;
};

export async function spVerifyApiKey({
    xApiKey,
}: {
    xApiKey: string;
}) {
    const pool = await getOnPremPool({ poolKey: "service" });

    try {
        const result = await pool
            .request()
            .input("apiKeyIdInput", sql.VarChar(100), xApiKey)
            .execute("[dbo].[02spverifyApiKey]");

        const row = result.recordset?.[0] as unknown as SpOutput;

        const notes = JSON.parse(row.notes) as Notes;

        if (!row || row.result !== "SUCCESS") {
            throw new Error("SP did not return SUCCESS");
        }

        return {
            success: true,
            data: notes,
        };
    } catch (error: unknown) {

        // FIXME: test
        // console.log(`src/lib/db-scripts/sp-verify-api-key.ts error`, error);

        const msg =
            error instanceof Error ? error.message : "Internal server error";

        return {
            success: false,
            error: msg,
        };
    }
}
