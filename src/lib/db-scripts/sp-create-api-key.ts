import sql from "mssql";
import { getOnPremPool } from "@/lib/db";

type SpCreateApiKeyRow = {
    Result: "SUCCESS";
    ApiKeyId: string;
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
            .input("Username", sql.VarChar(100), username)
            .input("ApiKeyId", sql.VarChar(100), apiKeyId)
            .input("ApiKeySecretHash", sql.VarChar(255), apiKeySecretHash)
            .execute("dbo.SP_CreateApiKey");

        const row = result.recordset?.[0] as SpCreateApiKeyRow | undefined;

        if (!row || row.Result !== "SUCCESS") {
            throw new Error("SP did not return SUCCESS");
        }

        return {
            success: true,
            data: row,
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
