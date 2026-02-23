import { getOnPremPool } from "@/lib/db";
import sql from "mssql";

export type SpVerifyApiKeyRow = {
    Result: "SUCCESS";
    IDApiKey: number;
    IDApiUser: number;
    ApiKeySecretHash: string;
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
            .input("ApiKeyId", sql.VarChar(100), xApiKey)
            .execute("dbo.SP_VerifyApiKey");

        const row = result.recordset?.[0] as SpVerifyApiKeyRow | undefined;

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
