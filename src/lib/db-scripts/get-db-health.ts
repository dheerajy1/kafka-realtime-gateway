import { getOnPremPool, isDbConnecting } from "@/lib/db";

export async function getDbHealth(): Promise<
  "connected" | "connecting" | "disconnected"
> {
  try {
    if (isDbConnecting()) {
      return "connecting";
    }

    const pool = await getOnPremPool({ poolKey: "default" });

    await pool.query("SELECT 1");

    return "connected";
  } catch {
    return "disconnected";
  }
}
