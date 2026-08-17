/** Decode KafkaJS message headers into string map for WS event frames. */

export function decodeKafkaHeaders(
  headers: Record<string, unknown> | undefined | null,
): Record<string, string> {
  if (!headers || typeof headers !== "object") return {};
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers)) {
    if (v == null) continue;
    if (Buffer.isBuffer(v)) {
      out[k] = v.toString("utf8");
    } else if (typeof v === "string") {
      out[k] = v;
    } else if (Array.isArray(v) && v.length > 0) {
      const first = v[0];
      out[k] = Buffer.isBuffer(first) ? first.toString("utf8") : String(first);
    } else {
      out[k] = String(v);
    }
  }
  return out;
}
