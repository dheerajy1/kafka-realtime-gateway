/** ULID shape check (Crockford base32, 26 chars). Does not verify timestamp. */
export function isUlid(value: string): boolean {
  return typeof value === "string" && /^[0-9A-HJKMNP-TV-Z]{26}$/i.test(value);
}
