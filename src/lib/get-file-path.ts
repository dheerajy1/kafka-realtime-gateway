import path from "path";
import { fileURLToPath } from "url";

export function getFilePath() {
  const fullPath = fileURLToPath(import.meta.url);
  return path.relative(process.cwd(), fullPath);
}