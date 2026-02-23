import { env } from "@/lib/env";

export const ALLOWED_ORIGINS = {
  fixed: [
    "http://localhost:3000",
    // Add any other exact origins here (e.g., your production domain later)
  ],
  wildcards: [
    ".trycloudflare.com",
    ".devtunnels.ms",
    ".vercel.app",
    ".dheerajy1dev.dpdns.org",
  ],
};

export const runtimeTag = process.env.VERCEL_ENV
  ? `vercel-${process.env.VERCEL_ENV}`
  : "local-node";

// JWT
export const jwtVal = {
    name: 'jwt',
    secret: env.JWT_SECRET,
    exp: "1d",
    iss: "kafka-realtime-gateway"
}