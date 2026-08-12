import { z } from "zod";

export const JwtPayloadSchema = z.object({
    sub: z.string(),
    role: z.enum(["admin", "user"]),
    iat: z.number(),
    exp: z.number(),
    iss: z.string().optional(),
});

export type JwtPayloadType = z.infer<typeof JwtPayloadSchema>;

// =========================
// Schema for required auth headers
// =========================
export const NoAuthHeadersSchema = z.object({
    "x-client-id": z
        .string()
        .trim()
        .min(1, { message: "x-client-id is required and cannot be empty" })
        .max(44, "CLIENT_ID must match expected length")
        .describe("Client identifier"),
    "x-client-secret": z
        .string()
        .trim()
        .min(1, { message: "x-client-secret is required and cannot be empty" })
        .max(100, "CLIENT_SECRET must be 32 bytes base64 (openssl rand -base64 32)")
        .describe("Client secret"),
});

export const AuthHeadersSchema = z.object({
    "x-client-id": z
        .string()
        .trim()
        .min(1, { message: "x-client-id is required and cannot be empty" })
        .max(44, "x-client-id must match expected max")
        .describe("Client identifier"),
    "x-client-secret": z
        .string()
        .trim()
        .min(1, { message: "x-client-secret is required and cannot be empty" })
        .max(100, "x-client-secret must be 32 bytes base64 (openssl rand -base64 32)")
        .describe("Client secret"),
    authorization: z
        .string()
        .regex(/^Bearer .+/, "Must be a valid Bearer token")
        .describe("JWT Bearer token"),
});

export const apiKeyAuthHeadersSchema = z.object({
    "x-api-key": z
        .string()
        .trim()
        .min(1, { message: "x-api-key is required and cannot be empty" })
        .max(44, "CLIENT_ID must match expected max")
        .describe("Client identifier"),
    "x-api-secret": z
        .string()
        .trim()
        .min(1, { message: "x-api-secret is required and cannot be empty" })
        .max(100, "CLIENT_SECRET must be 32 bytes base64 (openssl rand -base64 32)")
        .describe("Client secret"),
});

// =========================
// LOGIN REQUEST
// =========================
export const LoginRequestSchema = z.object({
    username: z.string(),
    password: z.string(),
});

export const LoginInputSchema = LoginRequestSchema
// z.object({
//     headers: NoAuthHeadersSchema,
//     body: LoginRequestSchema,
// })

// =========================
// LOGIN RESPONSE SUCCESS 
// =========================
export const LoginResponseSchema = z.object({
    success: z.literal(true),
    httpCode: z.literal(200),
    message: z.string(),
    data: z.object({
        token: z.string(),
        expiresAt: z.string(),
    }),
});

export type LoginResponse = z.infer<typeof LoginResponseSchema>;

// =========================
// LOGOUT REQUEST
// =========================
export const LogoutRequestSchema = z.object({
    username: z.string(),
    password: z.string(),
});

export const LogoutInputSchema = z.object({
    headers: NoAuthHeadersSchema,
    // body: LoginRequestSchema,
})
// =========================
// SIGNUP REQUEST
// =========================
export const SignupRequestSchema = z.object({
    username: z.string(),
    password: z.string(),
});

export const SignupInputSchema = z.object({
    headers: NoAuthHeadersSchema,
    body: SignupRequestSchema,
})

// =========================
// SIGNUP RESPONSE SUCCESS 
// =========================
export const SignupResponseSchema = z.object({
    success: z.literal(true),
    httpCode: z.literal(200),
    message: z.string(),
    data: z.object({
        userId: z.number(),
    }),
});

// =========================
// REFRESH TOKEN REQUEST
// =========================

export const RefreshTokenInputSchema = z.object({
    headers: z.object({
        "x-client-id": z
            .string()
            .trim()
            .min(1, { message: "x-client-id is required and cannot be empty" })
            .max(44, "x-client-id must match expected length")
            .describe("Client identifier"),
        "x-client-secret": z
            .string()
            .trim()
            .min(1, { message: "x-client-secret is required and cannot be empty" })
            .max(100, "x-client-secret must be 32 bytes base64 (openssl rand -base64 32)")
            .describe("Client secret"),
        authorization: z
            .string()
            .regex(/^Bearer .+/, "Must be a valid Bearer token")
            .describe("JWT Bearer token"),
    }),
})

// =========================
// REFRESH TOKEN RESPONSE SUCCESS
// =========================
export const RefreshTokenResponseSchema = z.object({
    success: z.literal(true),
    httpCode: z.literal(200),
    message: z.string(),
    data: z.object({
        token: z.object({
            accessToken: z.string(),
            refreshToken: z.string(),
        }),
    }),
});

export type RefreshTokenResponse = z.infer<typeof RefreshTokenResponseSchema>;

// =========================
// ERROR RESPONSE
// =========================
export const AuthErrorResponseSchema = z.object({
    success: z.literal(false),
    httpCode: z.number(),
    error: z.string(),
    message: z.string(),
});

export type AuthErrorResponse = z.infer<typeof AuthErrorResponseSchema>;


// =========================
// MISCELLANEOUS
// =========================

export type ErrorCode =
  | "BAD_REQUEST"
  | "UNAUTHORIZED"
  | "PAYMENT_REQUIRED"
  | "FORBIDDEN"
  | "NOT_FOUND"
  | "METHOD_NOT_SUPPORTED"
  | "TIMEOUT"
  | "CONFLICT"
  | "PRECONDITION_FAILED"
  | "PAYLOAD_TOO_LARGE"
  | "UNSUPPORTED_MEDIA_TYPE"
  | "UNPROCESSABLE_CONTENT"
  | "PRECONDITION_REQUIRED"
  | "TOO_MANY_REQUESTS"
  | "CLIENT_CLOSED_REQUEST"
  | "INTERNAL_SERVER_ERROR"
  | "NOT_IMPLEMENTED"
  | "BAD_GATEWAY"
  | "SERVICE_UNAVAILABLE"
  | "GATEWAY_TIMEOUT";

export type CtxAuthError = {
  code: ErrorCode | null;
  httpCode: number | null;
  message: string | null;
  error: string | null;
};
