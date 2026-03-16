import { jwtVal } from "@/lib/constants";
import { spVerifyApiKey } from "@/lib/db-scripts/sp-verify-api-key";
import { env } from "@/lib/env";
import { MyError, errors } from "@/lib/errors";
import { AuthHeadersSchema, NoAuthHeadersSchema, apiKeyAuthHeadersSchema } from "@/types/auth";
import { jwt } from "@elysiajs/jwt";
import { Elysia } from "elysia";

/**
 * noAuth
 * ─ public procedures
 */
export const NoAuth = new Elysia()
    .macro({
        noAuth: {
            resolve({ headers }) {
                // ----------------------------
                // Missing headers → UNAUTHORIZED
                // ----------------------------
                if (
                    headers["x-client-id"] == null ||
                    headers["x-client-secret"] == null
                ) {
                    throw new MyError({
                        code: "UNAUTHORIZED",
                        message: errors.UNAUTHORIZED.MISSING_AUTH_HEADERS.message,
                        error: errors.UNAUTHORIZED.MISSING_AUTH_HEADERS.error,
                    });
                }

                // ----------------------------
                // Present but invalid → BAD_REQUEST
                // ----------------------------
                const parseResult = NoAuthHeadersSchema.safeParse(headers);

                if (!parseResult.success) {
                    throw new MyError({
                        code: "BAD_REQUEST",
                        message: errors.BAD_REQUEST.INVALID_HEADER_VALUES.message,
                        error: errors.BAD_REQUEST.INVALID_HEADER_VALUES.error,
                    });
                }

                const parsed = parseResult.data;

                const valid = parsed["x-client-id"] === env.CLIENT_ID && parsed["x-client-secret"] === env.CLIENT_SECRET;

                if (!valid) {
                    throw new MyError({
                        code: "BAD_REQUEST",
                        message: errors.BAD_REQUEST.INVALID_HEADER_VALUES.message,
                        error: errors.BAD_REQUEST.INVALID_HEADER_VALUES.error,
                    });
                }

                return {
                    ctx: {
                        headers: Object.assign({}, headers, parseResult.data),
                    }
                };
            }
        }
    });

/**
 * auth
 * ─ JWT protected procedures
 * ─ attaches `apiUser` to context
 */
export const Auth = new Elysia()
    .use(jwt(jwtVal))
    .macro({
        Auth: {
            async resolve({ headers, jwt }) {

                // ----------------------------
                // Missing headers → UNAUTHORIZED
                // ----------------------------
                if (
                    headers["x-client-id"] == null ||
                    headers["x-client-secret"] == null
                ) {
                    throw new MyError({
                        code: "UNAUTHORIZED",
                        message: errors.UNAUTHORIZED.MISSING_AUTH_HEADERS.message,
                        error: errors.UNAUTHORIZED.MISSING_AUTH_HEADERS.error,
                    });
                }

                // ----------------------------
                // Present but invalid → BAD_REQUEST
                // ----------------------------
                const parseResult = AuthHeadersSchema.safeParse(headers);

                if (!parseResult.success) {
                    throw new MyError({
                        code: "BAD_REQUEST",
                        message: errors.BAD_REQUEST.INVALID_HEADER_VALUES.message,
                        error: errors.BAD_REQUEST.INVALID_HEADER_VALUES.error,
                    });
                }
                // ----------------------------
                // Invalid Bearer format → UNAUTHORIZED
                // ----------------------------
                const { authorization } = parseResult.data;

                if (!authorization?.startsWith("Bearer ")) {
                    throw new MyError({
                        code: "UNAUTHORIZED",
                        message: errors.UNAUTHORIZED.MISSING_AUTHORIZATION_TOKEN.message,
                        error: errors.UNAUTHORIZED.MISSING_AUTHORIZATION_TOKEN.error
                    });
                }

                const token = authorization.slice("Bearer ".length).trim();
                const payload = await jwt.verify(token);

                if (!payload) {
                    throw new MyError({
                        code: "UNAUTHORIZED",
                        message: errors.UNAUTHORIZED.INVALID_TOKEN.message,
                        error: errors.UNAUTHORIZED.INVALID_TOKEN.error
                    });
                }

                const { sub, role } = payload

                if (!sub || !role) {
                    throw new MyError({
                        code: "UNAUTHORIZED",
                        message: errors.UNAUTHORIZED.MISSING_SUB_OR_ROLE.message,
                        error: errors.UNAUTHORIZED.MISSING_SUB_OR_ROLE.error
                    });
                }

                return {
                    ctx: {
                        headers: { headers, ...parseResult.data },
                        username: sub,
                        role: role
                    }
                };
            }
        }
    });


// This runs when you write .guard({ apiKey: true }) or .post(..., { apiKey: true })

export const apiKeyAuth = new Elysia()
    .use(NoAuth)
    .macro("apiKey", {
        noAuth: true,
        resolve: async ({ ctx }) => {

            const { headers } = ctx;

            if (headers["x-api-key"] == null ||
                headers["x-api-secret"] == null) {
                throw new MyError({
                    code: "UNAUTHORIZED",
                    message: errors.UNAUTHORIZED.MISSING_API_KEY.message,
                    error: errors.UNAUTHORIZED.MISSING_API_KEY.error,
                });
            }

            // ----------------------------
            // Present but invalid → BAD_REQUEST
            // ----------------------------
            const parseResult = apiKeyAuthHeadersSchema.safeParse(headers);

            if (!parseResult.success) {
                throw new MyError({
                    code: "BAD_REQUEST",
                    message: errors.BAD_REQUEST.INVALID_HEADER_VALUES.message,
                    error: errors.BAD_REQUEST.INVALID_HEADER_VALUES.error,
                });
            }

            const { "x-api-key": xApiKey, "x-api-secret": xApiSecret } = parseResult.data;

            // Resolve + validate API key metadata via SP
            const { success, data, error } = await spVerifyApiKey({
                xApiKey,
            });

            if (error || !success || !data) {
                throw new MyError({
                    code: "UNAUTHORIZED",
                    message: errors.UNAUTHORIZED.INVALID_API_KEY.message,
                    error: errors.UNAUTHORIZED.INVALID_API_KEY.error,
                });
            }

            // Verify secret hash (crypto stays in backend)
            const valid = await Bun.password.verify(
                xApiSecret,
                data.apiKeySecretHash
            );

            if (!valid) {
                throw new MyError({
                    code: "UNAUTHORIZED",
                    message: errors.UNAUTHORIZED.INVALID_API_KEY.message,
                    error: errors.UNAUTHORIZED.INVALID_API_KEY.error,
                });
            }

            // Attach context
            return {
                ctx: {
                    headers: Object.assign({}, ctx.headers, parseResult.data),
                    xApiKey,
                    apiUserId: data.apiUserId,
                },

            };
        }
    });
