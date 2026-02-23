import { spCreateApiKey } from "@/lib/db-scripts/sp-create-api-key";
import { MyError, errors } from "@/lib/errors";
import { Auth } from "@/middleware/auth";
import crypto from "crypto";
import { Elysia } from "elysia";
import z from "zod";

const router = new Elysia()
    .use(Auth);

router.post(
    "/api-keys",
    async ({ ctx, body }) => {
        const { apiKeyId } = body;
        const { username } = ctx;

        // generate secret (shown ONCE)
        const apiKeySecret = crypto.randomBytes(32).toString("hex");

        // hash secret (never store raw)
        const apiKeySecretHash = await Bun.password.hash(apiKeySecret);

        // call SP (DB owns logic)
        const { success, data, error } = await spCreateApiKey({
            username,
            apiKeyId,
            apiKeySecretHash,
        });

        if (error || !success || !data) {
            // duplicate key or SP THROW bubbles up here
            console.error(`/api/v1/api-keys`, error)
            throw new MyError({
                code: "CONFLICT",
                message: errors.CONFLICT.DATABASE_CONFLICT.message,
                error: errors.CONFLICT.DATABASE_CONFLICT.error,
            });
        }

        // return secret ONCE
        return {
            success: true,
            statusCode: 200,
            message: "API key created successfully",
            data: {
                apiKeyId,
                apiKeySecret,
            },
        };
    },
    {
        Auth: true,
        body: z.object({
            apiKeyId: z
                .string()
                .min(8)
                .max(64)
                .regex(/^[a-zA-Z0-9_-]+$/),
        }),
        // headers: AuthHeadersSchema,
        detail: {
            tags: ["API Keys"],
            summary: "Create API key",
            description: "Create an API key for an authenticated API user",
        },
    }
);

export default router;
