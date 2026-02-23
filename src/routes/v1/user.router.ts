import { Elysia } from "elysia";
import apiKeys from "@/routes/v1/user/api-keys"

const router = new Elysia()
    .use(apiKeys);

export default router;
