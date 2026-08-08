import httpPublish from "@/routes/v1/kafka/http-publish";
import wsPublish from "@/routes/v1/kafka/ws-publish";
import wsSubscribe from "@/routes/v1/kafka/ws-subscribe";

import { Elysia } from "elysia";

const router = new Elysia().use(httpPublish).use(wsPublish).use(wsSubscribe);

export default router;
