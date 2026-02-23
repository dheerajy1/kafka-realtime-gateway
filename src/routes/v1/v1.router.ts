import { Elysia } from "elysia";
import authRouter from "@/routes/v1/auth.router"
import userRouter from "@/routes/v1/user.router"
import kafkaRouter from "@/routes/v1/kafka.router"

const router = new Elysia({ prefix: "/api/v1" })
    .use(authRouter)
    .use(userRouter)
    .use(kafkaRouter);

export default router;
