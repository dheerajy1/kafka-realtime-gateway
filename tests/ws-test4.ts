import { Elysia } from "elysia";

const hash = await Bun.password.hash("secret");

export default new Elysia()
  .use(
    new Elysia().macro("testAuth", {
      resolve: async () => {
        console.log("[TEST4] begin");

        const ok = await Bun.password.verify("secret", hash);

        console.log("[TEST4] verified", ok);

        return {
          userId: 1,
        };
      },
    }),
  )
  .guard({
    testAuth: true,
  })
  .ws("/ws-test4", {
    open(ws) {
      console.log("[TEST4] OPEN");

      ws.send({
        type: "ready",
      });
    },

    message(ws, message) {
      console.log("[TEST4] MESSAGE", message);

      ws.send({
        type: "echo",
        message,
      });
    },

    close() {
      console.log("[TEST4] CLOSE");
    },
  });