import { Elysia } from "elysia";

export default new Elysia()
  .use(
    new Elysia().macro("testAuth", {
      resolve: async () => {
        console.log("[TEST3] begin");

        await Bun.sleep(100);

        console.log("[TEST3] end");

        return {
          userId: 1,
        };
      },
    }),
  )
  .guard({
    testAuth: true,
  })
  .ws("/ws-test3", {
    open(ws) {
      console.log("[TEST3] OPEN");

      ws.send({
        type: "ready",
      });
    },

    message(ws, message) {
      console.log("[TEST3] MESSAGE", message);

      ws.send({
        type: "echo",
        message,
      });
    },

    close() {
      console.log("[TEST3] CLOSE");
    },
  });