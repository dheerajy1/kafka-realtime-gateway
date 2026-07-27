import { Elysia } from "elysia";

export default new Elysia()
  .use(
    new Elysia().macro("testAuth", {
      resolve() {
        console.log("[TEST2] resolve begin");

        console.log("[TEST2] resolve end");

        return {
          userId: 1,
        };
      },
    }),
  )
  .guard({
    testAuth: true,
  })
  .ws("/ws-test2", {
    open(ws) {
      console.log("[TEST2] OPEN");

      ws.send({
        type: "ready",
      });
    },

    message(ws, message) {
      console.log("[TEST2] MESSAGE", message);

      ws.send({
        type: "echo",
        message,
      });
    },

    close() {
      console.log("[TEST2] CLOSE");
    },
  });
