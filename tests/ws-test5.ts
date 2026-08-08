import { Elysia } from "elysia";

export default new Elysia()
  .use(
    new Elysia().macro("testAuth", {
      resolve: async () => {
        console.log("[TEST5] begin");

        await Bun.sleep(20);

        const fakeUser = {
          apiUserId: 1,
        };

        console.log(fakeUser);

        return fakeUser;
      },
    }),
  )
  .guard({
    testAuth: true,
  })
  .ws("/ws-test5", {
    open(ws) {
      console.log("[TEST5] OPEN");

      ws.send({
        type: "ready",
      });
    },

    message(ws, message) {
      console.log("[TEST5] MESSAGE", message);

      ws.send({
        type: "echo",
        message,
      });
    },

    close() {
      console.log("[TEST5] CLOSE");
    },
  });