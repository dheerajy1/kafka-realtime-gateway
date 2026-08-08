import { Elysia } from "elysia";

export default new Elysia()
  .ws("/ws-test", {

    open(ws) {
      console.log("[TEST1] OPEN");

      ws.send(JSON.stringify({
        type: "ready"
      }));
    },

    message(ws, message) {
      console.log("[TEST1] MESSAGE", message);

      ws.send(JSON.stringify({
        type: "echo",
        message
      }));
    },

    close() {
      console.log("[TEST1] CLOSE");
    }

  });