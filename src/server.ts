import app from "@/index";
import { env } from "@/lib/env";

const port = env.APP_PORT;

app.listen(port, () => {
  console.log(`🚀 Kafka realtime gateway running on http://localhost:${port}`)
});
