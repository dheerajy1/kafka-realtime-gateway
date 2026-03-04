import app from "@/index";
import { isProd } from "@/lib/constants";

const port = isProd ? 8089 : 5000;

app.listen(port, () => {
  console.log(`🚀 Kafka realtime gateway running on http://localhost:${port}`)
});
