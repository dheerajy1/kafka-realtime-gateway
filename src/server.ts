import app from "@/index"

const port = 8089;

app.listen(port, () => {
  console.log(`🚀 Kafka realtime gateway running on http://localhost:${port}`)
});
