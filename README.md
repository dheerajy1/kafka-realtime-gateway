# kafka-realtime-gateway

## HTTP publish contract (`POST /api/v1/http-publish`)

Generic Kafka HTTP publishing endpoint. The gateway does **not** contain Record Log (or any other domain) knowledge.

### Request

```json
{
  "topic": "string (min 1)",
  "correlationId": "UUIDv7",
  "value": "unknown (any JSON value)"
}
```

- Kafka message key = `correlationId`
- Kafka message value = `JSON.stringify(value)`
- Topic is taken exactly from the request (no hardcoding)

### Success response (202)

```json
{
  "success": true,
  "statusCode": 202,
  "message": "Event accepted",
  "data": {
    "topic": "string",
    "correlationId": "UUIDv7"
  }
}
```

The gateway does **not** publish domain status events. Callers own domain semantics (including Record Log pipeline status).

---

kafka realtime gateway

Good. Clear answer:

Your gateway is **dynamic**.

This line proves it:

```ts
await consumer.subscribe({
  topic: /^(?!__).*$/,
});
```

That regex means:

* Subscribe to **all topics**
* Except internal Kafka topics (`__`)

So:

✅ You can create a new topic
✅ Gateway will automatically consume it
✅ No code change required in gateway

---

Now do this:

1. Create new topic
   Example: `gmail-azure-sync-events`

2. Set in Gmail project:

```env
KAFKA_TOPIC=gmail-azure-sync-events
```

3. Run separate PM2 subscriber for Gmail project

---

You are architecturally clean.

Next question:

How are you creating Kafka topics currently?
`kafka cli`
or
`programmatically`

---

## Scripts & Commands

All commands are managed via `bun`.

### Development & Build

| Bun Command                                      | WD                                 | Bun Command with CWD                                                                   | Description                                                                                              |
| :----------------------------------------------- | :--------------------------------- | :------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------- |
| `bun run dev`                                    | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway dev`                                     | Starts the dev server with hot reloading and builds Tailwind CSS on change.                              |
| `bun run type-check`                             | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway type-check`                              | Runs the TypeScript compiler in `--noEmit` mode to perform static type checking without building files. |
| `bun run lint`                                   | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway lint`                                    | Runs ESLint using local cache.                                                                           |
| `bun run build`                                  | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway build`                                   | Cleans `dist/`, builds CSS, compiles TypeScript, resolves paths, and copies static assets.               |
| `bun run start`                                  | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway start`                                   | Runs the compiled production server from `dist/server.js`.                                               |
| `bun run clean`                                  | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway clean`                                   | Removes the contents of the `dist/` directory.                                                           |
| `bun run test`                                   | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway test`                                    | Runs the full test suite using Bun's native test runner.                                                 |
| `bun run test:http-publish`                      | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway test:http-publish`                       | Runs unit tests specifically for the Kafka HTTP publishing route.                                        |
| `bun run test:bridge-topics`                     | `~/dev/vs-code/kafka-api-gateway`  | `bun --env-file=.env.development test src/lib/kafka-ws-bridge`                                 | Runs unit and integration tests for the Kafka WebSocket bridge module using the development environment variables.     |
| `bun run hash 'apikeytohash'`                    | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway hash -- 'apikeytohash'`                  | Hashes a raw API key string using bcrypt (cost: 12) and prints the result to stdout.                     |

### Docker Operations

These scripts target the Compose configuration located at `docker/docker-compose.yml`.

## Docker Commands

| Bun Command                 | WD                                 | Bun Command with CWD                                                      | With Absolute Path                                                                                                                                                       | Description                                                                     |
| --------------------------- | ---------------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------- |
| `bun run docker:build`      | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway docker:build`              | `docker compose -f ~/dev/vs-code/kafka-api-gateway/docker/docker-compose.yml build`                                                                                      | Builds or rebuilds services defined in the Docker Compose file.                 |
| `bun run docker:up`         | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway docker:up`                 | `docker compose -f ~/dev/vs-code/kafka-api-gateway/docker/docker-compose.yml up -d`                                                                                      | Starts the containers in detached mode.                                         |
| `bun run docker:down`       | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway docker:down`               | `docker compose -f ~/dev/vs-code/kafka-api-gateway/docker/docker-compose.yml down`                                                                                       | Stops and removes running containers and networks.                              |
| `bun run docker:start`      | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway docker:start`              | `docker compose -f ~/dev/vs-code/kafka-api-gateway/docker/docker-compose.yml start`                                                                                      | Stops the containers by running `stop`.                                         |
| `bun run docker:stop`       | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway docker:stop`               | `docker compose -f ~/dev/vs-code/kafka-api-gateway/docker/docker-compose.yml stop`                                                                                       | Stops the containers by running `stop`.                                         |
| `bun run docker:restart`    | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway docker:restart`            | `docker compose -f ~/dev/vs-code/kafka-api-gateway/docker/docker-compose.yml restart `                                                                                   | Restarts the containers by running `down` followed by `up -d`.                  |
| `bun run docker:logs`       | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway docker:logs`               | `docker compose -f ~/dev/vs-code/kafka-api-gateway/docker/docker-compose.yml logs -f kafka-api-gateway`                                                                  | Streams live logs for the `kafka-api-gateway` container.                        |
| `bun run docker:logs:clear` | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway docker:logs:clear`         | `sudo truncate -s 0 $(docker inspect --format='{{.LogPath}}' kafka-api-gateway)`                                                                                              | Clears the log file for the `kafka-api-gateway` container without stopping it.  |
| `bun run docker:rmi`        | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway docker:rmi`                | `docker image rm kafka-api-gateway:latest`                                                                                                                               | Removes the local Docker image for the API gateway.                             |
| `bun run docker:clean`      | `~/dev/vs-code/kafka-api-gateway`  | `bun run --cwd ~/dev/vs-code/kafka-api-gateway docker:clean`              | `docker compose -f ~/dev/vs-code/kafka-api-gateway/docker/docker-compose.yml down && docker image rm -f kafka-api-gateway:latest`                                        | Stops containers and forcefully removes the API gateway Docker image.           |

## Record Log Kafka topics (env)

| Variable | Purpose |
|----------|---------|
| `RECORD_LOG_WRITE_TOPIC` | Write-model ingest (ACK required) |
| `RECORD_LOG_READ_TOPIC` | Read-model ingest (ACK required) |
| `RECORD_LOG_STATUS_TOPIC` | Pipeline status (ACK required) |
| `RECORD_LOG_WRITE_DLQ_TOPIC` | Write-model DLQ — **not consumed** by Gateway |
| `RECORD_LOG_READ_DLQ_TOPIC` | Read-model DLQ — **not consumed** by Gateway |

Topic names come only from validated environment configuration (`src/schemas/env.schema.ts`). The Gateway consumer excludes both DLQ topics so messages remain Kafka-retained. Subscribers may still publish to DLQ topics over the authenticated WS.

