# Production setup

How to run the gateway as a Docker container. For local development see [dev-setup.md](dev-setup.md). For the WebSocket protocol see [websocket.md](websocket.md).

## 1. What runs where

The gateway is one container, `kafka-api-gateway`, built from `docker/Dockerfile` (a Rust build stage and a `debian:bookworm-slim` runtime stage). It listens on all interfaces, port `8089`, and Compose publishes `8089:8089`. PostgreSQL and Kafka run outside the container, on the Docker host.

`localhost` inside a container is the container itself, so the container reaches the host through `host.docker.internal`. Compose adds that name with `extra_hosts: host.docker.internal:host-gateway`.

## 2. Requirements

- Docker with Compose.
- The Tailwind CSS CLI on `PATH`, because `cargo docker-build` compiles the stylesheet first.
- PostgreSQL with the `kafka_api_gateway_db` database (see [dev-setup.md](dev-setup.md), section 3), reachable from the container.
- A Kafka broker with a listener on port 9094 that advertises `host.docker.internal:9094` (see [dev-setup.md](dev-setup.md), section 4).

## 3. Environment file

Compose reads `.env.production` from the repository root (`env_file: ../.env.production`). The file is not in git and not in the image: `.gitignore` and `.dockerignore` both exclude it.

It has the same 15 variables as `.env.development` (see [dev-setup.md](dev-setup.md), section 2). Three values differ for the container:

| Variable | Production value |
| --- | --- |
| `APP_ENV` | `production` |
| `ONPREM_DB_SERVER` | `host.docker.internal` |
| `KAFKA_BROKERS` | `host.docker.internal:9094` |

Use your own secrets here. Do not reuse the development ones.

Compose passes the file to the container when it is created. After editing `.env.production`, run `cargo docker-up` again so the container is recreated with the new values.

## 4. Build and start

```bash
cargo docker-build
```

```bash
cargo docker-up
```

`cargo docker-up` starts the container in the background. `restart: unless-stopped` brings it back after a Docker or host restart, unless you stopped it by hand.

Check that it started:

```bash
cargo docker-logs
```

A healthy start logs the PostgreSQL connection, the Kafka producer and consumer, the subscribed topics, and the line with the port.

## 5. Daily operation

| Task | Command |
| --- | --- |
| Follow the logs | `cargo docker-logs` |
| Stop the container | `cargo docker-stop` |
| Start it again | `cargo docker-start` |
| Restart it | `cargo docker-restart` |
| Empty the container log file | `cargo docker-logs-clear` |
| Remove the container and network | `cargo docker-down` |
| Remove the container and the image | `cargo docker-clean` |

`cargo docker-restart` reuses the existing container. To pick up new code or a changed env file, run `cargo docker-build` (code only) and then `cargo docker-up`, which recreates the container.

## 6. Shutdown

On `SIGTERM` (what `docker stop` sends) or Ctrl+C the gateway:

1. Stops accepting new connections and starts an 8 second limit.
2. Closes every WebSocket with code 1001, so clients know to reconnect.
3. Stops the Kafka consumer, flushes and closes the producer, and closes both database pools.
4. Logs `Shutdown complete` and exits.

If this takes longer than 8 seconds, the gateway logs `forcing exit` and exits with code 1.

Compose sets `stop_grace_period: 15s`, so Docker waits longer than the gateway's own 8 second limit before it sends `SIGKILL`.

## 7. Health checks

| Check | Request | Notes |
| --- | --- | --- |
| Process and DB state (portal) | `GET /healthz`, `/healthz/app`, `/healthz/db` | Always 200. The database state is in the body. |
| Process and DB state (API) | `GET /api/v1/health`, `/api/v1/health/app`, `/api/v1/health/db` | Needs the `x-client-id` and `x-client-secret` headers. A database failure returns 503 `DB_PROBE_FAILED`. |

## 8. Network and TLS

The gateway speaks plain HTTP and plain WebSocket. It does no TLS itself.

- The portal login cookie is `Secure`, so a browser only keeps it over HTTPS or on `http://localhost`. To use the portal from another machine, put the gateway behind a TLS terminating proxy or tunnel.
- CORS allows `http://localhost:3000` and the origin suffixes `.trycloudflare.com`, `.devtunnels.ms`, `.vercel.app` and `.dheerajy1dev.dpdns.org`. Requests without an `Origin` header are allowed.
- Anyone who can reach port 8089 can try the API. The client headers and API keys are the protection, so restrict access at the network level as well.

## 9. Running without Docker

To run the release build directly on the host:

```bash
cargo build-prod
```

```bash
cargo start
```

Both read `.env.production`. On the host, set `ONPREM_DB_SERVER` and `KAFKA_BROKERS` to host-local addresses (`localhost`, port 9092) instead of the `host.docker.internal` values.