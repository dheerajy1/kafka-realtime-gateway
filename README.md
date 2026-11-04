# kafka-realtime-gateway

An HTTP and WebSocket gateway in front of Apache Kafka, written in Rust (axum). Clients publish to and subscribe to Kafka topics through authenticated HTTP and WebSocket endpoints. Users, API keys and the topic registry live in PostgreSQL.

## Stack

| Part | Choice |
| --- | --- |
| Web framework | axum 0.8 on tokio |
| Kafka | `kafkit-client` (producer, consumer bridge, topic admin) |
| Database | PostgreSQL through `sqlx` |
| Pages | Dioxus server-side rendering, Tailwind CSS |
| API docs | OpenAPI with Scalar UI at `GET /openapi` (behind the portal gate) |
| Tooling | cargo workspace with an `xtask` crate for the aliases below |

## Quick start

1. Copy `.env.example` to `.env.development` and fill in the values.
2. Start Kafka and PostgreSQL.
3. Run `cargo dev`. The gateway listens on `http://localhost:8089`.

Full setup: [docs/dev-setup.md](docs/dev-setup.md). Production and Docker: [docs/prod-setup.md](docs/prod-setup.md). Build plan and decisions: [docs/build.md](docs/build.md).

## Scripts & Commands

All commands run from the repository root, `~/dev/vs-code/kafka-api-gateway-rust` (WD). They are cargo aliases defined in `.cargo/config.toml`.

### Development & Build

| Command | WD | Description |
| --- | --- | --- |
| `cargo dev` | `~/dev/vs-code/kafka-api-gateway-rust` | Loads `.env.development`, builds Tailwind CSS, runs the gateway in debug mode. |
| `cargo start` | `~/dev/vs-code/kafka-api-gateway-rust` | Loads `.env.production`, builds Tailwind CSS, runs the gateway in release mode. |
| `cargo build-prod` | `~/dev/vs-code/kafka-api-gateway-rust` | Validates `.env.production`, builds Tailwind CSS, builds the release binary. |
| `cargo css` | `~/dev/vs-code/kafka-api-gateway-rust` | Compiles `src/tailwind.css` into `public/global.css`. |
| `cargo hash 'value'` | `~/dev/vs-code/kafka-api-gateway-rust` | Prints the bcrypt hash (cost 12) of the value. |
| `cargo test-ws 'api-key' 'api-secret'` | `~/dev/vs-code/kafka-api-gateway-rust` | Runs the live WebSocket scenario against a running gateway and broker (manual check, not part of CI). |
| `cargo test --workspace` | `~/dev/vs-code/kafka-api-gateway-rust` | Runs the automated tests (no database or broker needed). |

### Docker Operations

These aliases target the Compose file at `docker/docker-compose.yml`.

| Command | WD | Underlying command | Description |
| --- | --- | --- | --- |
| `cargo docker-build` | `~/dev/vs-code/kafka-api-gateway-rust` | `cargo css`, then `docker compose -f docker/docker-compose.yml build` | Rebuilds the stylesheet, then builds the image. |
| `cargo docker-up` | `~/dev/vs-code/kafka-api-gateway-rust` | `docker compose -f docker/docker-compose.yml up -d` | Creates and starts the container in detached mode. |
| `cargo docker-down` | `~/dev/vs-code/kafka-api-gateway-rust` | `docker compose -f docker/docker-compose.yml down` | Stops and removes the container and network. |
| `cargo docker-start` | `~/dev/vs-code/kafka-api-gateway-rust` | `docker compose -f docker/docker-compose.yml start` | Starts the stopped container. |
| `cargo docker-stop` | `~/dev/vs-code/kafka-api-gateway-rust` | `docker compose -f docker/docker-compose.yml stop` | Stops the container (graceful shutdown, 15 s limit). |
| `cargo docker-restart` | `~/dev/vs-code/kafka-api-gateway-rust` | `docker compose -f docker/docker-compose.yml restart` | Restarts the container. |
| `cargo docker-logs` | `~/dev/vs-code/kafka-api-gateway-rust` | `docker compose -f docker/docker-compose.yml logs -f` | Streams the live logs. |
| `cargo docker-logs-clear` | `~/dev/vs-code/kafka-api-gateway-rust` | `sudo truncate -s 0 $(docker inspect --format='{{.LogPath}}' kafka-api-gateway)` | Empties the container log file without stopping it. |
| `cargo docker-rmi` | `~/dev/vs-code/kafka-api-gateway-rust` | `docker rmi kafka-api-gateway:latest` | Removes the local image. |
| `cargo docker-clean` | `~/dev/vs-code/kafka-api-gateway-rust` | `docker compose -f docker/docker-compose.yml down`, then `docker rmi kafka-api-gateway:latest` | Removes the container and the image. |

## License

See [LICENSE](LICENSE).
