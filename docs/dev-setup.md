# Development setup

How to run the gateway on your machine. For Docker and production see [prod-setup.md](prod-setup.md). For the WebSocket protocol see [websocket.md](websocket.md).

## 1. Prerequisites

| Tool | Notes |
| --- | --- |
| Rust (stable) | The crate uses edition 2024. Install with `rustup`. |
| Tailwind CSS standalone CLI | Must be on `PATH` as `tailwindcss`. `cargo dev`, `cargo start`, `cargo build-prod` and `cargo docker-build` run it. |
| PostgreSQL 18 | The scripts use `uuidv7()`. |
| Apache Kafka 4.1 | Single node (KRaft). Running it with Docker is the easiest way. |
| Docker | Needed for Kafka (optional) and for the gateway image. |

## 2. Environment files

| File | Used by | In git |
| --- | --- | --- |
| `.env.example` | Template with every variable | yes |
| `.env.development` | `cargo dev`, `cargo test-ws` | no |
| `.env.production` | `cargo start`, `cargo build-prod`, Docker Compose | no |

Create the development file:

```bash
cp .env.example .env.development
```

Every variable below is required. A missing or invalid value stops the gateway at boot with a message naming the variable.

| Variable | Rule | Meaning |
| --- | --- | --- |
| `APP_PORT` | positive integer | Port the gateway listens on (all interfaces). |
| `APP_ENV` | `development` or `production` | Runtime environment. Any other value stops the gateway at boot. It is shown in the startup log line. |
| `APP_NAME` | 1 to 50 chars | Application name. |
| `JWT_SECRET` | 1 to 50 chars | Secret used to sign JWTs. |
| `CLIENT_ID`, `CLIENT_SECRET` | 1 to 50 chars each | Client credentials checked by the server gate. |
| `ONPREM_DB_SERVER` | 1 to 50 chars | PostgreSQL host. |
| `ONPREM_DB_PORT` | positive integer | PostgreSQL port (default 5432). |
| `ONPREM_DB_DATABASE` | 1 to 50 chars | Database name. |
| `ONPREM_DB_USER_PUBLIC`, `ONPREM_DB_PASSWORD_PUBLIC` | 1 to 50 chars each | The public database login. |
| `ONPREM_DB_USER_SERVICE`, `ONPREM_DB_PASSWORD_SERVICE` | 1 to 50 chars each | The service database login. The gateway connects with it at startup. |
| `KAFKA_BROKERS` | at least 1 char | Broker address list. `localhost:9092` for local runs. |
| `KAFKA_CLIENT_ID` | 1 to 100 chars | Kafka client id. |
| `KAFKA_GROUP_ID` | 1 to 100 chars | Kafka consumer group id. |

Passwords hashed with bcrypt are made with `cargo hash 'value'`.

## 3. Database

The schema, the three functions and the seed data live in the separate `kafka_api_gateway_db` repository. Run its scripts in this order:

1. `create_db_kafka_api_gateway_db.sql`, connected to the `postgres` database.
2. `logins/create_login_kag_proj_public.sql` and `logins/create_login_kag_proj_service.sql` (set the passwords first, they must match the env file).
3. `scripts-table/*.sql`: `kafka_topic`, `api_keys`, `portal_users`, `api_users`.
4. `scripts-fun/*.sql`: `fn_get_gateway_kafka_topics`, `fn_create_api_key`, `fn_verify_api_key`.
5. `data/*.txt` in the order `01`, `02`, `03`.

The gateway reads topics, users and API keys only through those functions.

## 4. Kafka

Local runs use a broker on `localhost:9092`. The Docker container reaches the same broker on `host.docker.internal:9094`, so the broker needs two listeners:

| Listener | Port | Advertised address | Used by |
| --- | --- | --- | --- |
| `PLAINTEXT` | 9092 | `localhost:9092` | the gateway running on the host (`cargo dev`) |
| `PLAINTEXT_HOST` | 9094 | `host.docker.internal:9094` | the gateway running in Docker |

Set `KAFKA_BROKERS` to match: `localhost:9092` in `.env.development`, `host.docker.internal:9094` in `.env.production`.

Messages up to 10 MB are accepted by the gateway. To carry more than 1 MB through Kafka, raise the broker `message.max.bytes`, the topic `max.message.bytes` and the replica fetch limit.

Every topic the gateway may use needs a Kafka topic and a row in `kafka_topic`. After changing the rows, call `POST /api/v1/topics/reload` (or restart the gateway).

## 5. Run the gateway

```bash
cargo dev
```

The gateway listens on `http://localhost:<APP_PORT>`. Stop it with Ctrl+C: it closes WebSocket connections with code 1001 and exits.

The OpenAPI reference is at `/openapi` (Scalar UI) and `/openapi/json`. It describes the HTTP endpoints only. The WebSocket endpoints are in [websocket.md](websocket.md).

Kafka health: `/healthz` and `/api/v1/health` show a small Kafka block, and `/healthz/kafka` (portal) and `/api/v1/health/kafka` (API) show the full result. See [prod-setup.md](prod-setup.md), section 7.

The Kafka client library repeats warnings many times a second while the broker is down, so by default the log shows only its errors. To see its warnings, start with `RUST_LOG=info,kafkit_client=warn cargo dev`.

## 6. Checks

Run these before every commit, in this order. CI runs the same checks.

### 6.1 format

```bash
cargo fmt
```
### 6.2 check

```bash
cargo check --workspace --all-targets
```
- reduce noise

```bash
cargo check --workspace --all-targets 2>&1 | grep -Ev '^( *Compiling| *Checking [^ ]+ v[^ ]+$)'
```

### 6.3 format and check

```bash
cargo fmt && cargo check --workspace --all-targets
```

- reduce noise

```bash
cargo fmt && cargo check --workspace --all-targets 2>&1 | grep -Ev '^( *Compiling| *Checking [^ ]+ v[^ ]+$)'
```

### 6.4 clippy

```bash
cargo clippy --workspace --all-targets --locked -- -D warnings
```

### 6.5 Testing

```bash
cargo test --workspace
```

- reduce noise

```bash
cargo test --workspace 2>&1 | grep -P '^(?!.*SIGTERM).*(Finished|Running|test result|FAILED|panicked|error|Doc-tests)'
```

### 6.6 Combined

```bash
cargo fmt && cargo check --workspace --all-targets && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace
```

- reduce noise

```bash
cargo fmt && cargo check --workspace --all-targets 2>&1 | grep -Ev '^( *Compiling| *Checking [^ ]+ v[^ ]+$)' && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace 2>&1 | grep -P '^(?!.*SIGTERM).*(Finished|Running|test result|FAILED|panicked|error|Doc-tests)'
```

The automated tests use in-memory fakes. They need no database and no broker. Tests live in the `tests/` folder.

## 7. Live WebSocket test

`cargo test-ws` runs a 40 step scenario against a running gateway and a real broker: auth, subscribe, publish, ACK, large messages and the 1001 close. It cannot run in CI, so run it by hand before a release.

Prerequisites:

- The gateway runs with `.env.development` (`cargo dev` in another terminal).
- Kafka runs, with the topics `gateway-test` and `gateway-test-ack`.
- `kafka_topic` has rows for both: `gateway-test` consumes, needs no ACK and allows subscriber publishing. `gateway-test-ack` consumes, needs an ACK and allows subscriber publishing.
- An API key id and secret.

```bash
cargo test-ws 'api-key-id' 'api-key-secret'
```

To check the 1001 close, run the hold mode, then stop the gateway with Ctrl+C in the other terminal:

```bash
cargo test-ws 'api-key-id' 'api-key-secret' hold
```