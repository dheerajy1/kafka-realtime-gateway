# Build stage: compiles the release binary
FROM rust:1-bookworm AS builder

WORKDIR /app

COPY . .

# The binary is named "main" in Cargo.toml
RUN cargo build --release --locked --package kafka-api-gateway

# Runtime stage: only the binary and CA certificates
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/main ./main

EXPOSE 8089

CMD ["./main"]