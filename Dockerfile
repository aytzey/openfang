# syntax=docker/dockerfile:1
FROM oven/bun:1.3.11 AS bun

FROM rust:1-slim-bookworm AS builder
WORKDIR /build
COPY --from=bun /usr/local/bin/bun /usr/local/bin/bun
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY xtask ./xtask
COPY agents ./agents
COPY packages ./packages
COPY deploy ./deploy
RUN cargo build --locked --release --bin openfang

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/openfang /usr/local/bin/
COPY --from=builder /build/agents /opt/openfang/agents
COPY deploy/openfang-entrypoint.sh /usr/local/bin/openfang-entrypoint
RUN chmod +x /usr/local/bin/openfang-entrypoint
EXPOSE 4200
VOLUME /data
ENV HOME=/data
ENTRYPOINT ["openfang-entrypoint"]
CMD ["start"]
