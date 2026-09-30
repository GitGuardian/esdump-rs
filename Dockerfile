FROM --platform=$BUILDPLATFORM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y cmake && apt-get clean

WORKDIR /usr/src/

COPY . .

RUN cargo install --locked --path=.

FROM --platform=$BUILDPLATFORM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y openssl ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/cargo/bin/esdump-rs /usr/local/bin/esdump-rs

ENTRYPOINT ["esdump-rs"]
