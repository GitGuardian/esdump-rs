FROM rust:slim-bookworm AS builder

WORKDIR /usr/src/

COPY . .

RUN cargo install --locked --path=.


FROM debian:bookworm-slim

COPY --from=builder /usr/local/cargo/bin/esdump-rs /usr/local/bin/esdump-rs

ENTRYPOINT ["esdump-rs"]
