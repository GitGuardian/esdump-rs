FROM --platform=$BUILDPLATFORM rust:1-buster AS builder

RUN apt-get update && apt-get install cmake -y

WORKDIR /usr/src/

COPY . .

RUN cargo install --locked --path=.

FROM --platform=$BUILDPLATFORM debian:buster

RUN apt-get update && apt-get install openssl -y && apt-get clean

COPY --from=builder /usr/local/cargo/bin/esdump-rs /usr/local/bin/esdump-rs

ENTRYPOINT ["esdump-rs"]
