# esdump-rs

Dump elasticsearch indexes to blob storage, really-really fast :rocket:

## Installation

**Releases:** Grab a pre-built executable [from the releases page](https://github.com/GitGuardian/esdump-rs/releases)

**Docker:** `docker run ghcr.io/gitguardian/esdump-rs:latest`

## Usage

```shell
cargo run --profile release http://localhost:9200 --index=test-index --batches-per-file=5 --batch-size=5000 s3://es-dump/test/ --env-file=test.env --concurrency=10
```