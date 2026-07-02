#!/bin/sh
# Run the sleet test suite on Linux in Docker. Cargo caches persist in
# named volumes so reruns are fast. Docker-dependent tests (MinIO)
# skip themselves inside the container.
set -eu
cd "$(dirname "$0")/.."
exec docker run --rm \
  -v "$PWD":/src \
  -v sleet-cargo-registry:/usr/local/cargo/registry \
  -v sleet-cargo-target:/target \
  -e CARGO_TARGET_DIR=/target \
  -w /src \
  rust:1.89 \
  cargo test --locked
