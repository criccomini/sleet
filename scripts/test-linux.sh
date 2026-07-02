#!/bin/sh
# Run the sleet test suite on Linux in Docker. Cargo caches persist in
# named volumes so reruns are fast. The MinIO test skips itself: no
# SLEET_S3_ENDPOINT is set inside the container. Debug info is
# disabled and build jobs are capped so the linker fits in Docker's
# memory limit.
set -eu
cd "$(dirname "$0")/.."
exec docker run --rm \
  -v "$PWD":/src \
  -v sleet-cargo-registry:/usr/local/cargo/registry \
  -v sleet-cargo-target:/target \
  -e CARGO_TARGET_DIR=/target \
  -e CARGO_PROFILE_DEV_DEBUG=0 \
  -e CARGO_PROFILE_TEST_DEBUG=0 \
  -e CARGO_BUILD_JOBS=2 \
  -w /src \
  rust:1.89 \
  cargo test --locked
