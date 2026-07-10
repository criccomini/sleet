# Changelog

All notable changes to Sleet are documented in this file.

## 0.1.0 - 2026-07-10

Initial release.

- Coordinate garbage collection, compaction coordinators, and compaction
  workers across a stateless fleet using object storage.
- Mirror SlateDB databases to continuous and periodic targets, retain restore
  points, and restore backups into empty database roots.
- Register databases and inspect fleet placement, queue depth, and mirror lag
  through the CLI.
- Control the same operations through the asynchronous Rust `Fleet` API.
- Resolve object-store credentials and provider options from the process
  environment for fleet roots, databases, and mirror destinations.
- Publish JSON Schemas for configuration, heartbeats, and CLI responses.
