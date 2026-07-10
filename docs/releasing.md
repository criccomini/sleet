# Releasing

Sleet publishes one Cargo package containing the Rust library and the `sleet`
binary. A matching Git tag and GitHub Release carry prebuilt CLI archives.

## Prepare a release

1. Set the version in `Cargo.toml` and update `CHANGELOG.md`.
2. Cut the compatibility corpus for that version:

   ```sh
   UPDATE_CORPUS=1 cargo test --test corpus
   ```

3. Run the release checks:

   ```sh
   cargo fmt --check
   cargo clippy --all-targets --all-features --locked -- -D warnings
   cargo test --locked
   cargo check --locked --no-default-features --lib
   cargo package --locked
   ```

4. Commit and push the release preparation, then wait for every CI job to
   pass on that exact commit.

The tag must be `v<version>` and must match `Cargo.toml`. Pushing it starts
the release workflow, which builds Linux x86-64, macOS x86-64, and macOS
Arm64 archives, writes SHA-256 checksums, and creates the GitHub Release.

## First crates.io release

Crates.io requires the first version of a new crate to be published manually.
From the verified release commit:

```sh
cargo publish --locked
git tag -a v0.1.0 -m "Sleet 0.1.0"
git push origin v0.1.0
```

The `v0.1.0` release workflow intentionally skips its crates.io publishing
job because the version has already been published manually.

## Later releases

After `0.1.0` exists, configure a crates.io trusted publisher with:

- repository: `criccomini/sleet`
- workflow: `release.yml`
- GitHub environment: `release`

Later version tags use GitHub OIDC to publish to crates.io. The protected
`release` environment is the approval boundary, so no long-lived crates.io
token is stored in GitHub.
