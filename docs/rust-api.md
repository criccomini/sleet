# Rust API

The `sleet` crate exposes the same fleet operations as the command-line
interface. The API is asynchronous and uses Tokio. The application owns its
runtime, tracing subscriber, and shutdown signal.

For library-only use, disable the default `cli` feature:

```toml
[dependencies]
sleet = { version = "0.1", default-features = false }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Open a fleet

`Fleet::open` accepts the same object-store URLs as the CLI:

```rust,no_run
use sleet::{Fleet, StatusOptions};

# async fn example() -> Result<(), sleet::Error> {
let fleet = Fleet::open("s3://ops/sleet")?;
fleet.register("s3://data/orders").await?;

let status = fleet
    .status(StatusOptions::default().with_compactions(true))
    .await?;
println!("{} databases", status.databases.len());
# Ok(())
# }
```

Sleet reads object-store credentials and provider options from the process
environment whenever it opens a URL. For AWS, this includes static access
keys, web identity, container credentials, region, and endpoint settings
recognized by the `object_store` crate.

The environment is process-wide. A process must be able to access the fleet
root and every database or mirror destination that it may own. Use separate
fleets or a shared cross-account role when databases require distinct
credential domains.

## Run a node

`run_node` runs until its cancellation token fires. Cancellation stops owned
tasks and deletes the node heartbeat before returning.

```rust,no_run
use sleet::{CancellationToken, Fleet, NodeOptions, Service};

# async fn example() -> Result<(), sleet::Error> {
let fleet = Fleet::open("s3://ops/sleet")?;
let shutdown = CancellationToken::new();

let options = NodeOptions::new("worker-1")
    .with_services([Service::CompactionWorkers]);
fleet.run_node(options, shutdown).await?;
# Ok(())
# }
```

`NodeOptions` defaults to all services and uses the machine's available
parallelism as its mirror-job limit.

## Mirror operations

Run one sync pass for a registered target:

```rust,no_run
# use sleet::{Fleet, MirrorSyncOptions};
# async fn example() -> Result<(), sleet::Error> {
# let fleet = Fleet::open("s3://ops/sleet")?;
let report = fleet
    .sync_mirror(
        "s3://data/orders",
        "backup",
        MirrorSyncOptions::default(),
    )
    .await?;
println!("copied {} objects", report.objects_copied);
# Ok(())
# }
```

Restore a backup into an empty root without opening a fleet:

```rust,no_run
use sleet::{RestorePoint, mirror_restore};

# async fn example() -> Result<(), sleet::Error> {
mirror_restore(
    "s3://backups/orders",
    "s3://restores/orders",
    RestorePoint::Latest,
)
.await?;
# Ok(())
# }
```

The response structures are the same types used for CLI JSON output. Their
wire representation remains defined by `schema/cli.schema.json`.
