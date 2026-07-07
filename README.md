# Sleet

Sleet runs background services for fleets of
[SlateDB](https://slatedb.io) databases.

Point a pool of Sleet nodes at one object-store root. Register the SlateDB
database roots you want managed. The nodes will read the registry and split
the work.

Sleet handles:

- garbage collection
- standalone compaction coordinators
- distributed compaction workers
- optional physical mirroring to another bucket

## Quick start

Register a SlateDB database:

```sh
sleet register s3://path/to/sleet/state s3://app-data/db1
```

Start a node:

```sh
sleet run s3://path/to/sleet/state --node-id sleet-1
```

Check fleet status:

```sh
sleet status s3://path/to/sleet/state
```

By default, nodes offer all services:

```text
gc,compactor-coordinator,compaction-workers,mirror
```

You can run specialized nodes with `--services`:

```sh
sleet run s3://path/to/sleet/state \
  --node-id worker-1 \
  --services compaction-workers
```

## Documentation

See the following directories for more information:

- [docs](docs): Sleet documentation.
- [examples](examples): Example configuration files.
- [rfcs](rfcs): Sleet design documents.

## License

MIT
