use std::path::Path;
use std::time::Duration;

use sleet::spec::{self, FleetSpec, Service};

fn parse(s: &str) -> FleetSpec {
    toml::from_str(s).expect("spec parses")
}

fn parse_ok(s: &str) -> FleetSpec {
    let spec = parse(s);
    spec.validate().expect("spec validates");
    spec
}

fn errors(s: &str) -> String {
    parse(s)
        .validate()
        .expect_err("spec is invalid")
        .to_string()
}

#[test]
fn example_spec_loads() {
    let spec = spec::load(Path::new("examples/fleet.toml")).expect("example loads");
    assert_eq!(spec.discover.len(), 2);
    assert_eq!(spec.database.len(), 2);
}

#[test]
fn empty_spec_resolves_to_builtin_defaults() {
    let r = parse_ok("").resolve("s3://bucket/db");
    assert_eq!(
        r.services,
        vec![Service::Gc, Service::Compactor, Service::Workers]
    );
    // SlateDB GC defaults; WAL fence GC dry-runs.
    assert_eq!(r.gc.manifest.interval, Duration::from_secs(60));
    assert_eq!(r.gc.manifest.min_age, Duration::from_secs(300));
    assert!(!r.gc.manifest.dry_run);
    assert!(r.gc.wal_fence.dry_run);
    // SlateDB compactor defaults.
    assert_eq!(r.compactor.poll_interval, Duration::from_secs(5));
    assert_eq!(
        r.compactor.worker_heartbeat_timeout,
        Duration::from_secs(30)
    );
    assert_eq!(r.compactor.scheduler.max_compaction_sources, 8);
    // SlateDB worker defaults, count=1.
    assert_eq!(r.workers.count, 1);
    assert_eq!(r.workers.max_sst_size, 256 * 1024 * 1024);
    assert_eq!(r.workers.compression_codec, None);
}

#[test]
fn precedence_layers_apply_in_order() {
    let spec = parse_ok(
        r#"
        [defaults]
        services = ["gc"]
        [defaults.workers]
        count = 2

        [[discover]]
        url = "s3://prod/"
        [discover.workers]
        count = 3

        [[database]]
        url = "s3://prod/special"
        [database.workers]
        count = 4
        "#,
    );
    // Not under any root: defaults only.
    assert_eq!(spec.resolve("gs://other/db").workers.count, 2);
    // Under the root: root wins.
    assert_eq!(spec.resolve("s3://prod/db").workers.count, 3);
    // Explicit entry wins over the root.
    assert_eq!(spec.resolve("s3://prod/special").workers.count, 4);
    // Unset fields fall through: services comes from defaults everywhere.
    assert_eq!(
        spec.resolve("s3://prod/special").services,
        vec![Service::Gc]
    );
    // Fields unset at every layer keep built-ins.
    assert_eq!(
        spec.resolve("s3://prod/special").workers.poll_interval,
        Duration::from_secs(5)
    );
}

#[test]
fn longest_matching_root_wins() {
    let spec = parse_ok(
        r#"
        [[discover]]
        url = "s3://prod/"
        [discover.workers]
        count = 2

        [[discover]]
        url = "s3://prod/tenants/"
        [discover.workers]
        count = 5
        "#,
    );
    assert_eq!(spec.resolve("s3://prod/db").workers.count, 2);
    assert_eq!(spec.resolve("s3://prod/tenants/acme").workers.count, 5);
    // Prefix match respects path boundaries.
    assert_eq!(spec.resolve("s3://prod/tenantsx").workers.count, 2);
}

#[test]
fn unknown_fields_are_rejected() {
    assert!(toml::from_str::<FleetSpec>("[fleet]\nnode = \"x\"").is_err());
    assert!(toml::from_str::<FleetSpec>("[defaults.gc]\nwals = {}").is_err());
}

#[test]
fn heartbeat_interval_must_be_below_node_timeout() {
    let msg = errors(
        r#"
        [fleet]
        heartbeat_interval = "30s"
        node_timeout = "30s"
        "#,
    );
    assert!(msg.contains("heartbeat_interval"), "{msg}");
}

#[test]
fn bad_urls_are_rejected() {
    let msg = errors("[[database]]\nurl = \"not a url\"");
    assert!(msg.contains("database[0].url"), "{msg}");
    let msg = errors("[[discover]]\nurl = \"ftp://host/\"");
    assert!(msg.contains("unsupported URL scheme"), "{msg}");
}

#[test]
fn duplicate_databases_are_rejected() {
    let msg = errors(
        r#"
        [[database]]
        url = "s3://b/db"
        [[database]]
        url = "s3://b/db/"
        "#,
    );
    assert!(msg.contains("duplicates"), "{msg}");
}

#[test]
fn bad_globs_are_rejected() {
    let msg = errors(
        r#"
        [[discover]]
        url = "s3://b/"
        exclude = ["a{"]
        "#,
    );
    assert!(msg.contains("glob"), "{msg}");
}

#[test]
fn zero_workers_are_rejected() {
    let msg = errors("[defaults.workers]\ncount = 0");
    assert!(msg.contains("workers.count"), "{msg}");
}

#[test]
fn scheduler_bounds_check_the_layered_result() {
    // min comes from defaults, max from the database entry; only the
    // resolved combination is invalid.
    let msg = errors(
        r#"
        [defaults.compactor.scheduler]
        min_compaction_sources = 6

        [[database]]
        url = "s3://b/db"
        [database.compactor.scheduler]
        max_compaction_sources = 4
        "#,
    );
    assert!(msg.contains("min_compaction_sources"), "{msg}");
}

#[test]
fn node_id_charset_is_enforced() {
    let msg = errors("[fleet]\nnode_id = \"a/b\"");
    assert!(msg.contains("node_id"), "{msg}");
}
