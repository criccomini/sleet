use std::path::Path;
use std::time::Duration;

use sleet::config::{self, DatabaseConfig, Service, SleetConfig};

fn config(s: &str) -> SleetConfig {
    config::parse_config(s).expect("config parses and validates")
}

fn config_errors(s: &str) -> String {
    let config: SleetConfig = toml::from_str(s).expect("config parses");
    config
        .validate()
        .expect_err("config is invalid")
        .to_string()
}

fn database(fleet: &SleetConfig, s: &str) -> DatabaseConfig {
    config::parse_database(fleet, s).expect("database file parses and validates")
}

fn read(path: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
    std::fs::read_to_string(path).expect("example file reads")
}

#[test]
fn example_configs_load() {
    let fleet = config(&read("examples/sleet.toml"));
    database(&fleet, &read("examples/db.toml"));
}

#[test]
fn empty_config_resolves_to_builtin_defaults() {
    let r = config("").resolve(None);
    assert_eq!(
        r.services,
        vec![
            Service::Gc,
            Service::CompactorCoordinator,
            Service::CompactionWorkers
        ]
    );
    // SlateDB GC defaults; WAL fence GC dry-runs.
    assert_eq!(r.gc.manifest.interval, Duration::from_secs(60));
    assert_eq!(r.gc.manifest.min_age, Duration::from_secs(300));
    assert!(!r.gc.manifest.dry_run);
    assert!(r.gc.wal_fence.dry_run);
    // SlateDB compactor defaults.
    assert_eq!(r.coordinator.poll_interval, Duration::from_secs(5));
    assert_eq!(
        r.coordinator.worker_heartbeat_timeout,
        Duration::from_secs(30)
    );
    assert_eq!(r.coordinator.scheduler.max_compaction_sources, 8);
    // SlateDB worker defaults, count=1.
    assert_eq!(r.workers.count, 1);
    assert_eq!(r.workers.max_sst_size, 256 * 1024 * 1024);
    assert_eq!(r.workers.compression_codec, None);
}

#[test]
fn precedence_layers_apply_in_order() {
    let fleet = config(
        r#"
        [database]
        services = ["gc"]
        [database.compaction-workers]
        count = 2
        "#,
    );
    let db = database(
        &fleet,
        r#"
        [compaction-workers]
        count = 4
        "#,
    );
    // No registry overrides: the [database] table applies.
    assert_eq!(fleet.resolve(None).workers.count, 2);
    // The registry file wins per field.
    assert_eq!(fleet.resolve(Some(&db)).workers.count, 4);
    // Unset fields fall through: services comes from [database].
    assert_eq!(fleet.resolve(Some(&db)).services, vec![Service::Gc]);
    // Fields unset at every layer keep built-ins.
    assert_eq!(
        fleet.resolve(Some(&db)).workers.compactions_poll_interval,
        Duration::from_secs(5)
    );
}

#[test]
fn empty_registry_file_means_fleet_config() {
    let fleet = config("[database]\nservices = [\"gc\"]");
    let db = database(&fleet, "");
    assert_eq!(fleet.resolve(Some(&db)), fleet.resolve(None));
}

#[test]
fn empty_services_disables_a_database() {
    let fleet = config("");
    let db = database(&fleet, "services = []");
    assert!(fleet.resolve(Some(&db)).services.is_empty());
}

#[test]
fn unknown_fields_are_rejected() {
    assert!(toml::from_str::<SleetConfig>("[node]\ntimeout = \"30s\"").is_err());
    assert!(toml::from_str::<SleetConfig>("[database.gc]\nwals = {}").is_err());
    assert!(toml::from_str::<DatabaseConfig>("url = \"s3://b/db\"").is_err());
}

#[test]
fn heartbeat_interval_must_be_below_heartbeat_timeout() {
    let msg = config_errors(
        r#"
        [node]
        heartbeat_interval = "30s"
        heartbeat_timeout = "30s"
        "#,
    );
    assert!(msg.contains("heartbeat_interval"), "{msg}");
}

#[test]
fn config_poll_must_be_positive() {
    let msg = config_errors("[node]\nconfig_poll = \"0s\"");
    assert!(msg.contains("config_poll"), "{msg}");
}

#[test]
fn zero_workers_are_rejected() {
    let msg = config_errors("[database.compaction-workers]\ncount = 0");
    assert!(msg.contains("compaction-workers.count"), "{msg}");
}

#[test]
fn scheduler_bounds_check_the_layered_result() {
    // min comes from the fleet [database] table, max from the registry
    // file; only the resolved combination is invalid.
    let fleet = config(
        r#"
        [database.compactor-coordinator.scheduler]
        min_compaction_sources = 6
        "#,
    );
    let msg = config::parse_database(
        &fleet,
        r#"
        [compactor-coordinator.scheduler]
        max_compaction_sources = 4
        "#,
    )
    .expect_err("layered scheduler bounds are invalid")
    .to_string();
    assert!(msg.contains("min_compaction_sources"), "{msg}");
}
