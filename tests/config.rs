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
    assert_eq!(r.services, Service::ALL.to_vec());
    // Mirror runs by default but is a no-op with zero targets.
    assert!(r.mirror.targets.is_empty());
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

#[test]
fn mirror_targets_parse_layer_and_travel_together() {
    use sleet::config::{CopierKind, MirrorMode};
    let fleet = config(
        r#"
        [database.mirror.targets.dr]
        url = "s3://dr-bucket/mirrors"
        source_prefix = "s3://user-data"
        mode = "continuous"
        copier = "builtin"
        poll = "10s"
        "#,
    );
    let resolved = fleet.resolve(None);
    let dr = &resolved.mirror.targets["dr"];
    assert_eq!(dr.url.as_deref(), Some("s3://dr-bucket/mirrors"));
    assert_eq!(dr.source_prefix.as_deref(), Some("s3://user-data"));
    assert_eq!(dr.mode, MirrorMode::Continuous);
    assert_eq!(dr.poll, Duration::from_secs(10));
    assert_eq!(dr.checkpoint_lifetime, Duration::from_secs(900), "default");

    // The registry file opts out of dr, adds a periodic backup with
    // retention, and repoints a replica with a bare url: the inherited
    // source_prefix must NOT survive (url and source_prefix travel
    // together).
    let db = database(
        &fleet,
        r#"
        [mirror.targets.dr]
        disabled = true

        [mirror.targets.backup]
        url = "gs://backups/db1"
        mode = "periodic"
        interval = "24h"
        copier = "external"

        [mirror.targets.backup.retention]
        keep = "30d"
        "#,
    );
    let resolved = fleet.resolve(Some(&db));
    assert!(resolved.mirror.targets["dr"].disabled);
    // Non-travel fields still fall through on the disabled target.
    assert_eq!(
        resolved.mirror.targets["dr"].url.as_deref(),
        Some("s3://dr-bucket/mirrors")
    );
    let backup = &resolved.mirror.targets["backup"];
    assert_eq!(backup.mode, MirrorMode::Periodic);
    assert_eq!(backup.copier, CopierKind::External);
    assert_eq!(backup.interval, Duration::from_secs(24 * 3600));
    assert_eq!(backup.keep, Some(Duration::from_secs(30 * 24 * 3600)));
    assert_eq!(backup.source_prefix, None);

    // A db layer that sets only url clears an inherited prefix.
    let repointed = database(
        &fleet,
        "[mirror.targets.dr]\nurl = \"s3://elsewhere/db1\"\n",
    );
    let dr = &fleet.resolve(Some(&repointed)).mirror.targets["dr"];
    assert_eq!(dr.url.as_deref(), Some("s3://elsewhere/db1"));
    assert_eq!(
        dr.source_prefix, None,
        "url and source_prefix travel together"
    );
}

#[test]
fn mirror_target_validation_rejects_bad_fields() {
    let msg = config_errors("[database.mirror.targets.dr]\nmode = \"continuous\"");
    assert!(msg.contains("url is required"), "{msg}");

    let msg = config_errors("[database.mirror.targets.dr]\nurl = \"ftp://nope/x\"");
    assert!(msg.contains("scheme"), "{msg}");

    let msg = config_errors("[database.mirror.targets.dr]\nurl = \"s3://ok/x\"\npoll = \"0s\"");
    assert!(msg.contains("poll"), "{msg}");

    let msg =
        config_errors("[database.mirror.targets.dr]\nurl = \"s3://ok/x\"\ncopy_parallelism = 0");
    assert!(msg.contains("copy_parallelism"), "{msg}");

    let msg = config_errors(
        "[database.mirror.targets.dr]\nurl = \"s3://ok/x\"\n[database.mirror.targets.dr.retention]\nkeep = \"0s\"",
    );
    assert!(msg.contains("retention.keep"), "{msg}");

    // Target names key placement and the pin checkpoint: same charset
    // as node ids.
    let msg = config_errors("[database.mirror.targets.\"bad name\"]\nurl = \"s3://ok/x\"");
    assert!(msg.contains("1-128 chars"), "{msg}");

    // A disabled target needs no url.
    config("[database.mirror.targets.dr]\ndisabled = true");
}

#[test]
fn mirror_validation_checks_the_layered_result() {
    // The fleet layer alone is valid (disabled); the db layer enables
    // it without supplying a url anywhere: only the resolved
    // combination is invalid.
    let fleet = config("[database.mirror.targets.dr]\ndisabled = true");
    let msg = sleet::config::parse_database(&fleet, "[mirror.targets.dr]\ndisabled = false")
        .expect_err("enabled target without url")
        .to_string();
    assert!(msg.contains("url is required"), "{msg}");
}
