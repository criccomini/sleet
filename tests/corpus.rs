//! Wire-format compatibility corpus: serialized artifacts from each
//! release, parsed and re-verified by the current code. Guards the
//! promises the frozen formats make to mixed-version fleets.
//!
//! Each `tests/corpus/v<version>/` directory holds:
//! - `heartbeat.json`: a heartbeat body; must still deserialize.
//! - `config.toml`, `db.toml`: fleet and registry configs; must still
//!   parse and validate.
//! - `registry-names.tsv`: `canonical-url <tab> file_name`; the
//!   current encoder must produce and decode the same names.
//! - `placement-scores.tsv`: `canonical-url <tab> service <tab> node
//!   <tab> hex`; the current hash must produce identical scores.
//!   Canonical URLs, because placement only ever hashes registry keys,
//!   which are canonicalized on load.
//!
//! Cut a new corpus directory at each release with:
//!
//! ```sh
//! UPDATE_CORPUS=1 cargo test --test corpus
//! ```

use std::path::PathBuf;

use sleet::config::{self, Service};
use sleet::heartbeat::{Heartbeat, ServiceSummary};
use sleet::{placement, registry};

fn corpus_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus")
}

const SAMPLE_URLS: &[&str] = &[
    "s3://bucket/db",
    "s3://bucket/tenants/acme db",
    "gs://analytics/events",
    "file:///var/data/db1",
    "s3://bucket/uni\u{00e7}ode/p\u{00e4}th",
];

fn sample_heartbeat() -> Heartbeat {
    Heartbeat::new(
        "sleet-1",
        "0.14.1",
        vec![
            ServiceSummary {
                service: Service::Gc,
                running: 12,
                backoff: 1,
            },
            ServiceSummary {
                service: Service::CompactionWorkers,
                running: 3,
                backoff: 0,
            },
        ],
    )
}

fn generate() {
    let dir = corpus_root().join(format!("v{}", env!("CARGO_PKG_VERSION")));
    std::fs::create_dir_all(&dir).unwrap();
    let heartbeat = serde_json::to_string_pretty(&sample_heartbeat()).unwrap();
    std::fs::write(dir.join("heartbeat.json"), heartbeat).unwrap();
    for (example, name) in [
        ("examples/sleet.toml", "config.toml"),
        ("examples/db.toml", "db.toml"),
    ] {
        let body = std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(example))
            .unwrap();
        std::fs::write(dir.join(name), body).unwrap();
    }
    let mut names = String::new();
    for url in SAMPLE_URLS {
        let canonical = registry::canonicalize_url(url).unwrap();
        names.push_str(&format!(
            "{canonical}\t{}\n",
            registry::file_name(&canonical)
        ));
    }
    std::fs::write(dir.join("registry-names.tsv"), names).unwrap();
    let mut scores = String::new();
    for url in SAMPLE_URLS {
        let canonical = registry::canonicalize_url(url).unwrap();
        for service in Service::ALL {
            for node in ["sleet-1", "sleet-2"] {
                scores.push_str(&format!(
                    "{canonical}\t{}\t{node}\t{:016x}\n",
                    service.as_str(),
                    placement::score(&canonical, service, node)
                ));
            }
        }
    }
    std::fs::write(dir.join("placement-scores.tsv"), scores).unwrap();
}

fn service_by_name(name: &str) -> Service {
    Service::ALL
        .into_iter()
        .find(|s| s.as_str() == name)
        .unwrap_or_else(|| panic!("unknown service {name:?} in corpus"))
}

/// Every corpus directory (one per past release) must parse and
/// re-verify with the current code.
#[test]
fn corpus_of_every_release_still_parses() {
    if std::env::var_os("UPDATE_CORPUS").is_some() {
        generate();
    }
    let mut versions = 0;
    for entry in std::fs::read_dir(corpus_root()).expect("corpus exists") {
        let dir = entry.unwrap().path();
        if !dir.is_dir() {
            continue;
        }
        versions += 1;
        let read = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap();

        let heartbeat: Heartbeat = serde_json::from_str(&read("heartbeat.json"))
            .unwrap_or_else(|e| panic!("{dir:?} heartbeat: {e}"));
        assert!(!heartbeat.node_id.is_empty());

        let fleet = config::parse_config(&read("config.toml"))
            .unwrap_or_else(|e| panic!("{dir:?} config: {e}"));
        config::parse_database(&fleet, &read("db.toml"))
            .unwrap_or_else(|e| panic!("{dir:?} db config: {e}"));

        for line in read("registry-names.tsv").lines() {
            let (url, name) = line.split_once('\t').unwrap();
            assert_eq!(registry::file_name(url), name, "{dir:?}: encoder changed");
            assert_eq!(
                registry::parse_file_name(name).as_deref(),
                Some(url),
                "{dir:?}: decoder changed"
            );
        }

        for line in read("placement-scores.tsv").lines() {
            let fields: Vec<&str> = line.split('\t').collect();
            let (url, service, node, hex) = (fields[0], fields[1], fields[2], fields[3]);
            let want = u64::from_str_radix(hex, 16).unwrap();
            assert_eq!(
                placement::score(url, service_by_name(service), node),
                want,
                "{dir:?}: the frozen hash changed for {url} {service} {node}"
            );
        }
    }
    assert!(versions >= 1, "corpus must hold at least one release");
}
