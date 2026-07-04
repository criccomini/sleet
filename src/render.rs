//! Text rendering for subcommand responses: the human-readable half of
//! the CLI. `--format json` bypasses this layer entirely, so response
//! types hold only data; presentation lives here.
//!
//! Layout convention: borderless left-aligned tables with uppercase
//! headers, columns padded to their widest cell, two-space gutters, no
//! trailing whitespace, no color. Sections within one response are
//! separated by a blank line. trycmd snapshots in `tests/cmd/` pin the
//! output.

use std::io::{self, Write};

use crate::config::Service;
use crate::response::{
    MirrorDrillResponse, MirrorPrefixesResponse, MirrorRestoreResponse, MirrorStatus,
    MirrorSyncResponse, MirrorVerifyResponse, RegisterResponse, StatusResponse,
};

/// Human-readable rendering of a response.
pub trait Render {
    /// Write the response as text to `w`.
    fn render(&self, w: &mut dyn Write) -> io::Result<()>;
}

/// A borderless, left-aligned table.
pub struct Table {
    headers: &'static [&'static str],
    rows: Vec<Vec<String>>,
}

impl Table {
    /// An empty table with these column headers.
    pub fn new(headers: &'static [&'static str]) -> Self {
        Self {
            headers,
            rows: Vec::new(),
        }
    }

    /// Append a row; `cells` must match the header count.
    pub fn row(&mut self, cells: Vec<String>) {
        debug_assert_eq!(cells.len(), self.headers.len());
        self.rows.push(cells);
    }

    /// Write the table, columns padded to their widest cell.
    pub fn write(&self, w: &mut dyn Write) -> io::Result<()> {
        let mut widths: Vec<usize> = self.headers.iter().map(|h| h.len()).collect();
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                widths[i] = widths[i].max(cell.len());
            }
        }
        let write_row = |w: &mut dyn Write, cells: &[&str]| -> io::Result<()> {
            let last = cells.len() - 1;
            for (i, cell) in cells.iter().enumerate() {
                if i == last {
                    writeln!(w, "{cell}")?;
                } else {
                    write!(w, "{cell:<width$}  ", width = widths[i])?;
                }
            }
            Ok(())
        };
        write_row(w, self.headers)?;
        for row in &self.rows {
            let cells: Vec<&str> = row.iter().map(String::as_str).collect();
            write_row(w, &cells)?;
        }
        Ok(())
    }
}

fn join_services(services: &[Service]) -> String {
    services
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

impl Render for StatusResponse {
    fn render(&self, w: &mut dyn Write) -> io::Result<()> {
        let dash = || "-".to_string();
        let mut nodes = Table::new(&["NODE", "LIVE", "HEARTBEAT", "SERVICES", "SLEET", "SLATEDB"]);
        for n in &self.nodes {
            nodes.row(vec![
                n.node_id.clone(),
                if n.live { "yes" } else { "no" }.into(),
                humantime::format_duration(n.heartbeat_age.0).to_string(),
                join_services(&n.services),
                n.sleet_version.clone().unwrap_or_else(dash),
                n.slatedb_version.clone().unwrap_or_else(dash),
            ]);
        }
        nodes.write(w)?;
        writeln!(w)?;

        let with_queues = self.databases.iter().any(|db| db.queue.is_some());
        let mut services = Table::new(if with_queues {
            &["DATABASE", "SERVICE", "NODES", "QUEUE"][..]
        } else {
            &["DATABASE", "SERVICE", "NODES"][..]
        });
        for db in &self.databases {
            for s in &db.services {
                let mut row = vec![
                    db.url.clone(),
                    s.service.as_str().into(),
                    if s.nodes.is_empty() {
                        dash()
                    } else {
                        s.nodes.join(",")
                    },
                ];
                if with_queues {
                    row.push(match &db.queue {
                        Some(q) => format!("{} waiting, {} running", q.claimable, q.running),
                        None => dash(),
                    });
                }
                services.row(row);
            }
        }
        services.write(w)?;

        if !self.mirrors.is_empty() {
            writeln!(w)?;
            let mut mirrors = Table::new(&[
                "DATABASE",
                "TARGET",
                "DESTINATION",
                "MANIFESTS",
                "WAL",
                "SECONDS",
                "VERIFIED",
            ]);
            let behind = |v: &Option<u64>| v.map_or_else(dash, |n| n.to_string());
            let verified = |m: &MirrorStatus| match (m.verify_ok, &m.verified_age) {
                (Some(ok), Some(age)) => format!(
                    "{} {} ago",
                    if ok { "ok" } else { "FAIL" },
                    humantime::format_duration(age.0)
                ),
                _ => dash(),
            };
            for m in &self.mirrors {
                mirrors.row(vec![
                    m.database.clone(),
                    m.target.clone(),
                    m.destination.clone(),
                    behind(&m.manifests_behind),
                    behind(&m.wal_behind),
                    behind(&m.seconds_behind),
                    verified(m),
                ]);
            }
            mirrors.write(w)?;
            for m in &self.mirrors {
                if let Some(error) = &m.error {
                    writeln!(w, "error: {} target {}: {error}", m.database, m.target)?;
                }
            }
        }

        if !self.warnings.is_empty() {
            writeln!(w)?;
            for warning in &self.warnings {
                writeln!(w, "warning: {warning}")?;
            }
        }
        Ok(())
    }
}

impl Render for RegisterResponse {
    fn render(&self, w: &mut dyn Write) -> io::Result<()> {
        if self.created {
            writeln!(w, "registered {} at {}", self.url, self.file)
        } else {
            writeln!(w, "{} already registered at {}", self.url, self.file)
        }
    }
}

impl Render for MirrorSyncResponse {
    fn render(&self, w: &mut dyn Write) -> io::Result<()> {
        if !self.committed {
            return writeln!(
                w,
                "{} target {} is caught up at manifest {} ({})",
                self.database, self.target, self.head, self.destination
            );
        }
        writeln!(
            w,
            "synced {} target {} to manifest {} ({}): {} manifests, {} objects, {} bytes",
            self.database,
            self.target,
            self.head,
            self.destination,
            self.manifests_committed,
            self.objects_copied,
            self.bytes_copied
        )?;
        if self.pruned_manifests > 0 || self.pruned_objects > 0 {
            writeln!(
                w,
                "pruned {} manifests, {} objects",
                self.pruned_manifests, self.pruned_objects
            )?;
        }
        Ok(())
    }
}

impl Render for MirrorVerifyResponse {
    fn render(&self, w: &mut dyn Write) -> io::Result<()> {
        let mut points = Table::new(&["RESTORE POINT", "OBJECTS", "OK"]);
        for p in &self.points {
            points.row(vec![
                p.manifest_id.to_string(),
                p.objects.to_string(),
                if p.problems.is_empty() { "yes" } else { "no" }.into(),
            ]);
        }
        points.write(w)?;
        let problems: Vec<(u64, &String)> = self
            .points
            .iter()
            .flat_map(|p| p.problems.iter().map(move |x| (p.manifest_id, x)))
            .collect();
        if !problems.is_empty() {
            writeln!(w)?;
            for (id, problem) in problems {
                writeln!(w, "problem: restore point {id}: {problem}")?;
            }
        }
        writeln!(w)?;
        let deep = if self.deep { " (deep)" } else { "" };
        if self.ok {
            writeln!(
                w,
                "{} target {} verifies at {}{deep}",
                self.database, self.target, self.destination
            )
        } else {
            writeln!(
                w,
                "{} target {} FAILS verification at {}{deep}",
                self.database, self.target, self.destination
            )
        }
    }
}

impl Render for MirrorRestoreResponse {
    fn render(&self, w: &mut dyn Write) -> io::Result<()> {
        writeln!(
            w,
            "restored {} at manifest {} into {}: {} manifests, {} objects, {} bytes",
            self.backup,
            self.manifest_id,
            self.destination,
            self.manifests_committed,
            self.objects_copied,
            self.bytes_copied
        )
    }
}

impl Render for MirrorDrillResponse {
    fn render(&self, w: &mut dyn Write) -> io::Result<()> {
        writeln!(
            w,
            "drilled {} target {}: manifest {} restored from {} \
             ({} manifests, {} objects, {} bytes)",
            self.database,
            self.target,
            self.manifest_id,
            self.backup,
            self.manifests_committed,
            self.objects_copied,
            self.bytes_copied
        )?;
        writeln!(w, "scanned {} keys, {} bytes", self.keys, self.bytes)?;
        if self.kept {
            writeln!(w, "scratch kept at {}", self.scratch)
        } else {
            writeln!(w, "scratch {} removed", self.scratch)
        }
    }
}

impl Render for MirrorPrefixesResponse {
    fn render(&self, w: &mut dyn Write) -> io::Result<()> {
        // The payload is the service-native configuration snippet;
        // the filter lists ride inside it.
        writeln!(
            w,
            "{}",
            serde_json::to_string_pretty(&self.configuration).expect("configuration serializes")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response::{DatabaseStatus, NodeStatus, QueueStatus, ServicePlacement};
    use std::time::Duration;

    /// A populated status renders exactly this text; trycmd pins the
    /// empty case, this pins the full one deterministically.
    #[test]
    fn populated_status_renders_stably() {
        let response = StatusResponse {
            nodes: vec![NodeStatus {
                node_id: "sleet-1".into(),
                live: true,
                heartbeat_age: Duration::from_secs(2).into(),
                services: vec![Service::Gc, Service::CompactionWorkers],
                sleet_version: Some("0.1.0".into()),
                slatedb_version: None,
            }],
            databases: vec![DatabaseStatus {
                url: "s3://b/db".into(),
                services: vec![
                    ServicePlacement {
                        service: Service::Gc,
                        nodes: vec!["sleet-1".into()],
                    },
                    ServicePlacement {
                        service: Service::CompactorCoordinator,
                        nodes: vec![],
                    },
                ],
                queue: Some(QueueStatus {
                    claimable: 3,
                    running: 1,
                }),
            }],
            mirrors: vec![],
            warnings: vec!["no live node offers compactor-coordinator".into()],
        };
        let mut out = Vec::new();
        response.render(&mut out).unwrap();
        let expected = "\
NODE     LIVE  HEARTBEAT  SERVICES               SLEET  SLATEDB
sleet-1  yes   2s         gc,compaction-workers  0.1.0  -

DATABASE   SERVICE                NODES    QUEUE
s3://b/db  gc                     sleet-1  3 waiting, 1 running
s3://b/db  compactor-coordinator  -        3 waiting, 1 running

warning: no live node offers compactor-coordinator
";
        assert_eq!(String::from_utf8(out).unwrap(), expected);
    }

    /// The mirrors table renders lag columns with dashes for unknowns
    /// and surfaces per-target read errors after the table.
    #[test]
    fn mirror_status_renders_stably() {
        use crate::response::MirrorStatus;
        let response = StatusResponse {
            nodes: vec![],
            databases: vec![],
            mirrors: vec![
                MirrorStatus {
                    database: "s3://b/db".into(),
                    target: "dr".into(),
                    destination: "s3://dr/db".into(),
                    source_manifest_id: Some(12),
                    target_manifest_id: Some(10),
                    manifests_behind: Some(2),
                    wal_behind: Some(7),
                    seconds_behind: Some(31),
                    verified_age: Some(std::time::Duration::from_secs(180).into()),
                    verify_ok: Some(true),
                    verify_problems: Some(0),
                    error: None,
                },
                MirrorStatus {
                    database: "s3://b/db".into(),
                    target: "backup".into(),
                    destination: "gs://backups/db".into(),
                    source_manifest_id: None,
                    target_manifest_id: None,
                    manifests_behind: None,
                    wal_behind: None,
                    seconds_behind: None,
                    verified_age: None,
                    verify_ok: None,
                    verify_problems: None,
                    error: Some("bucket unreachable".into()),
                },
            ],
            warnings: vec![],
        };
        let mut out = Vec::new();
        response.render(&mut out).unwrap();
        let expected = "\
NODE  LIVE  HEARTBEAT  SERVICES  SLEET  SLATEDB

DATABASE  SERVICE  NODES

DATABASE   TARGET  DESTINATION      MANIFESTS  WAL  SECONDS  VERIFIED
s3://b/db  dr      s3://dr/db       2          7    31       ok 3m ago
s3://b/db  backup  gs://backups/db  -          -    -        -
error: s3://b/db target backup: bucket unreachable
";
        assert_eq!(String::from_utf8(out).unwrap(), expected);
    }
}
