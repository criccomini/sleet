//! Text rendering for subcommand responses — the human-readable half of
//! the CLI. `--format json` bypasses this layer entirely, so every
//! response type carries data only and presentation lives here.
//!
//! Layout convention: borderless left-aligned tables with uppercase
//! headers, columns padded to their widest cell, two-space gutters, no
//! trailing whitespace, no color. Sections within one response are
//! separated by a blank line. trycmd snapshots in `tests/cmd/` pin the
//! output.

use std::io::{self, Write};

use crate::config::Service;
use crate::response::{RegisterResponse, StatusResponse};

/// Human-readable rendering of a response.
pub trait Render {
    fn render(&self, w: &mut dyn Write) -> io::Result<()>;
}

/// A borderless, left-aligned table.
pub struct Table {
    headers: &'static [&'static str],
    rows: Vec<Vec<String>>,
}

impl Table {
    pub fn new(headers: &'static [&'static str]) -> Self {
        Self {
            headers,
            rows: Vec::new(),
        }
    }

    pub fn row(&mut self, cells: Vec<String>) {
        debug_assert_eq!(cells.len(), self.headers.len());
        self.rows.push(cells);
    }

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
}
