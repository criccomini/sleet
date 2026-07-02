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

use crate::response::{RegisterResponse, StatusResponse};
use crate::spec::Service;

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
        let mut nodes = Table::new(&["NODE", "LIVE", "HEARTBEAT", "SERVICES", "SLEET", "SLATEDB"]);
        for n in &self.nodes {
            nodes.row(vec![
                n.node_id.clone(),
                if n.live { "yes" } else { "no" }.into(),
                humantime::format_duration(n.heartbeat_age.0).to_string(),
                join_services(&n.services),
                n.sleet_version.clone(),
                n.slatedb_version.clone(),
            ]);
        }
        nodes.write(w)?;
        writeln!(w)?;

        let mut services = Table::new(&["DATABASE", "SERVICE", "NODES"]);
        for db in &self.databases {
            for s in &db.services {
                services.row(vec![
                    db.url.clone(),
                    s.service.as_str().into(),
                    if s.nodes.is_empty() {
                        "-".into()
                    } else {
                        s.nodes.join(",")
                    },
                ]);
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
