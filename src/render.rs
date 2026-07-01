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

use crate::response::{DbEditAction, DbEditResponse, DbListResponse, StatusResponse};

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

impl Render for StatusResponse {
    fn render(&self, w: &mut dyn Write) -> io::Result<()> {
        let mut nodes = Table::new(&["NODE", "LIVE", "HEARTBEAT"]);
        for n in &self.nodes {
            nodes.row(vec![
                n.node_id.clone(),
                if n.live { "yes" } else { "no" }.into(),
                humantime::format_duration(n.heartbeat_age.0).to_string(),
            ]);
        }
        nodes.write(w)?;
        writeln!(w)?;

        let mut services = Table::new(&["DATABASE", "SERVICE", "NODE", "STATE"]);
        for db in &self.databases {
            for s in &db.services {
                services.row(vec![
                    db.url.clone(),
                    s.service.as_str().into(),
                    s.node_id.clone(),
                    s.state.as_str().into(),
                ]);
            }
        }
        services.write(w)
    }
}

impl Render for DbListResponse {
    fn render(&self, w: &mut dyn Write) -> io::Result<()> {
        let mut databases = Table::new(&["DATABASE", "SERVICES"]);
        for db in &self.databases {
            let services = match &db.services {
                Some(s) => s.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(","),
                None => "(defaults)".into(),
            };
            databases.row(vec![db.url.clone(), services]);
        }
        databases.write(w)?;

        if !self.roots.is_empty() {
            writeln!(w)?;
            let mut roots = Table::new(&["ROOT", "RESCAN", "MAX_DEPTH"]);
            for r in &self.roots {
                roots.row(vec![
                    r.url.clone(),
                    humantime::format_duration(r.rescan.0).to_string(),
                    r.max_depth.to_string(),
                ]);
            }
            roots.write(w)?;
        }
        Ok(())
    }
}

impl Render for DbEditResponse {
    fn render(&self, w: &mut dyn Write) -> io::Result<()> {
        match (self.action, self.changed) {
            (DbEditAction::Added, true) => writeln!(w, "added {} to {}", self.url, self.spec),
            (DbEditAction::Added, false) => {
                writeln!(w, "{} already in {}; no change", self.url, self.spec)
            }
            (DbEditAction::Removed, true) => {
                writeln!(w, "removed {} from {}", self.url, self.spec)
            }
            (DbEditAction::Removed, false) => {
                writeln!(w, "{} not found in {}; no change", self.url, self.spec)
            }
        }
    }
}
