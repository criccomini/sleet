//! Exposes the resolved slatedb dependency version as SLATEDB_VERSION,
//! carried in heartbeat bodies and shown by `sleet status`.

fn main() {
    println!("cargo:rerun-if-changed=Cargo.lock");
    let lock = std::fs::read_to_string("Cargo.lock").expect("read Cargo.lock");
    let version = slatedb_version(&lock).expect("slatedb in Cargo.lock");
    println!("cargo:rustc-env=SLATEDB_VERSION={version}");
}

fn slatedb_version(lock: &str) -> Option<&str> {
    let mut in_slatedb = false;
    for line in lock.lines() {
        let line = line.trim();
        if line == "name = \"slatedb\"" {
            in_slatedb = true;
        } else if in_slatedb {
            if let Some(v) = line.strip_prefix("version = \"") {
                return v.strip_suffix('"');
            }
            if line.starts_with("[[") {
                in_slatedb = false;
            }
        }
    }
    None
}
