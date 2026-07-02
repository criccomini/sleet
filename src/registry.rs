//! The database registry: `dbs/<db>.toml` files at the fleet root.
//!
//! A registry file's name is the percent-encoded canonical database URL
//! plus `.toml`, so the filename alone identifies the database and an
//! empty file is valid. `register` canonicalizes URLs before encoding so
//! one database cannot be registered under two spellings.

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};

/// URL schemes accepted for databases, matching `object_store`.
const URL_SCHEMES: &[&str] = &[
    "s3", "s3a", "gs", "az", "adl", "azure", "abfs", "abfss", "file", "memory", "http", "https",
];

/// Everything except RFC 3986 unreserved characters is percent-encoded,
/// so encoded names contain no `/` and survive as object keys.
const ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Object stores cap keys at 1024 bytes; registry names must leave room
/// for the fleet root prefix and `dbs/`.
const MAX_FILE_NAME: usize = 900;

/// A URL that cannot be canonicalized, or a database URL that cannot
/// be registered.
#[derive(Debug, thiserror::Error)]
pub enum UrlError {
    /// The URL does not parse.
    #[error("invalid URL {url:?}: {source}")]
    Invalid {
        /// The URL as given.
        url: String,
        /// The parse failure.
        source: url::ParseError,
    },
    /// The URL's scheme is not an object-store scheme.
    #[error("unsupported URL scheme {scheme:?} (expected one of {})", URL_SCHEMES.join(", "))]
    UnsupportedScheme {
        /// The rejected scheme.
        scheme: String,
    },
    /// The database URL's registry file name exceeds the length cap.
    #[error(
        "database URL too long: its registry file name is {len} bytes \
         (max {MAX_FILE_NAME}; object-store keys cap at 1024)"
    )]
    TooLong {
        /// The encoded file name's length in bytes.
        len: usize,
    },
}

/// Canonicalize an object-store URL: lowercase scheme and host, drop
/// trailing slashes. This keeps each location to a single spelling.
/// Fleet roots and database URLs both go through this.
pub fn canonicalize_url(url: &str) -> Result<String, UrlError> {
    let mut parsed = url::Url::parse(url).map_err(|source| UrlError::Invalid {
        url: url.into(),
        source,
    })?;
    if !URL_SCHEMES.contains(&parsed.scheme()) {
        return Err(UrlError::UnsupportedScheme {
            scheme: parsed.scheme().into(),
        });
    }
    // The url crate lowercases the scheme but leaves hosts of non-special
    // schemes (s3://, gs://, ...) as written; fold them ourselves.
    if let Some(host) = parsed.host_str()
        && host.chars().any(|c| c.is_ascii_uppercase())
    {
        let lower = host.to_ascii_lowercase();
        parsed
            .set_host(Some(&lower))
            .map_err(|source| UrlError::Invalid {
                url: url.into(),
                source,
            })?;
    }
    Ok(parsed.as_str().trim_end_matches('/').to_string())
}

/// Canonicalize a database URL and check that it fits a registry file
/// name. Database paths use this; the length cap does not apply to
/// fleet roots, which never become registry names.
pub fn canonicalize_database_url(url: &str) -> Result<String, UrlError> {
    let canonical = canonicalize_url(url)?;
    let len = file_name(&canonical).len();
    if len > MAX_FILE_NAME {
        return Err(UrlError::TooLong { len });
    }
    Ok(canonical)
}

/// The registry file name for a canonical database URL.
pub fn file_name(canonical_url: &str) -> String {
    format!("{}.toml", utf8_percent_encode(canonical_url, ENCODE_SET))
}

/// The canonical database URL a registry file name encodes, if valid.
pub fn parse_file_name(name: &str) -> Option<String> {
    let encoded = name.strip_suffix(".toml")?;
    let url = percent_decode_str(encoded).decode_utf8().ok()?;
    Some(url.into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalization_is_idempotent_and_case_folds() {
        let c = canonicalize_url("S3://Bucket/Path/db/").unwrap();
        assert_eq!(c, "s3://bucket/Path/db");
        assert_eq!(canonicalize_url(&c).unwrap(), c);
    }

    #[test]
    fn aliases_collapse_to_one_spelling() {
        let a = canonicalize_url("s3://b/db").unwrap();
        let b = canonicalize_url("s3://b/db/").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn bad_urls_are_rejected() {
        assert!(canonicalize_url("not a url").is_err());
        assert!(matches!(
            canonicalize_url("ftp://host/db"),
            Err(UrlError::UnsupportedScheme { .. })
        ));
    }

    #[test]
    fn file_names_roundtrip() {
        let url = "s3://bucket/tenants/acme db";
        let name = file_name(url);
        assert!(!name.contains('/'), "{name}");
        assert_eq!(name, "s3%3A%2F%2Fbucket%2Ftenants%2Facme%20db.toml");
        assert_eq!(parse_file_name(&name).as_deref(), Some(url));
    }

    #[test]
    fn non_registry_names_are_ignored() {
        assert_eq!(parse_file_name("README.md"), None);
        assert_eq!(parse_file_name("s3%FF.toml"), None);
    }

    #[test]
    fn oversized_database_urls_are_rejected() {
        let long = format!("s3://bucket/{}", "x".repeat(1000));
        assert!(matches!(
            canonicalize_database_url(&long),
            Err(UrlError::TooLong { .. })
        ));
        // Percent-encoding expansion counts: 300 spaces encode 3x.
        let expansive = format!("s3://bucket/{}", "µ".repeat(400));
        assert!(matches!(
            canonicalize_database_url(&expansive),
            Err(UrlError::TooLong { .. })
        ));
        // The cap is a registry concern; plain canonicalization has none.
        assert!(canonicalize_url(&long).is_ok());
    }
}
