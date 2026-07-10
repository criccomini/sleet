//! Object-store construction shared by fleet and database roots.

use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use url::Url;

/// Open an object-store URL with the provider options from this
/// process's environment. Non-Unicode variables are skipped, matching
/// the cloud builders' `from_env` behavior.
pub(crate) fn parse_url(
    url: &Url,
) -> Result<(Box<dyn ObjectStore>, StorePath), object_store::Error> {
    let options = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)));
    object_store::parse_url_opts(url, options)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHILD: &str = "SLEET_STORE_ENV_TEST_CHILD";

    /// Run the parser in a child test process so changing an environment
    /// variable cannot race the rest of the test suite. The deliberately
    /// invalid S3 option must reach the builder and fail there.
    #[test]
    fn environment_options_reach_the_store_builder() {
        if std::env::var_os(CHILD).is_some() {
            let url = Url::parse("s3://bucket/prefix").unwrap();
            let error = parse_url(&url).unwrap_err().to_string();
            assert!(
                error.contains("not-a-boolean"),
                "environment option did not reach S3 builder: {error}"
            );
            return;
        }

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "store::tests::environment_options_reach_the_store_builder",
            ])
            .env(CHILD, "1")
            .env("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "not-a-boolean")
            .status()
            .unwrap();
        assert!(status.success(), "child parser test failed: {status}");
    }
}
