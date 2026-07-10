#!/bin/sh

set -eu

repository="criccomini/sleet"
release_url="https://github.com/${repository}/releases"

die() {
    printf 'sleet installer: %s\n' "$*" >&2
    exit 1
}

need() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

download() {
    url=$1
    destination=$2
    curl -fsSL "$url" -o "$destination" \
        || die "failed to download $url"
}

need awk
need curl
need grep
need mktemp
need tar

version=${SLEET_VERSION:-}
if [ -z "$version" ]; then
    latest=$(curl -fsSL -o /dev/null -w '%{url_effective}' \
        "${release_url}/latest") \
        || die "failed to resolve the latest release"
    version=${latest##*/}
fi

case "$version" in
    v*) ;;
    *) version="v${version}" ;;
esac

printf '%s\n' "$version" \
    | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z][0-9A-Za-z.-]*)?$' \
    || die "invalid version: $version"

os=$(uname -s)
arch=$(uname -m)
case "${os}:${arch}" in
    Darwin:arm64 | Darwin:aarch64)
        target="aarch64-apple-darwin"
        ;;
    Darwin:x86_64 | Darwin:amd64)
        target="x86_64-apple-darwin"
        ;;
    Linux:x86_64 | Linux:amd64)
        target="x86_64-unknown-linux-gnu"
        ;;
    *)
        die "unsupported platform: ${os} ${arch}"
        ;;
esac

if [ -n "${SLEET_INSTALL_DIR:-}" ]; then
    install_dir=$SLEET_INSTALL_DIR
elif [ -n "${HOME:-}" ]; then
    install_dir="${HOME}/.local/bin"
else
    die "HOME is not set; set SLEET_INSTALL_DIR to an installation directory"
fi

archive="sleet-${version}-${target}.tar.gz"
archive_root="sleet-${version}-${target}"
asset_url="${release_url}/download/${version}"
tmp_dir=
install_tmp=

cleanup() {
    if [ -n "$install_tmp" ]; then
        rm -f "$install_tmp"
    fi
    if [ -n "$tmp_dir" ]; then
        rm -rf "$tmp_dir"
    fi
}
trap cleanup 0
trap 'exit 1' 1 2 3 15

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/sleet-install.XXXXXX") \
    || die "failed to create a temporary directory"

download "${asset_url}/${archive}" "${tmp_dir}/${archive}"
download "${asset_url}/SHA256SUMS" "${tmp_dir}/SHA256SUMS"

expected=$(awk -v file="$archive" \
    '$2 == file || $2 == ("*" file) { print $1; exit }' \
    "${tmp_dir}/SHA256SUMS")
[ -n "$expected" ] || die "release checksums do not contain $archive"

if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "${tmp_dir}/${archive}" | awk '{ print $1 }')
elif command -v shasum >/dev/null 2>&1; then
    actual=$(shasum -a 256 "${tmp_dir}/${archive}" | awk '{ print $1 }')
else
    die "required checksum command not found: sha256sum or shasum"
fi

[ "$actual" = "$expected" ] || die "checksum verification failed for $archive"

tar -xzf "${tmp_dir}/${archive}" -C "$tmp_dir" \
    || die "failed to extract $archive"
binary="${tmp_dir}/${archive_root}/sleet"
[ -f "$binary" ] || die "release archive does not contain the sleet binary"

mkdir -p "$install_dir" || die "failed to create $install_dir"
install_tmp=$(mktemp "${install_dir}/.sleet.tmp.XXXXXX") \
    || die "failed to create a temporary file in $install_dir"
cp "$binary" "$install_tmp" || die "failed to copy sleet into $install_dir"
chmod 0755 "$install_tmp" || die "failed to make sleet executable"
mv "$install_tmp" "${install_dir}/sleet" \
    || die "failed to install sleet into $install_dir"
install_tmp=

printf 'Installed sleet %s to %s/sleet\n' "$version" "$install_dir"
case ":${PATH:-}:" in
    *":${install_dir}:"*) ;;
    *)
        printf 'Add %s to PATH to run sleet.\n' "$install_dir"
        ;;
esac
