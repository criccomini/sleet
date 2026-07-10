#!/bin/sh

set -eu

root=$(CDPATH= cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/sleet-installer-test.XXXXXX")
trap 'rm -rf "$tmp"' 0
trap 'exit 1' 1 2 3 15

version="v9.8.7"
fixtures="${tmp}/fixtures"
fake_bin="${tmp}/bin"
mkdir -p "$fixtures" "$fake_bin" "${tmp}/home"

checksum() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{ print $1 }'
    else
        shasum -a 256 "$1" | awk '{ print $1 }'
    fi
}

for target in \
    aarch64-apple-darwin \
    x86_64-apple-darwin \
    x86_64-unknown-linux-gnu
do
    directory="sleet-${version}-${target}"
    mkdir -p "${fixtures}/${directory}"
    cat > "${fixtures}/${directory}/sleet" <<EOF
#!/bin/sh
printf '%s\n' '${version} ${target}'
EOF
    chmod 0755 "${fixtures}/${directory}/sleet"
    tar -czf "${fixtures}/${directory}.tar.gz" \
        -C "$fixtures" "$directory"
    rm -rf "${fixtures:?}/${directory}"
done

: > "${fixtures}/SHA256SUMS"
for archive in "${fixtures}"/*.tar.gz; do
    printf '%s  %s\n' "$(checksum "$archive")" "${archive##*/}" \
        >> "${fixtures}/SHA256SUMS"
done
awk '{ print "0000000000000000000000000000000000000000000000000000000000000000  " $2 }' \
    "${fixtures}/SHA256SUMS" > "${fixtures}/BAD_SHA256SUMS"

cat > "${fake_bin}/uname" <<'EOF'
#!/bin/sh
case "${1:-}" in
    -s) printf '%s\n' "${TEST_UNAME_S:-Linux}" ;;
    -m) printf '%s\n' "${TEST_UNAME_M:-x86_64}" ;;
    *) exit 1 ;;
esac
EOF

cat > "${fake_bin}/curl" <<'EOF'
#!/bin/sh
set -eu

output=
url=
while [ "$#" -gt 0 ]; do
    case "$1" in
        -o)
            output=$2
            shift 2
            ;;
        -w)
            shift 2
            ;;
        -*)
            shift
            ;;
        *)
            url=$1
            shift
            ;;
    esac
done

case "$url" in
    */releases/latest)
        [ "${TEST_FAIL_LATEST:-0}" = 0 ] || exit 1
        printf '%s' "$TEST_LATEST_URL"
        ;;
    */SHA256SUMS)
        if [ "${TEST_BAD_CHECKSUM:-0}" = 1 ]; then
            cp "${TEST_FIXTURE_DIR}/BAD_SHA256SUMS" "$output"
        else
            cp "${TEST_FIXTURE_DIR}/SHA256SUMS" "$output"
        fi
        ;;
    */*.tar.gz)
        cp "${TEST_FIXTURE_DIR}/${url##*/}" "$output"
        ;;
    *)
        printf 'unexpected URL: %s\n' "$url" >&2
        exit 1
        ;;
esac
EOF
chmod 0755 "${fake_bin}/curl" "${fake_bin}/uname"

export TEST_FIXTURE_DIR="$fixtures"
export TEST_LATEST_URL="https://github.com/criccomini/sleet/releases/tag/${version}"

linux_dir="${tmp}/linux"
PATH="${fake_bin}:$PATH" \
TEST_UNAME_S=Linux \
TEST_UNAME_M=x86_64 \
HOME="${tmp}/home" \
SLEET_INSTALL_DIR="$linux_dir" \
    sh "${root}/install.sh" > "${tmp}/linux.out"
[ -x "${linux_dir}/sleet" ]
[ "$("${linux_dir}/sleet")" = "${version} x86_64-unknown-linux-gnu" ]
grep -q "Installed sleet ${version}" "${tmp}/linux.out"

mac_dir="${tmp}/mac"
PATH="${fake_bin}:$PATH" \
TEST_FAIL_LATEST=1 \
TEST_UNAME_S=Darwin \
TEST_UNAME_M=arm64 \
HOME="${tmp}/home" \
SLEET_INSTALL_DIR="$mac_dir" \
SLEET_VERSION="${version#v}" \
    sh "${root}/install.sh" > "${tmp}/mac.out"
[ "$("${mac_dir}/sleet")" = "${version} aarch64-apple-darwin" ]

if PATH="${fake_bin}:$PATH" \
    TEST_UNAME_S=Linux \
    TEST_UNAME_M=aarch64 \
    HOME="${tmp}/home" \
    SLEET_INSTALL_DIR="${tmp}/unsupported" \
    SLEET_VERSION="$version" \
    sh "${root}/install.sh" > /dev/null 2> "${tmp}/unsupported.err"
then
    printf 'unsupported platform unexpectedly succeeded\n' >&2
    exit 1
fi
grep -q 'unsupported platform: Linux aarch64' "${tmp}/unsupported.err"

if PATH="${fake_bin}:$PATH" \
    TEST_BAD_CHECKSUM=1 \
    TEST_UNAME_S=Linux \
    TEST_UNAME_M=x86_64 \
    HOME="${tmp}/home" \
    SLEET_INSTALL_DIR="${tmp}/bad-checksum" \
    SLEET_VERSION="$version" \
    sh "${root}/install.sh" > /dev/null 2> "${tmp}/checksum.err"
then
    printf 'bad checksum unexpectedly succeeded\n' >&2
    exit 1
fi
grep -q 'checksum verification failed' "${tmp}/checksum.err"
[ ! -e "${tmp}/bad-checksum/sleet" ]

printf 'installer tests passed\n'
