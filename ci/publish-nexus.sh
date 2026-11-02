#!/bin/sh
# publish-nexus.sh - put one release on the Kopens Nexus as common:<artifactId>:<version>.
#
#   ci/publish-nexus.sh <artifactId> <version> <main.jar> [extra.zip|extra.tar.gz ...]
#
# Plain PUTs, because the shell and alpine runners here have no Maven. A release coordinate is
# read back and compared by sha256, so a 200 from the PUT alone never counts as published.
# NEXUS_USERNAME and NEXUS_PASSWORD are project CI variables. DRY_RUN=1 prints the plan only.
set -eu

artifact=$1; version=$2; shift 2
base="${NEXUS_RELEASES_URL:-https://maven.kopens.io/releases/}common/$artifact/$version"
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT

cat > "$work/pom" <<POM
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>common</groupId>
  <artifactId>$artifact</artifactId>
  <version>$version</version>
  <packaging>jar</packaging>
</project>
POM

put() {
    src=$1; name=$2
    if [ "${DRY_RUN:-0}" = 1 ]; then
        echo "[publish] would PUT $base/$name ($(wc -c < "$src") bytes)"
        return 0
    fi
    sha1sum "$src" | cut -d' ' -f1 > "$work/sha1"
    curl --fail --silent --show-error -u "$NEXUS_USERNAME:$NEXUS_PASSWORD" --upload-file "$src" "$base/$name"
    curl --fail --silent --show-error -u "$NEXUS_USERNAME:$NEXUS_PASSWORD" --upload-file "$work/sha1" "$base/$name.sha1"
    curl --fail --silent --show-error -u "$NEXUS_USERNAME:$NEXUS_PASSWORD" -o "$work/back" "$base/$name"
    want=$(sha256sum "$src" | cut -d' ' -f1); got=$(sha256sum "$work/back" | cut -d' ' -f1)
    if [ "$want" != "$got" ]; then
        echo "[publish] FAIL $name read back as $got, built $want" >&2
        exit 1
    fi
    echo "[publish] $name sha256=$want"
}

if [ "${DRY_RUN:-0}" != 1 ] && { [ -z "${NEXUS_USERNAME:-}" ] || [ -z "${NEXUS_PASSWORD:-}" ]; }; then
    echo "[publish] FAIL NEXUS_USERNAME / NEXUS_PASSWORD are not set on this project" >&2
    exit 1
fi

jar=$1; shift
put "$work/pom" "$artifact-$version.pom"
put "$jar" "$artifact-$version.jar"
for extra in "$@"; do
    case "$extra" in
        *.tar.gz) ext=tar.gz ;;
        *.zip) ext=zip ;;
        *) echo "[publish] FAIL unknown extra: $extra" >&2; exit 1 ;;
    esac
    put "$extra" "$artifact-$version.$ext"
done
echo "[publish] common:$artifact:$version published"
