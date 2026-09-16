#!/usr/bin/env python3
"""Assert the built jar says which product it is.

The jar is published under upstream's own file name (apache-cassandra-6.0.0.jar) so that it drops
into an existing Cassandra installation with nothing else changed. That is deliberate, and it means
the file name can no longer answer "is this stock Apache Cassandra or Aster TSDB?". The manifest
answers it instead, and this check is what keeps the answer from quietly falling off the build.

Upstream leaves these attributes empty, so their presence is the whole signal.

Written in Python rather than `unzip | grep` for two reasons, both measured: the build image
(eclipse-temurin:21-jdk) ships no unzip, and a manifest wraps at 72 bytes and continues with a
leading space - a line-anchored grep silently misses a wrapped value.
"""
import re
import sys
import zipfile

MANIFEST = "META-INF/MANIFEST.MF"


def manifest_text(jar_path):
    with zipfile.ZipFile(jar_path) as jar:
        text = jar.read(MANIFEST).decode("utf-8", "replace")
    # A manifest wraps lines at 72 bytes and continues them with a single leading space.
    return text.replace("\r\n", "\n").replace("\r", "\n").replace("\n ", "")


def attribute(text, name):
    match = re.search(r"^%s:\s*(.*)$" % re.escape(name), text, re.MULTILINE)
    return match.group(1).strip() if match else ""


def main():
    if len(sys.argv) != 3:
        print("usage: check-jar-identity.py <jar> <expected-version>", file=sys.stderr)
        return 2
    jar_path, expected_version = sys.argv[1], sys.argv[2].strip()

    try:
        text = manifest_text(jar_path)
    except (OSError, KeyError, zipfile.BadZipFile) as exc:
        print("[identity] ERROR cannot read %s from %s: %s" % (MANIFEST, jar_path, exc), file=sys.stderr)
        return 1

    # (attribute, expected exact value or None, regex the value must match, why it matters)
    checks = [
        ("Implementation-Title", "Aster TSDB", None,
         "스톡 Apache jar 과 구분되지 않는다"),
        ("Implementation-Vendor", "KOPENS", None,
         "누가 빌드했는지 못 읽는다"),
        ("Implementation-Version", expected_version, None,
         "파일 이름이 약속한 버전과 jar 이 주장하는 버전이 다르다 - 드롭인이 깨진다"),
        ("Aster-Product", None, r".+",
         "제품명이 없다"),
        ("Aster-Upstream", None, r".+",
         "어느 상류 브랜치의 포크인지 못 읽는다"),
        ("Aster-Upstream-SHA", None, r"[0-9a-f]{40}",
         "상류 기준점을 못 읽는다 - 40자 SHA 여야 한다"),
    ]

    failed = 0
    for name, exact, pattern, why in checks:
        value = attribute(text, name)
        if exact is not None:
            good = value == exact
            want = "'%s'" % exact
        else:
            good = bool(re.fullmatch(pattern, value))
            want = "/%s/" % pattern
        if not good:
            print("[identity] FAIL %s=%r, expected %s - %s" % (name, value or "<absent>", want, why),
                  file=sys.stderr)
            failed += 1

    if failed:
        print("[identity] %d attribute(s) wrong - a release jar must say what it is." % failed,
              file=sys.stderr)
        return 1

    print("[identity] OK")
    for line in text.split("\n"):
        if re.match(r"^(Implementation|Aster)-", line):
            print("  " + line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
