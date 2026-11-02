#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Assert the built jar carries the commit it was built from.

build.xml's _get-git-sha fails silently (failonerror=false, failifexecutionfails=false): when git
is missing from the build image the jar is still produced, but its Implementation-Git-SHA reads
"Unknown". A jar stamped that way cannot be traced back from a deployed node to a commit, so this
check turns that silent fallback into a build failure.
"""
import re
import sys
import zipfile

MANIFEST = "META-INF/MANIFEST.MF"


def manifest_attribute(jar_path, name):
    with zipfile.ZipFile(jar_path) as jar:
        text = jar.read(MANIFEST).decode("utf-8", "replace")
    # A manifest wraps lines at 72 bytes and continues them with a single leading space.
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\n ", "")
    match = re.search(r"^%s:\s*(.*)$" % re.escape(name), text, re.MULTILINE)
    return match.group(1).strip() if match else ""


def main():
    if len(sys.argv) != 3:
        print("usage: check-jar-git-sha.py <jar> <expected-sha>", file=sys.stderr)
        return 2
    jar_path, expected = sys.argv[1], sys.argv[2].strip()
    stamped = manifest_attribute(jar_path, "Implementation-Git-SHA")
    print("Implementation-Git-SHA=%s (expected %s)" % (stamped or "<absent>", expected or "<unset>"))

    if not stamped or stamped == "Unknown":
        print("ERROR: the jar carries no commit. Is git missing from the build image?", file=sys.stderr)
        return 1
    if not re.fullmatch(r"[0-9a-f]{7,40}", stamped):
        print("ERROR: %r is not a git object name." % stamped, file=sys.stderr)
        return 1
    if not expected:
        # Outside CI there is nothing to compare against; a well-formed stamp is enough.
        print("No expected sha given; the stamp is well formed.")
        return 0
    # git describe may abbreviate, so compare on the shorter of the two.
    if not expected.startswith(stamped) and not stamped.startswith(expected):
        print("ERROR: the jar was built from %s, but this pipeline runs %s." % (stamped, expected),
              file=sys.stderr)
        return 1
    print("The jar carries this pipeline's commit.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
