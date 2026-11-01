#!/usr/bin/env bash
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

# Upgrade boot test: does a release candidate start on cluster metadata an EARLIER build wrote?
#
# Every other gate starts fresh clusters, and on 2026-11-01 that is exactly how v6.0.0-r2 passed them
# and then could not boot node 41: upstream changed the TCM metadata layout without a version bump.
# This boots the candidate on a copy of an existing node's system metadata, with that node's IP and
# cassandra.yaml, waits for the node to rewrite the snapshot in its own layout (it does that by itself after
# reading a legacy one) and restarts, so both "reads the old layout" and "reads what it writes" are proven.
#
# usage: docker/upgrade-boot-test.sh <image> <metadata-dir> <cassandra.yaml> <node-ip> [<jar>]
#   <metadata-dir>  a COPY of the node's data dir holding system, system_schema, system_cluster_metadata
#                   (+ system_auth, system_distributed); it is copied again here and never modified
#   <node-ip>       the node's listen_address (its identity in the metadata); an isolated docker network
#                   on its /24 is created, nothing is published
#   <jar>           optional main jar to mount over the image's, to test a build without an image
#
# Taking the metadata copy from a production node is read-only (tar of the dirs above); see
# .claude/skills/docker-integration-test for the node 41 procedure.

set -uo pipefail

IMAGE="${1:?image}"; META="${2:?metadata dir}"; YAML="${3:?cassandra.yaml}"; IP="${4:?node ip}"; JAR="${5:-}"
NAME="cassandra-upgrade-boot-$$"
NET="upgrade-boot-net-${IP%.*}"
NET="${NET//./-}"
WORK="$(mktemp -d)"
# UPGRADE_BOOT_LOG=<file> keeps the node's log after the run, for reading a FAIL.
trap 'docker logs "$NAME" > "${UPGRADE_BOOT_LOG:-/dev/null}" 2>&1; docker rm -f "$NAME" >/dev/null 2>&1; rm -rf "$WORK"' EXIT

mkdir -p "$WORK/cassandra/commitlog" "$WORK/cassandra/hints" "$WORK/cassandra/saved_caches" "$WORK/cassandra/cdc_raw"
cp -a "$META" "$WORK/cassandra/data"
# Reuse any network that already owns the subnet (docker refuses overlapping pools).
for n in $(docker network ls -q); do
    if docker network inspect -f '{{range .IPAM.Config}}{{.Subnet}} {{end}}' "$n" 2>/dev/null | grep -qw "${IP%.*}.0/24"; then
        NET="$(docker network inspect -f '{{.Name}}' "$n")"; break
    fi
done
docker network inspect "$NET" >/dev/null 2>&1 || docker network create --subnet "${IP%.*}.0/24" "$NET" >/dev/null

JAR_MOUNT=()
[ -n "$JAR" ] && JAR_MOUNT=(-v "$(realpath "$JAR"):/opt/cassandra/lib/apache-cassandra-6.0.0.jar:ro")

docker run -d --name "$NAME" --network "$NET" --ip "$IP" --memory 6g -e MAX_HEAP_SIZE=2G -e HEAP_NEWSIZE=400M \
    -v "$WORK/cassandra:$(grep -E '^commitlog_directory:' "$YAML" | awk '{print $2}' | xargs dirname)" \
    -v "$(realpath "$YAML"):/opt/cassandra/conf/cassandra.yaml:ro" "${JAR_MOUNT[@]}" \
    --entrypoint /opt/cassandra/bin/cassandra "$IMAGE" -f -R >/dev/null || { echo "FAIL: container did not start"; exit 1; }

# wait_boot <n>: the n-th "Startup complete", or a startup exception, or the container exiting
wait_boot() {
    for _ in $(seq 1 60); do
        [ "$(docker logs "$NAME" 2>&1 | grep -c 'Startup complete')" -ge "$1" ] && return 0
        grep -q 'Exception encountered during startup' <<< "$(docker logs "$NAME" 2>&1)" && return 1
        [ "$(docker inspect -f '{{.State.Status}}' "$NAME")" = running ] || return 1
        sleep 5
    done
    return 1
}

if ! wait_boot 1; then
    echo "FAIL: the candidate does not start on this metadata"
    docker logs "$NAME" 2>&1 | grep -A12 'Exception encountered during startup' | head -20
    exit 1
fi
echo "ok   first boot on the existing metadata"
# Here-strings, not `docker logs | grep -q`: under pipefail, grep -q exiting early SIGPIPEs the writer and
# turns a match into a failure -- the false FAILs the integration test showed on 2026-11-01.
# The WARN MetadataSnapshots logs when it had to read the old layout; not the bare phrase "legacy Accord layout",
# which the rewrite's own INFO line also contains.
LEGACY_READ='written by a build before upstream 4da20e07e8'
grep -q "$LEGACY_READ" <<< "$(docker logs "$NAME" 2>&1)" && echo "     (read a snapshot in the pre-4da20e07e8 layout)"

# A legacy snapshot is rewritten by the node itself once startup has progressed
# (LegacyAccordLayout.rewriteSnapshotIfLegacyWasRead) -- no operator step. Wait for that, then restart.
if grep -q "$LEGACY_READ" <<< "$(docker logs "$NAME" 2>&1)"; then
    for _ in $(seq 1 24); do
        grep -q 'Rewrote the cluster metadata snapshot' <<< "$(docker logs "$NAME" 2>&1)" && break
        sleep 5
    done
    grep -q 'Rewrote the cluster metadata snapshot' <<< "$(docker logs "$NAME" 2>&1)" \
        || { echo "FAIL: the node did not rewrite the legacy snapshot by itself"; docker logs "$NAME" 2>&1 | grep -A5 'Could not rewrite' | head; exit 1; }
    echo "ok   the node rewrote the snapshot in the current layout by itself"
fi
docker restart "$NAME" >/dev/null
if ! wait_boot 2; then
    echo "FAIL: the candidate does not start on the metadata it wrote itself"
    docker logs "$NAME" 2>&1 | grep -A12 'Exception encountered during startup' | tail -20
    exit 1
fi
if [ "$(docker logs "$NAME" 2>&1 | grep -c "$LEGACY_READ")" -gt 1 ]; then
    echo "FAIL: the second boot still needed the legacy layout -- the snapshot was not rewritten"
    exit 1
fi
echo "ok   second boot on the snapshot it rewrote"
echo "PASS"
