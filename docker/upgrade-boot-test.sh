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
# cassandra.yaml, takes a metadata snapshot in the candidate's layout and restarts, so both "reads what the
# earlier build wrote" and "reads what it writes" are proven.
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
# Then prove it reads what it writes: take a metadata snapshot in this build's layout, so the restart loads
# that instead of the one an earlier build wrote.
docker exec "$NAME" /opt/cassandra/bin/nodetool cms snapshot >/dev/null 2>&1 \
    || { echo "FAIL: nodetool cms snapshot failed on the candidate"; exit 1; }
echo "ok   took a metadata snapshot in the candidate's layout"
docker restart "$NAME" >/dev/null
if ! wait_boot 2; then
    echo "FAIL: the candidate does not start on the metadata it wrote itself"
    docker logs "$NAME" 2>&1 | grep -A12 'Exception encountered during startup' | tail -20
    exit 1
fi
echo "ok   second boot on the snapshot it wrote"
echo "PASS"
