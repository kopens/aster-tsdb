#!/bin/bash
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
# Cassandra configuration applied by BOTH halves of the tiering benchmark before the node starts.
#
# This file is not executed on the host: scale-test.sh (untiered baseline) and tiering-bench.sh
# (tiered run) each read it and splice it into the container's `bash -c` entrypoint, so the two
# runs cannot drift. They *did* drift once -- tiering-bench.sh was missing native_transport_timeout
# while scale-test.sh set it to 600s, so every tiered query over the 12s default was truncated
# server-side and read as a product regression. Keeping the block in one file is the fix.
#
# Reads GC_CHOICE from the environment (zgc = as shipped, g1 = the alternative block).
# Everything not touched here is the shipped configuration.

CFG="$CASSANDRA_CONF/cassandra.yaml"

# Aggregating over millions of rows takes longer than the stock request timeouts allow.
sed -ri "s/^read_request_timeout:.*/read_request_timeout: 600000ms/" "$CFG"
sed -ri "s/^range_request_timeout:.*/range_request_timeout: 600000ms/" "$CFG"
sed -ri "s/^write_request_timeout:.*/write_request_timeout: 60000ms/" "$CFG"
sed -ri "s/^request_timeout:.*/request_timeout: 600000ms/" "$CFG"
# native_transport_timeout caps the whole request server-side (default 12s) and is not present in
# the shipped yaml, so it has to be appended rather than substituted.
grep -q "^native_transport_timeout:" "$CFG" \
    && sed -ri "s/^native_transport_timeout:.*/native_transport_timeout: 600s/" "$CFG" \
    || printf "\nnative_transport_timeout: 600s\n" >> "$CFG"

OPTS="$CASSANDRA_CONF/jvm21-server.options"
if [ "${GC_CHOICE:-zgc}" = g1 ]; then
    # Turn the shipped generational-ZGC block off and the G1 block on, and restore compressed oops
    # (the -UseCompressedOops line is a ZGC-only workaround).
    sed -ri "s/^-XX:\+UseZGC/#-XX:+UseZGC/; s/^-XX:\+ZGenerational/#-XX:+ZGenerational/" "$OPTS"
    sed -ri "s/^-XX:-UseCompressedOops/#-XX:-UseCompressedOops/" "$OPTS"
    sed -ri "s/^#(-XX:\+UseG1GC|-XX:\+ParallelRefProcEnabled|-XX:MaxTenuringThreshold=2|-XX:G1HeapRegionSize=16m|-XX:\+UnlockExperimentalVMOptions|-XX:G1NewSizePercent=50|-XX:G1RSetUpdatingPauseTimePercent=5|-XX:MaxGCPauseMillis=300)$/\1/" "$OPTS"
fi
# safepoint logging is what makes the two collectors comparable: it records the actual
# stop-the-world time, which for ZGC is not visible in the plain gc lines.
printf "\n-Xlog:gc*,safepoint:file=%s/logs/gc.log:time,uptime:filecount=0\n" "$CASSANDRA_HOME" >> "$OPTS"
