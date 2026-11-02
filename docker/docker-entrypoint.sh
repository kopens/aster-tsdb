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
# Minimal Testcontainers-compatible entrypoint: configures cassandra.yaml for a single-node container.
# Of the CASSANDRA_* environment variables handled by the official cassandra image, only the ones actually
# set by Testcontainers' CassandraContainer are supported here: CASSANDRA_ENDPOINT_SNITCH and CASSANDRA_DC
# (HEAP_NEWSIZE / MAX_HEAP_SIZE / JVM_OPTS are consumed natively by cassandra-env.sh).
set -e

CFG="$CASSANDRA_CONF/cassandra.yaml"
NODE_IP="$(hostname -i 2>/dev/null | awk '{print $1}')"
if [ -z "$NODE_IP" ]; then
    NODE_IP="$(getent hosts "$(uname -n)" | awk '{print $1; exit}')"
fi

sed -ri 's/^(# )?listen_address:.*/listen_address: '"$NODE_IP"'/' "$CFG"
sed -ri 's/^(# )?rpc_address:.*/rpc_address: 0.0.0.0/' "$CFG"
if grep -q '^broadcast_rpc_address:' "$CFG"; then
    sed -ri 's/^broadcast_rpc_address:.*/broadcast_rpc_address: '"$NODE_IP"'/' "$CFG"
else
    printf '\nbroadcast_rpc_address: %s\n' "$NODE_IP" >> "$CFG"
fi
# A single-node container seeds itself. CASSANDRA_SEEDS (same name and meaning as the official
# image) points a node at an existing cluster instead -- docker/cluster-test.sh uses it to form a
# real 3-container cluster, which is the only way to exercise streaming between separate JVMs.
sed -ri 's/- seeds:.*/- seeds: "'"${CASSANDRA_SEEDS:-$NODE_IP}"'"/' "$CFG"

if [ -n "$CASSANDRA_ENDPOINT_SNITCH" ]; then
    sed -ri 's/^endpoint_snitch:.*/endpoint_snitch: '"$CASSANDRA_ENDPOINT_SNITCH"'/' "$CFG"
fi
if [ -n "$CASSANDRA_DC" ]; then
    sed -ri 's/^dc=.*/dc='"$CASSANDRA_DC"'/' "$CASSANDRA_CONF/cassandra-rackdc.properties"
    # Make sure GossipingPropertyFileSnitch does not fail on a missing rack definition.
    grep -q '^rack=' "$CASSANDRA_CONF/cassandra-rackdc.properties" \
        || echo 'rack=rack1' >> "$CASSANDRA_CONF/cassandra-rackdc.properties"
fi

# Testcontainers passes -Dcassandra.initial_token=0; align the token configuration with a single token.
if [[ "$JVM_OPTS" == *"cassandra.initial_token"* ]]; then
    sed -ri 's/^(# )?num_tokens:.*/num_tokens: 1/' "$CFG"
    sed -ri 's/^(# )?allocate_tokens_for_local_replication_factor:.*/# allocate_tokens_for_local_replication_factor: 3/' "$CFG"
fi

exec "$CASSANDRA_HOME/bin/cassandra" -f -R
