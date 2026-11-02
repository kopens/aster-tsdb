# syntax=docker/dockerfile:1
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
# Container image of aster-tsdb (Apache Cassandra 6.0.0 + time-series functions).
# Built as a multi-stage image so CI (kaniko) and local `docker build` need nothing but the repo checkout:
#   docker build -t aster-tsdb:6.0.0 -f docker/Dockerfile .
# The image is Testcontainers-compatible (see docker-entrypoint.sh) and is consumed by e.g. the
# plantpulse-jdbc-cassandra-driver integration tests via -Pcassandra.image=<image>.

FROM eclipse-temurin:21-jdk-jammy AS build
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends ant ant-optional python3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
# Requires the modules/accord submodule to be checked out (CI uses GIT_SUBMODULE_STRATEGY: recursive).
COPY . .
RUN ANT_OPTS=-Xmx4g ant _artifacts-init -Dno-checkstyle=true

FROM eclipse-temurin:21-jre-jammy
# python3 is required by the bundled cqlsh — used by the integration test, by Testcontainers
# wait strategies and by anyone shelling into the container.
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*
ENV CASSANDRA_HOME=/opt/cassandra \
    CASSANDRA_CONF=/opt/cassandra/conf \
    PATH=/opt/cassandra/bin:$PATH

COPY --from=build /src/build/dist/ /opt/cassandra/
COPY docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/docker-entrypoint.sh /opt/cassandra/bin/* \
    && mkdir -p /opt/cassandra/data /opt/cassandra/logs

EXPOSE 9042

ENTRYPOINT ["docker-entrypoint.sh"]
