#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Wrapper script that runs the command-line provided as its arguments after
# setting up the environment required for the daemon processes to run.
#
# Supported environment variables:
# JAVA_TOOL_OPTIONS: additional options passed to any embedded JVMs. Can be used, e.g.
#                    to set a max heap size with JAVA_TOOL_OPTIONS="-Xmx4g".

export IMPALA_HOME=/opt/impala

# Add directories containing dynamic libraries required by the daemons that
# are not on the system library paths.
export LD_LIBRARY_PATH=/opt/impala/lib
LD_LIBRARY_PATH+=:/usr/lib/jvm/jre-1.8.0/lib/amd64/server/

# Add directory with optional plugins that can be mounted for the container.
LD_LIBRARY_PATH+=:/opt/impala/lib/plugins

# Configs should be first on classpath
export CLASSPATH=/opt/impala/conf
# Append all of the jars in /opt/impala/lib to the classpath.
for jar in /opt/impala/lib/*.jar
do
  CLASSPATH+=:$jar
done
# Add atlas plugin implementation jars to the classpath. Atlas has
# its own class loader and expects the atlas-impala-plugin-impl
# directory by itself without any wildcards or individual jars listed.
CLASSPATH+=:/opt/impala/lib/atlas-impala-plugin-impl
echo "CLASSPATH: $CLASSPATH"
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"

# Default to 2GB heap. Allow overriding by externally-set JAVA_TOOL_OPTIONS.
export JAVA_TOOL_OPTIONS="-Xmx2g $JAVA_TOOL_OPTIONS"

# Various Hadoop libraries depend on having a username. If we're running under
# an unknown username, create an entry in the password file for this user.
if ! whoami ; then
  export USER=${HADOOP_USER_NAME:-dummyuser}
  echo "${USER}:x:$(id -u):$(id -g):,,,:/opt/impala:/bin/bash" >> /etc/passwd
  whoami
  cat /etc/passwd
fi

# =========================== HACK FOR DWX =======================================
# Currently only comms with HMS, Ranger and Atlas are kerberized. Impala only
# maintains the kinit cache when kerberos is enabled across the board. That does
# not work here because the internal connections will start failing (between
# executor <-> coordinator or coordinator <-> statestore etc). This hack essentially
# does the first time kinit with the supplied PRINCIPAL and KEYTAB and the subsequent
# renewal is handled by UGI class from Hadoop *hopefully* (since all the Hadoop based
# clients rely on UGI class and there is an auto renewal thread that maintains the tgt
# cache for us)
# This is a horrible hack and we need to figure out a way to have a tgt-renewer
# as a sidecar to the daemons.
if [ "${USE_KERBEROS}" == "true" ]; then
  SERVICE_KEYTAB=${SERVICE_KEYTAB:?SERVICE_KEYTAB is required for kinit}
  SERVICE_PRINCIPAL=${SERVICE_PRINCIPAL:?SERVICE_PRINCIPAL is required for kinit}
  kinit -V -k -t ${SERVICE_KEYTAB} ${SERVICE_PRINCIPAL}
  klist -e
fi

exec "$@"
