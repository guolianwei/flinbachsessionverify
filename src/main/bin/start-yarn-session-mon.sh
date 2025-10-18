#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

SCRIPT_DIR=$(cd ../../ "$(dirname "$0")" && pwd)
  # 计算上两级目录作为 CLOUD_HOME，并导出为环境变量
  # 使用 'export' 使得 CLOUD_HOME 对子进程也可见
export CLOUD_HOME
CLOUD_HOME=$(dirname "$(dirname "${SCRIPT_DIR}")")
export JAVA_HOME=$CLOUD_HOME/jdk/linux/jdk-21.0.2/

if [ ! -d "${JAVA_HOME}" ]; then
    echo "错误：JAVA_HOME 目录不存在于 '${JAVA_HOME}'" >&2
    return 1 # 返回错误码
fi

export FLINK_HOME=$(cd "$(dirname "$0")/.." && pwd)
export FLINK_ENV_JAVA_OPTS="-Dfile.encoding=utf8 --add-exports java.base/sun.net.util=ALL-UNNAMED --add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
export DEFAULT_HADOOP_CONFIG=$CLOUD_HOME/file/cloud_mon/DEFAULT_CONFIG
$FLINK_HOME/bin/yarn-session-mon.sh \
--queue default
--container 1
--slots 2 \
--jobManagerMemory 1024
--taskManagerMemory 1024
--name "MON-FLINK-SESSION-mon-LIGNT_WEIGHT" \
-Dyarn.tags=MON-FLINK-SESSION-mon-LIGNT_WEIGHT \
-Dyarn.provided.lib.dirs="hdfs:///user/tempodata/flink1117tdh8/1.17.1/lib;hdfs:///user/tempodata/df/df08/app-dependency" \
-Dyarn.ship-files="${DEFAULT_HADOOP_CONFIG}/yarn-job-jaas.conf;$DEFAULT_HADOOP_CONFIG/merit-tempodata.keytab;$DEFAULT_HADOOP_CONFIG/core-site.xml;$$DEFAULT_HADOOP_CONFIG/hdfs-site.xml;$$DEFAULT_HADOOP_CONFIG/yarn-site.xml;$DEFAULT_HADOOP_CONFIG/mapred-site.xml;$$DEFAULT_HADOOP_CONFIG/DEFAULT_CONFIG.zip;$DEFAULT_HADOOP_CONFIG/hbase-site.xml;$DEFAULT_HADOOP_CONFIG/hive-site.xml" \
-Denv.java.opts.client="--add-exports java.base/sun.net.util=ALL-UNNAMED --add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-exports java.base/sun.net.util=ALL-UNNAMED --add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED" \
-Denv.java.opts="-Dfile.encoding=utf8 --add-exports java.base/sun.net.util=ALL-UNNAMED --add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED  --add-reads=org.apache.arrow.flight.core=ALL-UNNAMED --add-opens=java.base/java.nio=org.apache.arrow.dataset,org.apache.arrow.memory.core,ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED -Dfile.encoding=utf8 --add-exports java.base/sun.net.util=ALL-UNNAMED --add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED  --add-reads=org.apache.arrow.flight.core=ALL-UNNAMED --add-opens=java.base/java.nio=org.apache.arrow.dataset,org.apache.arrow.memory.core,ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED"