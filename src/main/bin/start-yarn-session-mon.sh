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
#export FLINK_ENV_JAVA_OPTS="-Dfile.encoding=utf8 --add-exports java.base/sun.net.util=ALL-UNNAMED --add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
#export CLOUD_HOME=$(cd "$(dirname "$0")/../../../.." && pwd)
#export MON_NFS_HOME=$CLOUD_HOME/file/cloud_mon
#export FLINK_CONFIG_FOLDER_NAME=default_lightweight_session_flink-1.17.1_CONFIG
#export FLINK_CONF_DIR=$MON_NFS_HOME/${FLINK_CONFIG_FOLDER_NAME}/flinkconf
#export FLINK_LOG_DIR=$MON_NFS_HOME/${FLINK_CONFIG_FOLDER_NAME}/appwork/logs
$FLINK_HOME/bin/yarn-session-mon.sh \
-qu default  \
-t "$HADOOP_CONF_DIR/" \
-nm "MON-FLINK-SESSION-LIGHT_WEIGHT" \
-at "$YARN_APP_TYPE"