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

# Start/stop a Flink daemon.
USAGE="Usage: flink-daemon.sh (start-mon|stop-mon|stop-all) (taskexecutor|zookeeper|historyserver|standalonesession|standalonejob|sql-gateway) [args]"

STARTSTOP=$1
DAEMON=$2
ARGS=("${@:3}") # get remaining arguments as array

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

#
# @description: 动态生成 MON_JOB_CLASSPATH 环境变量
#
# 该函数会执行以下操作:
# 1. 获取当前脚本所在目录的绝对路径。
# 2. 基于脚本路径计算出上两级目录，并将其设置为 CLOUD_HOME 环境变量。
# 3. 构造包含 jar 文件的目标目录的完整路径。
# 4. 扫描该目录下的所有 .jar 文件。
# 5. 将找到的 jar 文件列表格式化成 Java Classpath 字符串。
# 6. 导出最终的 MON_JOB_CLASSPATH 环境变量。
#
generate_mon_job_classpath() {
  # --- 1. 获取当前脚本的绝对路径，并计算出 CLOUD_HOME ---
  # 'dirname "$0"' 获取脚本所在的目录
  # 'cd ... && pwd' 切换到该目录并打印工作目录，从而获得绝对路径
  local SCRIPT_DIR
  SCRIPT_DIR=$(cd ../../ "$(dirname "$0")" && pwd)

  # 计算上两级目录作为 CLOUD_HOME，并导出为环境变量
  # 使用 'export' 使得 CLOUD_HOME 对子进程也可见
  export CLOUD_HOME
  CLOUD_HOME=$(dirname "$(dirname "${SCRIPT_DIR}")")

  # --- 2. 定义固定的 jar 包相对路径 ---
  local RELATIVE_JAR_PATH="mon-plugins/third_env_plugin/df_flink_app_dependence"
  local JAR_DIR="${CLOUD_HOME}/${RELATIVE_JAR_PATH}"

  # --- 3. 检查目标目录是否存在 ---
  if [ ! -d "${JAR_DIR}" ]; then
    echo "错误：JAR 目录不存在于 '${JAR_DIR}'" >&2
    return 1 # 返回错误码
  fi

  # --- 4. 扫描所有 .jar 文件并构建 Classpath ---
  local classpath_list
  # 使用 find 命令查找所有 .jar 文件，然后用 paste 命令将换行符替换为冒号(:)
  # -s: 将所有行合并成一行
  # -d: 指定分隔符为冒号
  classpath_list=$(find "${JAR_DIR}" -name "*.jar" | paste -sd:)

  # --- 5. 检查是否找到了 jar 文件 ---
  if [ -z "${classpath_list}" ]; then
    echo "警告：在目录 '${JAR_DIR}' 中没有找到任何 .jar 文件。" >&2
    # 即使没有找到，也导出一个有效的（但为空的）classpath
    export MON_JOB_CLASSPATH=""
  else
    # 按照要求，在最前面加上一个冒号，然后导出为环境变量
    export MON_JOB_CLASSPATH="${classpath_list}"
  fi

  echo "MON_JOB_CLASSPATH 已成功生成并导出。"
}
# 调用函数来生成和导出环境变量
generate_mon_job_classpath

# 检查结果
# 如果函数执行失败，则退出
if [ $? -ne 0 ]; then
    echo "环境变量生成失败，脚本终止。"
    exit 1
fi

# 打印环境变量以验证
echo "----------------------------------------"
echo "CLOUD_HOME: ${CLOUD_HOME}"
echo "MON_JOB_CLASSPATH: ${MON_JOB_CLASSPATH}"
echo "----------------------------------------"

case $DAEMON in
    (taskexecutor)
        CLASS_TO_RUN=org.apache.flink.runtime.taskexecutor.TaskManagerRunner
    ;;

    (zookeeper)
        CLASS_TO_RUN=org.apache.flink.runtime.zookeeper.FlinkZooKeeperQuorumPeer
    ;;

    (historyserver)
        CLASS_TO_RUN=org.apache.flink.runtime.webmonitor.history.HistoryServer
    ;;

    (standalonesession)
        CLASS_TO_RUN=org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint
    ;;

    (standalonejob)
        CLASS_TO_RUN=org.apache.flink.container.entrypoint.StandaloneApplicationClusterEntryPoint
    ;;

    (sql-gateway)
        CLASS_TO_RUN=org.apache.flink.table.gateway.SqlGateway
        SQL_GATEWAY_CLASSPATH="`findSqlGatewayJar`":"`findFlinkPythonJar`"
    ;;

    (*)
        echo "Unknown daemon '${DAEMON}'. $USAGE."
        exit 1
    ;;
esac

if [ "$FLINK_IDENT_STRING" = "" ]; then
    FLINK_IDENT_STRING="$USER"
fi

FLINK_TM_CLASSPATH=`constructFlinkClassPath`

pid=$FLINK_PID_DIR/flink-$FLINK_IDENT_STRING-MON-$DAEMON.pid

mkdir -p "$FLINK_PID_DIR"

# Log files for daemons are indexed from the process ID's position in the PID
# file. The following lock prevents a race condition during daemon startup
# when multiple daemons read, index, and write to the PID file concurrently.
# The lock is created on the PID directory since a lock file cannot be safely
# removed. The daemon is started with the lock closed and the lock remains
# active in this script until the script exits.
command -v flock >/dev/null 2>&1
if [[ $? -eq 0 ]]; then
    exec 200<"$FLINK_PID_DIR"
    flock 200
fi

# Ascending ID depending on number of lines in pid file.
# This allows us to start multiple daemon of each type.
id=$([ -f "$pid" ] && echo $(wc -l < "$pid") || echo "0")

FLINK_LOG_PREFIX="${FLINK_LOG_DIR}/flink-${FLINK_IDENT_STRING}-MON-${DAEMON}-${id}-${HOSTNAME}"
log="${FLINK_LOG_PREFIX}.log"
out="${FLINK_LOG_PREFIX}.out"

log_setting=("-Dlog.file=${log}" "-Dlog4j.configuration=file:${FLINK_CONF_DIR}/log4j.properties" "-Dlog4j.configurationFile=file:${FLINK_CONF_DIR}/log4j.properties" "-Dlogback.configurationFile=file:${FLINK_CONF_DIR}/logback.xml")

function guaranteed_kill {
  to_stop_pid=$1
  daemon=$2

  # send sigterm for graceful shutdown
  kill $to_stop_pid
  # if timeout exists, use it
  if command -v timeout &> /dev/null ; then
    # wait 10 seconds for process to stop. By default, Flink kills the JVM 5 seconds after sigterm.
    timeout 10 tail --pid=$to_stop_pid -f /dev/null &> /dev/null
    if [ "$?" -eq 124 ]; then
      echo "Daemon $daemon didn't stop within 10 seconds. Killing it."
      # send sigkill
      kill -9 $to_stop_pid
    fi
  fi
}



case $STARTSTOP in

    (start-mon)

        # Print a warning if daemons are already running on host
        if [ -f "$pid" ]; then
          active=()
          while IFS='' read -r p || [[ -n "$p" ]]; do
            kill -0 $p >/dev/null 2>&1
            if [ $? -eq 0 ]; then
              active+=($p)
            fi
          done < "${pid}"

          count="${#active[@]}"

          if [ ${count} -gt 0 ]; then
            echo "[INFO] $count instance(s) of $DAEMON are already running on $HOSTNAME."
          fi
        fi

        # Evaluate user options for local variable expansion
        FLINK_ENV_JAVA_OPTS=$(eval echo ${FLINK_ENV_JAVA_OPTS})

        echo "Starting $DAEMON daemon on host $HOSTNAME."
        "$JAVA_RUN" $JVM_ARGS ${FLINK_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "`manglePathList "$FLINK_TM_CLASSPATH:$MON_JOB_CLASSPATH:$SQL_GATEWAY_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" ${CLASS_TO_RUN} "${ARGS[@]}" > "$out" 200<&- 2>&1 < /dev/null &

        mypid=$!

        # Add to pid file if successful start
        if [[ ${mypid} =~ ${IS_NUMBER} ]] && kill -0 $mypid > /dev/null 2>&1 ; then
            echo $mypid >> "$pid"
        else
            echo "Error starting $DAEMON daemon."
            exit 1
        fi
    ;;

    (stop-mon)
        if [ -f "$pid" ]; then
            # Remove last in pid file
            to_stop=$(tail -n 1 "$pid")

            if [ -z $to_stop ]; then
                rm "$pid" # If all stopped, clean up pid file
                echo "No $DAEMON daemon to stop on host $HOSTNAME."
            else
                sed \$d "$pid" > "$pid.tmp" # all but last line

                # If all stopped, clean up pid file
                [ $(wc -l < "$pid.tmp") -eq 0 ] && rm "$pid" "$pid.tmp" || mv "$pid.tmp" "$pid"

                if kill -0 $to_stop > /dev/null 2>&1; then
                    echo "Stopping $DAEMON daemon (pid: $to_stop) on host $HOSTNAME."
                    guaranteed_kill $to_stop $DAEMON
                else
                    echo "No $DAEMON daemon (pid: $to_stop) is running anymore on $HOSTNAME."
                fi
            fi
        else
            echo "No $DAEMON daemon to stop on host $HOSTNAME."
        fi
    ;;

    (stop-all)
        if [ -f "$pid" ]; then
            mv "$pid" "${pid}.tmp"

            while read to_stop; do
                if kill -0 $to_stop > /dev/null 2>&1; then
                    echo "Stopping $DAEMON daemon (pid: $to_stop) on host $HOSTNAME."
                    guaranteed_kill $to_stop $DAEMON
                else
                    echo "Skipping $DAEMON daemon (pid: $to_stop), because it is not running anymore on $HOSTNAME."
                fi
            done < "${pid}.tmp"
            rm "${pid}.tmp"
        fi
    ;;

    (*)
        echo "Unexpected argument '$STARTSTOP'. $USAGE."
        exit 1
    ;;

esac
