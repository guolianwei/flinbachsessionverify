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
stop_yarn_session_mon_local_process() {
  # --- 1. 定义常量 ---
  local a_flag="${FLINK_CONFIG_FOLDER_NAME}"
  local yarnsessioncli_class="org.apache.flink.yarn.cli.FlinkYarnSessionCli"

  # 步骤 3.1: 使用 pgrep -f 查找所有匹配 flag 的 java 进程的 PID
  local pids
  pids=$(pgrep -f "java.*${a_flag}")

  local process_list="" # 初始化为空

  # 步骤 3.2: 仅在找到 PID 的情况下，才使用 ps 检查
  if [ -n "${pids}" ]; then
      local pids_for_ps
      pids_for_ps=$(echo "${pids}" | tr '\n' ',' | sed 's/,$//') # sed 's/,$//' 删除末尾的逗号

      # 步骤 3.3: 使用 ps -fww -p [PIDs] 获取完整、不截断的命令行
      process_list=$(ps -fww -p "${pids_for_ps}" 2>/dev/null | grep -E "${yarnsessioncli_class}")
      # 2>/dev/null 隐藏 ps 可能报出的 "PID 不存在" 的错误 (以防进程在 pgrep 和 ps 之间死掉)
  fi

  # --- 4. 检查结果并打印 ---
  if [ -z "${process_list}" ]; then
    # 如果 process_list 字符串为空，说明未找到
    echo "--------------------------------------------------"
    echo "【停止失败】"
    echo "错误：未找到标记为 '${a_flag}' 的yarn轻量服务进程。"
    echo "（pgrep 找到的 PIDs: [${pids:-无}]）"
    echo "请检查启动日志以获取详细信息。"
    echo "--------------------------------------------------"
    return 1 # 返回非零值表示失败
  else
    # 找到了进程
    echo "--------------------------------------------------"
    echo "已找到以下 Flink 进程 (完整命令行)："
    echo ""

    # 打印 ps 的表头
    ps -fww -p $$ | head -n 1

    # 打印 grep 到的进程列表
    echo "${process_list}"
    echo ""

    # 从 ps -f (PID 在第二列) 输出中提取 PIDs
    local pids_found
    pids_found=$(echo "${process_list}" | awk '{print $2}' | tr '\n' ' ')

    echo "进程 PIDs: ${pids_found}"
    kill ${pids_found}
    echo "【停止成功】"
    echo "--------------------------------------------------"
    return 0 # 返回 0 表示成功
  fi
}

stop_yarn_session_mon_local_process
