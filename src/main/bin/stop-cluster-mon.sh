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

. "$bin"/config.sh
#
# @description: 停止所有包含特定标记的 Java 进程
#
# 该函数会查找并终止所有在其命令行参数中包含
# "meritdata_mon_light_weight_conf" 字符串的 Java 进程。
#
# 为防止误操作，在执行终止操作前，会显示将要被终止的进程列表，
# 并请求用户确认。
#
stop_meritdata_processes() {
  # 定义要搜索的标记
  local a_flag="meritdata_mon_light_weight_conf"

  # --- 1. 查找匹配的 Java 进程 ---
  # 使用 pgrep -af "java.*${a_flag}" 来精确查找:
  # -a: 显示完整的命令行
  # -f: 在整个命令行中匹配模式
  # "java.*${a_flag}": 匹配以 "java" 开头且包含我们指定标记的进程
  local process_list
  process_list=$(pgrep -af "java.*${a_flag}")

  # --- 2. 检查是否找到了任何进程 ---
  if [ -z "${process_list}" ]; then
    echo "信息：没有找到包含 '${a_flag}' 标记的 Java 进程。"
    return 0 # 正常退出
  fi

  # --- 3. 显示将要被终止的进程列表并请求确认 ---
  echo "警告：以下 Java 进程将被终止："
  echo "--------------------------------------------------"
  # 使用 -f 选项和精确的 PID 列表来显示更详细的进程信息
  # 'cut -d' ' -f1' 从 pgrep 的输出中提取 PID
  ps -f -p $(echo "${process_list}" | cut -d' ' -f1 | tr '\n' ',' | sed 's/,$//')
  echo "--------------------------------------------------"

  # 使用 read 命令向用户提问
  # -p: 显示提示信息
  # -r: 防止反斜杠被解释
  # -n 1: 读取一个字符后立即返回，无需按 Enter
  read -p "您确定要终止这些进程吗？ (y/N) " -r -n 1 confirm
  echo # 输出一个换行符，使格式更美观

  # --- 4. 根据用户输入执行操作 ---
  # 将用户输入转为小写，并与 "y" 比较
  if [[ "${confirm,,}" == "y" ]]; then
    echo "正在终止进程..."
    # 从进程列表中提取 PID (第一列)
    local pids_to_kill
    pids_to_kill=$(echo "${process_list}" | cut -d' ' -f1)

    # 使用 kill 命令终止进程
    # 使用 xargs 可以更好地处理 PID 列表
    echo "${pids_to_kill}" | xargs kill

    # 短暂等待并检查进程是否已成功终止
    sleep 2
    local remaining_processes
    remaining_processes=$(pgrep -f "java.*${a_flag}")

    if [ -z "${remaining_processes}" ]; then
        echo "成功：所有匹配的进程都已终止。"
    else
        echo "警告：某些进程可能未能正常终止。尝试强制终止 (kill -9)..."
        echo "${remaining_processes}" | cut -d' ' -f1 | xargs kill -9
        sleep 1
        # 最终检查
        if [ -z "$(pgrep -f "java.*${a_flag}")" ]; then
            echo "成功：所有进程已被强制终止。"
        else
            echo "错误：仍有进程未能终止，请手动检查。"
            return 1 # 返回错误码
        fi
    fi
  else
    echo "操作已取消。"
  fi
}
# Stop TaskManager instance(s)
TMWorkersMon stop-mon

# Stop JobManager instance(s)
shopt -s nocasematch
if [[ $HIGH_AVAILABILITY == "zookeeper" ]]; then
    # HA Mode
    readMasters

    if [ ${MASTERS_ALL_LOCALHOST} = true ] ; then
        for master in ${MASTERS[@]}; do
            "$FLINK_BIN_DIR"/jobmanager-mon.sh stop-mon
        done
    else
        for master in ${MASTERS[@]}; do
            ssh -n $FLINK_SSH_OPTS $master -- "nohup /bin/bash -l \"${FLINK_BIN_DIR}/jobmanager-mon.sh\" stop-mon &"
        done
    fi

else
    "$FLINK_BIN_DIR"/jobmanager-mon.sh stop-mon
fi
# 停止 监管轻量flink服务
stop_meritdata_processes
shopt -u nocasematch
