#!/bin/bash

# 自动获取 Flink 安装路径
# Get Flink installation path automatically
FLINK_HOME=$(cd "$(dirname "$0")/.." && pwd)
export FLINK_CONF_DIR=$FLINK_HOME/meritdata_mon_light_weight_conf
CONF_FILE="$FLINK_CONF_DIR/flink-conf.yaml"

# 创建任务运行日志目录
# Create directory for job logs
LOGS_DIR="$FLINK_HOME/logs"
mkdir -p "$LOGS_DIR"
echo "任务日志目录 (Log directory): $LOGS_DIR"
echo "=================================================="

#
# @description: 检查 Flink (JM/TM) 进程是否已成功启动
#
# @param $1: (可选) 等待的秒数。默认为 10。
#
# 该函数会等待指定秒数 (带倒计时)，然后检查包含
# "meritdata_mon_light_weight_conf" 标记的
# StandaloneSessionClusterEntrypoint (JobManager) 和
# TaskManagerRunner (TaskManager) Java 进程。
#
# 此版本使用 'ps -fww' 来防止命令行被截断。
#
check_flink_startup_status() {
  # --- 1. 定义常量 ---
  local a_flag="meritdata_mon_light_weight_conf"
  local jobmanager_class="org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint"
  local taskmanager_class="org.apache.flink.runtime.taskexecutor.TaskManagerRunner"

  # 【修改点】
  # 使用 ${1:-10} 语法:
  # 如果 $1 (函数的第一个参数) 存在且不为空，则使用 $1 的值。
  # 否则 (如果 $1 未传递或为空)，则使用默认值 10。
  local countdown_seconds=${1:-10}

  echo "将在 ${countdown_seconds} 秒后开始检查进程状态..."

  # --- 2. 执行倒计时 ---
  # 增加一个简单的输入验证，防止 $countdown_seconds 为非数字
  if ! [[ "${countdown_seconds}" =~ ^[0-9]+$ ]]; then
    echo "错误：提供的倒计时 '${countdown_seconds}' 不是一个有效的数字。将使用默认值 10。" >&2
    countdown_seconds=10
  fi

  for i in $(seq ${countdown_seconds} -1 1); do
    echo -n "请等待 ${i} 秒...  "
    echo -n -e "\r"
    sleep 1
  done
  echo "检查开始...                                "


  # --- 3. 查找进程 (分两步) ---

  # 步骤 3.1: 使用 pgrep -f 查找所有匹配 flag 的 java 进程的 PID
  local pids
  pids=$(pgrep -f "java.*${a_flag}")

  local process_list="" # 初始化为空

  # 步骤 3.2: 仅在找到 PID 的情况下，才使用 ps 检查
  if [ -n "${pids}" ]; then
      local pids_for_ps
      pids_for_ps=$(echo "${pids}" | tr '\n' ',' | sed 's/,$//') # sed 's/,$//' 删除末尾的逗号

      # 步骤 3.3: 使用 ps -fww -p [PIDs] 获取完整、不截断的命令行
      process_list=$(ps -fww -p "${pids_for_ps}" 2>/dev/null | grep -E "${jobmanager_class}|${taskmanager_class}")
      # 2>/dev/null 隐藏 ps 可能报出的 "PID 不存在" 的错误 (以防进程在 pgrep 和 ps 之间死掉)
  fi

  # --- 4. 检查结果并打印 ---
  if [ -z "${process_list}" ]; then
    # 如果 process_list 字符串为空，说明未找到
    echo "--------------------------------------------------"
    echo "【启动失败】"
    echo "错误：未找到标记为 '${a_flag}' 的 Flink JobManager 或 TaskManager 进程。"
    echo "（pgrep 找到的 PIDs: [${pids:-无}]）"
    echo "请检查启动日志以获取详细信息。"
    echo "--------------------------------------------------"
    return 1 # 返回非零值表示失败
  else
    # 找到了进程
    echo "--------------------------------------------------"
    echo "【启动成功】"
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
    echo "--------------------------------------------------"
    return 0 # 返回 0 表示成功
  fi
}
# 定义函数：显示服务访问地址
show_service_urls() {
      # 1. 获取最终的端口号
      FINAL_PORT=$(get_config "rest.port")
      FINAL_PORT=${FINAL_PORT:-8081} # 如果未设置，使用默认的8081
      echo ""
      echo "服务启动成功! Web UI 可通过以下地址访问:"
      echo "Service started successfully! Web UI is available at the following addresses:"

      # 2. 打印 localhost 地址
      echo "  - http://localhost:${FINAL_PORT}"

      # 3. 根据操作系统类型获取本机所有非回环IP地址
      # Detect OS and get all non-loopback IP addresses
      if [[ "$(uname)" == "Darwin" ]]; then
          # macOS
          IP_LIST=$(ifconfig | grep "inet " | grep -v "127.0.0.1" | awk '{print $2}')
      else
          # Linux
          IP_LIST=$(ip addr show | grep "inet " | grep -v "127.0.0.1" | awk '{print $2}' | cut -d'/' -f1)
      fi

      # 4. 循环打印所有IP地址组合的URL
      for ip in $IP_LIST; do
          echo "  - http://${ip}:${FINAL_PORT}"
      done
}
#
# 函数：从 flink-conf.yaml 读取指定的配置项
# Function: Read a specific configuration value from flink-conf.yaml
# 参数1: 配置项 Key (e.g., rest.port)
#
get_config() {
    local key=$1
    # 从配置文件中查找 key，排除注释行，然后提取 value
    # Find the key in the config file, exclude commented lines, and extract the value
    local value=$(grep "^\s*${key}:" "$CONF_FILE" | sed 's/#.*//' | awk '{print $2}')
    echo "$value"
}

#
# 函数：修改 Flink 配置
# Function: Modify a configuration in flink-conf.yaml
# 参数1: 配置项 Key (e.g., rest.port)
# 参数2: 配置项 Value (e.g., 8081)
#
update_config() {
    local key=$1
    local value=$2

    # 如果值不为空，则进行更新
    # If the value is not empty, proceed with the update
    if [ -n "$value" ]; then
        # 删除已存在的配置行
        # Delete the existing configuration line
        sed -i.bak "/^${key}:/d" "$CONF_FILE"

        # 在文件末尾追加新的配置
        # Append the new configuration to the end of the file
        echo "${key}: ${value}" >> "$CONF_FILE"
        echo "配置更新: ${key} 设置为 ${value}"
        echo "Configuration updated: ${key} set to ${value}"
    fi
}


# 服务控制循环
# Main control loop
while true; do
    echo ""
    echo "--- Flink 本地服务控制台 ---"
    echo "1. 启动 Flink (Start Flink)"
    echo "2. 查看运行中任务 (List Running Jobs)"
    echo "3. 检查运行中的Flink轻量服务进程(List Running Flink Service Processes)"
    echo "4. 获得轻量服务访问 Web UI 地址 (Get Flink Service Web UI Addresses)"
    echo "5. 停止 Flink (Stop Flink)"
    echo "6. 退出控制台 (Exit)"
    read -p "请输入选项(1-4) (Enter your choice): " choice

    case $choice in
        1)
            # --- 备份原始配置 ---
            if [ ! -f "${CONF_FILE}.original" ]; then
                cp "$CONF_FILE" "${CONF_FILE}.original"
                echo "首次运行，原始配置文件已备份至: ${CONF_FILE}.original"
            fi

            # --- 读取并显示当前配置 ---
            echo "---"
            echo "正在读取当前 Flink 配置..."
            CURRENT_REST_PORT=$(get_config "rest.port")
            CURRENT_JM_MEMORY=$(get_config "jobmanager.memory.process.size")
            CURRENT_TM_MEMORY=$(get_config "taskmanager.memory.process.size")

            echo "当前配置 (Current Configuration):"
            echo "  - REST 端口 (REST Port):                ${CURRENT_REST_PORT:-未设置 (默认: 8081)}"
            echo "  - JobManager 内存 (JobManager Memory):    ${CURRENT_JM_MEMORY:-未设置}"
            echo "  - TaskManager 内存 (TaskManager Memory):  ${CURRENT_TM_MEMORY:-未设置}"
            echo "---"

            # --- 询问用户操作 ---
            read -p "是否使用以上配置直接启动? [Y/n] (Use this configuration to start?): " confirm_start

            if [[ "$confirm_start" =~ ^[Nn]$ ]]; then
                # --- 获取用户输入进行修改 ---
                echo ""
                echo "请输入新的 Flink 配置 (直接回车将保留当前值)"
                echo "Please enter new Flink configuration (press Enter to keep the current value)"

                read -p "设置新 REST 端口 [当前: ${CURRENT_REST_PORT:-8081}]: " NEW_REST_PORT
                read -p "设置新 JobManager 内存 [当前: ${CURRENT_JM_MEMORY:-无}]: " NEW_JM_MEMORY
                read -p "设置新 TaskManager 内存 [当前: ${CURRENT_TM_MEMORY:-无}]: " NEW_TM_MEMORY
                echo "---"

                update_config "rest.port" "$NEW_REST_PORT"
                update_config "jobmanager.memory.process.size" "$NEW_JM_MEMORY"
                update_config "taskmanager.memory.process.size" "$NEW_TM_MEMORY"

                rm -f "${CONF_FILE}.bak"
            else
                echo "好的，将使用当前配置启动 Flink..."
                echo "OK, starting Flink with the current configuration..."
            fi

            # --- 启动 Flink ---
            echo ""
            echo "正在启动 监管轻量任务 Flink 集群..."
            echo "Starting Flink cluster..."
            $FLINK_HOME/bin/start-cluster-mon.sh


            check_flink_startup_status 10
            # 检查函数返回值
            if [ $? -eq 0 ]; then
                # 返回值为0，表示启动成功，调用显示服务地址函数
                # --- 构造并打印所有可用的 Web UI 地址 ---
                show_service_urls
            else
                # 返回值为非0，表示启动失败，输出错误信息
                echo ""
                echo "Flink 服务启动检查失败，请查看上述错误信息并排查问题。"
            fi

            ;;
        2)
            # ... (其他选项保持不变)
            echo "---"
            echo "正在运行的任务 (Running Jobs):"
            $FLINK_HOME/bin/flink list -all
            ;;
        3)
          echo "---"
          echo "正在检查运行中的Flink 轻量服务进程..."
          check_flink_startup_status 0
          ;;
        4)
          echo "---"
          echo "获得轻量服务访问 Web UI 地址..."
          show_service_urls
          ;;
        5)
            # ... (其他选项保持不变)
            echo "---"
            echo "正在停止 Flink 集群..."
            echo "Stopping Flink cluster..."
            $FLINK_HOME/bin/stop-cluster-mon.sh
            echo "服务已停止 (Service stopped)"
            ;;
        6)
            # ... (其他选项保持不变)
            echo "再见! (Goodbye!)"
            exit 0;
          ;;
        *)
            echo "无效选项，请输入 1-5 之间的数字。"
            echo "Invalid option, please enter a number between 1 and 4."
            ;;
    esac
done