#!/bin/bash

# 自动获取 Flink 安装路径
# Get Flink installation path automatically
FLINK_HOME=$(cd "$(dirname "$0")/.." && pwd)
export FLINK_HOME
CLOUD_HOME=$(cd "$(dirname "$0")/../../../.." && pwd)
export CLOUD_HOME
export JAVA_HOME=$CLOUD_HOME/jdk/linux/jdk-21.0.2/

if [ ! -d "${JAVA_HOME}" ]; then
    echo "错误：JAVA_HOME 目录不存在于 '${JAVA_HOME}'" >&2
    return 1 # 返回错误码
fi
export MON_NFS_HOME=$CLOUD_HOME/file/cloud_mon
export FLINK_CONFIG_FOLDER_NAME=default_lightweight_session_flink-1.17.1_CONFIG
export FLINK_CONF_DIR=$MON_NFS_HOME/${FLINK_CONFIG_FOLDER_NAME}/flinkconf
export FLINK_LOG_DIR=$MON_NFS_HOME/${FLINK_CONFIG_FOLDER_NAME}/appwork/logs
export FLINK_WORK_DIR=$MON_NFS_HOME/${FLINK_CONFIG_FOLDER_NAME}/appwork
export HADOOP_CONF_DIR=$MON_NFS_HOME/DEFAULT_CONFIG
#export DEFAULT_HADOOP_CONFIG=$CLOUD_HOME/file/cloud_mon/DEFAULT_CONFIG
export HBASE_CONF_DIR=$MON_NFS_HOME/DEFAULT_CONFIG
export HADOOP_CLASSPATH="${FLINK_HOME}/hadooplib/*"
export CONF_FILE="$FLINK_CONF_DIR/flink-conf.yaml"
export YARN_APP_TYPE="MD_MON-LIGHT_WEIGHT"
echo "FLINK_CONF_FILE_PATH: $CONF_FILE"
export YARN_SESSION_CLI_CLASS="org.apache.flink.yarn.cli.FlinkYarnSessionCli"

export FLINK_ENV_JAVA_OPTS="-Dfile.encoding=utf8 --add-exports java.base/sun.net.util=ALL-UNNAMED --add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"


# 创建任务运行日志目录
# Create directory for job logs
mkdir -p "$FLINK_LOG_DIR"
echo "日志目录 (Log directory): $FLINK_LOG_DIR"
echo "配置目录 (Config directory): $FLINK_CONF_DIR"
echo "配置文件 (Config file): $CONF_FILE"
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
  local a_flag="${FLINK_CONFIG_FOLDER_NAME}"
  local yarnsessioncli_class="$YARN_SESSION_CLI_CLASS"

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
      process_list=$(ps -fww -p "${pids_for_ps}" 2>/dev/null | grep -E "${yarnsessioncli_class}")
      # 2>/dev/null 隐藏 ps 可能报出的 "PID 不存在" 的错误 (以防进程在 pgrep 和 ps 之间死掉)
  fi

  # --- 4. 检查结果并打印 ---
  if [ -z "${process_list}" ]; then
    # 如果 process_list 字符串为空，说明未找到
    echo "--------------------------------------------------"
    if [ "$countdown_seconds" -eq 0 ]; then
      echo ""
    else
        echo "【启动失败】"
    fi
    echo "错误：未找到标记为 '${a_flag}' 的yarn轻量服务进程。"
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
    echo "服务访问地址 (Service URLs):"
    local result
    result=$( get_yarn_apps_by_type )
    echo "$result" | while read -r app_id app_name; do
        echo "处理应用: $app_id ($app_name)"
        # 在这里添加对每个应用的处理逻辑
        local tracking_url
        tracking_url=$(yarn application -status $app_id | grep -oP 'Tracking-URL : \K.*')
        echo "轻量服务地址: $tracking_url"
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
    local value
    value=$(grep "^\s*${key}:" "$CONF_FILE" | sed 's/#.*//' | awk '{print $2}')
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
check_light_weight_service() {
    # --- 检查是否已经有启动的进程，如果有，则提示已经启动 ---
    local a_flag="${FLINK_CONFIG_FOLDER_NAME}"
    local yarnsessioncli_class="$YARN_SESSION_CLI_CLASS"
    local pids
    local process_list

    # 查找包含特定标志的Java进程
    pids=$(pgrep -f "java.*${a_flag}" 2>/dev/null)

    if [ -n "${pids}" ]; then
        local pids_for_ps
        # 将PID列表转换为逗号分隔格式，用于ps命令
        pids_for_ps=$(echo "${pids}" | tr '\n' ',' | sed 's/,$//') # sed 's/,$//' 删除末尾的逗号

        # 使用 ps -fww -p [PIDs] 获取完整、不截断的命令行
        process_list=$(ps -fww -p "${pids_for_ps}" 2>/dev/null | grep -E "${yarnsessioncli_class}")
    fi

    if [ -z "${process_list}" ]; then
        echo "--------------------------------------------------"
        echo "未找到已经启动的轻量服务，开启启动新轻量服务..."
        return 0  # 未找到进程，返回0
    else
        echo "轻量服务已经启动..."
        echo "找到的进程信息："
        echo "${process_list}"
        return 1  # 找到进程，返回1
    fi
}
start_flink_session_service() {
              # ---  检查是否已经有启动的进程，如果有，则提示已经启动 ---
              echo "=== 检查服务状态 ==="
              if check_light_weight_service; then
                  echo "可以启动新服务"
              else
                  echo "服务已在运行，无需重复启动"
                  return 1
              fi

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
              #自动配置yarn.properties-file.location
              echo "更新$CONF_FILE 的配置项 yarn.properties-file.location: $FLINK_WORK_DIR"
              update_config "yarn.properties-file.location" "$FLINK_WORK_DIR"
              # --- 启动 Flink ---
              echo ""
              echo "正在启动 监管轻量任务 Flink 集群..."
              echo "Starting Flink cluster..."
              nohup $FLINK_HOME/bin/start-yarn-session-mon.sh > $FLINK_LOG_DIR/flink-yarn-session-mon-lightweight.log 2>&1 &

              sleep 2
              tail -f $FLINK_LOG_DIR/flink-yarn-session-mon-lightweight.log

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
}
get_yarn_apps_by_type() {
    # 执行YARN应用查询并过滤特定类型
    yarn application -list | grep "$YARN_APP_TYPE" | awk '{print $1, $2}'
}
# 函数：安全停止 Flink YARN Session 服务
# 返回值：停止成功返回0，用户取消返回1，未找到服务返回2
stop_flink_yarn_session() {
    echo "---"
    echo "正在停止轻量flink yarn session 服务"
    echo "Stopping flink yarn session..."

    # 首先检查是否有正在运行的Yarn应用
    echo "检查正在运行的Yarn应用..."
    result=$(get_yarn_apps_by_type)

    # 判断是否找到记录
    if [ -n "$result" ]; then
        # 计算找到的记录数
        count=$(echo "$result" | wc -l)
        echo "找到 ${count} 个正在运行的轻量flink服务："
        echo "$result"
        echo ""

        # 显示严重警告
        echo "⚠️  警告：停止服务将中断所有正在运行的Flink任务！"
        echo "   这将导致："
        echo "   - 所有运行中的Flink作业将被强制终止"
        echo "   - 未保存的计算结果可能会丢失"
        echo "   - 需要重新提交所有作业才能恢复服务"
        echo ""

        # 提示用户确认
        read -p "确认要停止服务吗？(y/N，默认N): " confirm_stop

        # 将用户输入转换为小写，并设置默认值
        confirm_stop=${confirm_stop:-n}
        confirm_stop=$(echo "$confirm_stop" | tr '[:upper:]' '[:lower:]')

        if [ "$confirm_stop" = "y" ] || [ "$confirm_stop" = "yes" ]; then
            echo "正在停止服务..."
            $FLINK_HOME/bin/stop-yarn-session-mon.sh

            # 检查停止是否成功
            local stop_status=$?
            if [ $stop_status -eq 0 ]; then
                echo "✅ 服务停止完成 (Service stopped completely)"
                # 可选：再次检查确认服务确实已停止
                sleep 2
                result_after_stop=$(get_yarn_apps_by_type)
                if [ -z "$result_after_stop" ]; then
                    echo "✅ 验证：所有轻量flink服务已确认停止"
                    return 0
                else
                    echo "⚠️  警告：部分服务可能仍在运行，请手动检查"
                    echo "$result_after_stop"
                    return 3
                fi
            else
                echo "❌ 服务停止失败，请手动检查 (Service stop failed, please check manually)"
                return 4
            fi
        else
            echo "操作已取消，服务继续运行 (Operation cancelled, service continues running)"
            return 1
        fi
    else
        echo "ℹ️  未找到正在运行的轻量flink服务，无需停止"
        echo "   (No running light-weight flink service found, no need to stop)"
        return 2
    fi
}
# 服务控制循环
# Main control loop
while true; do
    echo ""
    echo "--- Flink Yarn Session服务控制台 ---"
    echo "1. 启动 Flink on Yarn (Start Flink on Yarn)"
    echo "2. 查看正在运行的轻量服务（Flink session Yarn应用） (Yarn Running Apps)"
    echo "3. 检查运行中的Flink本地轻量服务进程(List Running Flink Service Processes)"
    echo "4. 获得轻量服务访问 Web UI 地址 (Get Flink Service Web UI Addresses)"
    echo "5. 停止轻量flink yarn session 服务"
    echo "6. 退出控制台 (Exit)"
    read -p "请输入选项(1-6) (Enter your choice): " choice

    case $choice in
        1)
            start_flink_session_service
            ;;
        2)
            # ... (其他选项保持不变)
            echo "---"
            echo "正在运行的Yarn应用 (Yarn Running Apps):"
            # 执行YARN应用查询并过滤特定类型
            result=$( get_yarn_apps_by_type )
            # 判断是否找到记录
            if [ -n "$result" ]; then
                # 计算找到的记录数
                count=$(echo "$result" | wc -l)
                echo "找到${count}条记录，如下："
                echo "$result"
                # 添加警告逻辑：如果记录数大于1
                if [ "$count" -gt 1 ]; then
                    echo ""
                    echo "警告(WARNING)：有多个轻量应用服务在yarn服务中，请检查，并停止无用的轻量应用服务"
                fi
            else
                echo "未找到Yarn服务上的轻量flink服务"
            fi
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
          # 调用停止函数
          stop_flink_yarn_session
          # 可以根据返回值做进一步处理，例如：
          stop_result=$?
          case $stop_result in
              0) echo "停止成功" ;;
              1) echo "用户取消了停止操作" ;;
              2) echo "没有运行的服务可停止" ;;
              3) echo "服务已停止，但建议手动验证" ;;
              4) echo "停止命令执行失败" ;;
          esac
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