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
    echo "3. 停止 Flink (Stop Flink)"
    echo "4. 退出控制台 (Exit)"
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

            # ##################################################################
            # ############## 以下是本次修改的核心部分 ############################
            # ##################################################################

            # --- 构造并打印所有可用的 Web UI 地址 ---
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
            # ##################################################################

            ;;
        2)
            # ... (其他选项保持不变)
            echo "---"
            echo "正在运行的任务 (Running Jobs):"
            $FLINK_HOME/bin/flink list -all
            ;;
        3)
            # ... (其他选项保持不变)
            echo "---"
            echo "正在停止 Flink 集群..."
            echo "Stopping Flink cluster..."
            $FLINK_HOME/bin/stop-cluster-mon.sh
            echo "服务已停止 (Service stopped)"
            ;;
        4)
            # ... (其他选项保持不变)
            echo "再见! (Goodbye!)"
            exit 0;
          ;;
        *)
            echo "无效选项，请输入 1-4 之间的数字。"
            echo "Invalid option, please enter a number between 1 and 4."
            ;;
    esac
done