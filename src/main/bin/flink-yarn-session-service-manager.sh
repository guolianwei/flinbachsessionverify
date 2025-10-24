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
#可选值: "操作系统环境自带" 或 "当前工具内置"
export MON_HADOOP_CLIENT_SOURCE="操作系统环境自带"
# --- 初始化 Flink 配置的函数 ---
#
# @description 检查 Flink 配置目录，如果不存在或不完整，则从模板目录拷贝，并拷贝【正式】的配置文件覆盖默认配置
# @assumes FLINK_HOME, FLINK_CONF_DIR, MON_NFS_HOME (global) 均已被设置
# @returns 0 on success, 1 on failure
#
initialize_flink_config() {
    # 源模板目录
    local source_conf_dir="$FLINK_HOME/meritdata_mon_light_weight_local_conf"
    # 目标核心配置文件
    local conf_file="$FLINK_CONF_DIR/flink-conf.yaml"

    local needs_init=0 # 标记是否需要初始化 (0=否, 1=是)

    # --- 检查是否需要初始化 ---

    # 1. 检查目标目录是否存在 (初次使用)
    if [ ! -d "${FLINK_CONF_DIR}" ]; then
        echo "信息：初次使用，正在初始化 Flink 配置目录..."
        echo "      (目标: ${FLINK_CONF_DIR})"
        needs_init=1

    # 2. 检查目录存在，但核心文件是否缺失 (配置不完整)
    elif [ ! -f "${conf_file}" ]; then
        echo "警告：配置目录 '${FLINK_CONF_DIR}' 已存在，但缺少 'flink-conf.yaml'。"
        echo "      系统将重新初始化轻量配置..."
        needs_init=1

    # 3. 配置已存在且完整
    else
        echo "信息：配置目录 '${FLINK_CONF_DIR}' 已存在且完整，跳过初始化。"
        return 0 # 成功，无需操作
    fi

    # --- 执行初始化 (仅当 needs_init=1) ---
    if [ $needs_init -eq 1 ]; then
        # 1. 检查源目录是否存在
        if [ ! -d "${source_conf_dir}" ]; then
            echo "错误：源配置模板目录 '${source_conf_dir}' 也不存在！无法初始化配置。" >&2
            return 1 # 致命错误
        fi

        # 2. 创建目标目录 (mkdir -p 是安全的，即使目录已存在)
        echo "正在创建目标配置目录..."
        mkdir -p "${FLINK_CONF_DIR}"
        if [ $? -ne 0 ]; then
            echo "错误：创建目录 '${FLINK_CONF_DIR}' 失败。" >&2
            return 1
        fi

        # 3. 拷贝所有【默认】文件
        echo "正在从 '${source_conf_dir}' 拷贝【默认】配置..."
        # 使用 -a 保留属性, 使用 /. 确保拷贝所有内容 (包括 .dotfiles)
        cp -a "${source_conf_dir}/." "${FLINK_CONF_DIR}/"

        if [ $? -ne 0 ]; then
            echo "错误：从 '${source_conf_dir}' 拷贝【默认】配置到 '${FLINK_CONF_DIR}' 失败。" >&2
            return 1
        fi

        echo "默认配置拷贝成功。"

        # --- 4. [修改点] 拷贝【正式】的 flink-conf.yaml 覆盖默认配置 ---
        local official_conf_source_dir="$MON_NFS_HOME/default_flink-1.17.1_CONFIG"
        local official_conf_file_source="${official_conf_source_dir}/flink-conf.yaml"
        local official_conf_file_dest="${FLINK_CONF_DIR}/flink-conf.yaml"

        echo "正在检查并拷贝【正式】的 'flink-conf.yaml' 配置文件..."

        # 4.1 检查正式配置文件是否存在
        if [ ! -f "${official_conf_file_source}" ]; then
            echo "错误：未找到正式的 Flink 配置文件：'${official_conf_file_source}'" >&2
            echo "       请在【系统管理】->【监管平台配置】 中配置【Hadoop配置、JDK配置和Flink配置】" >&2
            return 1 # 失败
        fi

        # 4.2 拷贝正式配置文件 (使用 -f 强制覆盖)
        cp -f "${official_conf_file_source}" "${official_conf_file_dest}"

        if [ $? -eq 0 ]; then
            echo "正式 'flink-conf.yaml' 拷贝成功。"
            return 0 # 最终成功
        else
            echo "错误：拷贝正式配置文件 '${official_conf_file_source}' 失败。" >&2
            return 1 # 失败
        fi
        # --- [拷贝正式配置结束] ---
    fi
}

# --- [修改] 调用初始化函数 ---
# 在 FLINK_CONF_DIR 定义后立即调用
initialize_flink_config

# 检查函数是否执行失败 (检查上一条命令的退出状态码 $?)
if [ $? -ne 0 ]; then
    echo "错误：Flink 配置初始化失败，脚本终止。" >&2
    return 1
fi

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
        tracking_url=$("${MON_HADOOP_BIN_PATH}"yarn application -status $app_id | grep -oP 'Tracking-URL : \K.*')
        echo "轻量服务地址: $tracking_url"
    done
}

#
# 函数：从 flink-conf.yaml 读取指定的配置项
# Function: Read a specific configuration value from flink-conf.yaml (Optimized)
# 参数1: 配置项 Key (e.g., rest.port)
#
get_config() {
    local key=$1
    # 1. 查找以 key: 开头的行 (忽略前面的空格)
    # 2. 移除行尾的注释
    # 3. 使用 sed 移除 key 和冒号 (以及前后的空格)，只保留值
    local value
    value=$(grep "^\s*${key}:" "$CONF_FILE" | sed 's/#.*//' | sed -E "s/^\s*${key}:\s*//")

    # 再次清理一下可能存在的前后空格
    value=$(echo "$value" | sed 's/^\s*//;s/\s*$//')

    echo "$value"
}

#
# 函数：检查 Flink 核心配置
# Function: Validate core Flink configurations
#
# @description 检查 flink-conf.yaml 中的一系列必需配置项是否已设置且不为空。
# @uses $CONF_FILE (global variable) 必须在调用此函数前设置
# @uses get_config (function)
# @returns 0 on success (all checks passed), 1 on failure
#
validate_flink_config() {
    # 1. 确保 CONF_FILE 变量有效且文件可读
    if [ -z "$CONF_FILE" ]; then
        echo "错误：[validate_flink_config] 变量 'CONF_FILE' 未设置。" >&2
        return 1
    fi

    if [ ! -f "$CONF_FILE" ] || [ ! -r "$CONF_FILE" ]; then
        echo "错误：[validate_flink_config] 无法读取配置文件于: '$CONF_FILE'" >&2
        return 1
    fi

    echo "正在开始检查配置文件: $CONF_FILE"

    # 2. 定义需要检查的配置项列表
    # (根据您的请求列表)
    local keys_to_check=(
        "yarn.provided.lib.dirs"
        "env.java.opts.client"
        "env.java.opts"
        "yarn.ship-archives"
        "env.java.home"
        "containerized.taskmanager.env.JAVA_HOME"
        "containerized.master.env.JAVA_HOME"
    )

    local validation_passed=true  # 标记是否所有检查都通过
    local missing_keys=()       # 用于存储所有缺失的 key

    # 3. 循环遍历并检查每一个 key
    for key in "${keys_to_check[@]}"; do
        # 调用 get_config 函数获取值
        local value
        value=$(get_config "$key")

        # 检查值是否为空
        if [ -z "$value" ]; then
            missing_keys+=("$key") # 将失败的 key 加入数组
            validation_passed=false
        fi
    done

    # 4. 总结并返回结果
    if [ "$validation_passed" = true ]; then
        echo "配置检查成功：所有必需属性均已配置。"
        return 0
    else
        echo "==================================================" >&2
        echo "【配置检查失败】" >&2
        echo "错误：在配置文件中缺少以下一个或多个必需属性，或其值为空：" >&2
        echo "" >&2
        echo "  配置文件路径: $CONF_FILE" >&2
        echo "" >&2
        echo "  缺失的属性:" >&2
        for key in "${missing_keys[@]}"; do
            echo "    - ${key}" >&2
        done
        echo "" >&2
        echo "请在【flink-conf.yaml】中配置对应属性。" >&2
        echo "==================================================" >&2
        return 1
    fi
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
              # 调用验证函数
              validate_flink_config
              # 检查验证结果 ($? 存储了上一个命令的返回值)
              if [ $? -ne 0 ]; then
                  echo "错误：Flink 配置验证失败，脚本终止。" >&2
                  return 1 # 或者 exit 1
              fi
              echo "配置验证通过，继续执行..."
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
set_hadoop_env() {
    # 2. 函数参数接收用户选择
    local hadoop_choice="$MON_HADOOP_CLIENT_SOURCE"  # 通过参数接收选择

    # 3. 参数验证
    if [ -z "$hadoop_choice" ]; then
        echo "错误：必须提供MON_HADOOP_CLIENT_SOURCE选择参数！" >&2
        echo "用法: 在config.sh中增加参数: MON_HADOOP_CLIENT_SOURCE=<选择>" >&2
        echo "可选值: \"操作系统环境自带\" 或 \"当前工具内置\"" >&2
        return 1
    fi

    # 4. 根据选择执行相应逻辑[1,2](@ref)
    case "$hadoop_choice" in
        "操作系统环境自带")
            # 检查 'hadoop' 命令是否存在于当前 PATH
            if command -v hadoop >/dev/null 2>&1; then
                echo "使用操作系统环境自带的 Hadoop客户端。" >&2
                # 打印当前的 HADOOP_CONF_DIR (如果已设置)
                if [ -n "$HADOOP_CONF_DIR" ]; then
                    echo "当前的 HADOOP_CONF_DIR 路径是: $HADOOP_CONF_DIR" >&2
                else
                    echo "警告：HADOOP_CONF_DIR 环境变量未设置。将依赖 Hadoop 默认配置。" >&2
                fi
            else
                echo "错误：'hadoop' 命令不存在于您的系统 PATH 中。" >&2
                return 1
            fi
            ;;

        "当前工具内置")
            echo "使用当前工具内置的 Hadoop 环境。" >&2

            # 检查 $CLOUD_HOME 是否存在[2](@ref)
            if [ -z "$CLOUD_HOME" ]; then
                echo "错误：CLOUD_HOME 环境变量未设置！无法使用内置 Hadoop。" >&2
                return 1
            fi

            # 设置临时的环境变量
            HADOOP_HOME="$CLOUD_HOME/mids/hadoopclient/hadoop-3.3.4"
            export HADOOP_HOME
            echo "HADOOP_HOME 已重置为: $HADOOP_HOME" >&2

            # 打印当前的 HADOOP_CONF_DIR (如果已设置)
            if [ -n "$HADOOP_CONF_DIR" ]; then
                echo "当前的 HADOOP_CONF_DIR 路径是: $HADOOP_CONF_DIR" >&2
            else
                echo "警告：HADOOP_CONF_DIR 环境变量未设置。将依赖 Hadoop 默认配置。" >&2
            fi
            MON_HADOOP_BIN_PATH="$HADOOP_HOME/bin/"
            echo "当前的 MON_HADOOP_BIN_PATH 路径是: $MON_HADOOP_BIN_PATH" >&2
            export MON_HADOOP_BIN_PATH
            ;;
        *)
            echo "错误：无效选择 '$hadoop_choice'！" >&2
            echo "请使用: \"操作系统环境自带\" 或 \"当前工具内置\"" >&2
            return 1
            ;;
    esac
}
# 假设 CLOUD_HOME 和 YARN_APP_TYPE 是已设置的环境变量
# export CLOUD_HOME="/opt/merit_cloud"
# export YARN_APP_TYPE="FLINK"

#
# @description: 查询 YARN 上的特定类型应用
#
# 该函数会首先提示用户选择 Hadoop 环境 (系统自带或工具内置)。
# 1. "系统自带": 会检查 hadoop 命令是否存在，并打印 classpath。
# 2. "工具内置": 会临时设置 HADOOP_HOME 和 PATH。
#
get_yarn_apps_by_type() {
    # 1. 检查必要的外部变量
    if [ -z "$YARN_APP_TYPE" ]; then
        echo "错误：YARN_APP_TYPE 环境变量未设置！" >&2
        return 1
    fi
    # 执行 YARN 查询
    echo "--------------------------------------------------" >&2
    echo "正在执行 YARN 查询 (yarn application -list)..." >&2
    "${MON_HADOOP_BIN_PATH}"yarn application -list | grep "$YARN_APP_TYPE" | awk '{print $1, $2}'
    return $?
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

#设置hadoop 环境
set_hadoop_env
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