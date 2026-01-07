#!/bin/bash

# 多进程启动脚本 - 使用4个不同的IP地址启动4个进程
# 使用方法: ./start-query-account-multi.sh [配置文件路径]

CONFIG_FILE="${1:-etc/tracker.yaml}"
LOG_DIR="logs"
PID_DIR="/tmp/tracker-pids"

# 创建日志和PID目录
mkdir -p "$LOG_DIR"
mkdir -p "$PID_DIR"

# 4个IP地址
IPS=(
    "156.254.126.67"
    "45.204.11.226"
    "154.211.23.227"
    "154.91.35.10"
)

# 启动函数
start_process() {
    local ip=$1
    local index=$2
    local log_file="${LOG_DIR}/query-account-${index}.log"
    local pid_file="${PID_DIR}/query-account-${index}.pid"
    
    echo "启动进程 $index，使用IP: $ip"
    echo "日志文件: $log_file"
    echo "PID文件: $pid_file"
    
    # 后台启动进程
    nohup ./tracker-linux query-account \
        --config "$CONFIG_FILE" \
        --local-ip "$ip" \
        > "$log_file" 2>&1 &
    
    local pid=$!
    echo $pid > "$pid_file"
    echo "进程 $index 已启动，PID: $pid"
    echo "---"
}

# 停止所有进程
stop_all() {
    echo "停止所有进程..."
    for pid_file in "$PID_DIR"/query-account-*.pid; do
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            if ps -p "$pid" > /dev/null 2>&1; then
                echo "停止进程 PID: $pid"
                kill "$pid"
            fi
            rm -f "$pid_file"
        fi
    done
    echo "所有进程已停止"
}

# 检查进程状态
status() {
    echo "检查进程状态..."
    for i in "${!IPS[@]}"; do
        local index=$((i+1))
        local pid_file="${PID_DIR}/query-account-${index}.pid"
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            if ps -p "$pid" > /dev/null 2>&1; then
                echo "进程 $index (IP: ${IPS[i]}) - 运行中 (PID: $pid)"
            else
                echo "进程 $index (IP: ${IPS[i]}) - 已停止"
                rm -f "$pid_file"
            fi
        else
            echo "进程 $index (IP: ${IPS[i]}) - 未启动"
        fi
    done
}

# 主逻辑
case "${2:-start}" in
    start)
        echo "=========================================="
        echo "启动多进程查询账户数据"
        echo "配置文件: $CONFIG_FILE"
        echo "=========================================="
        
        # 检查可执行文件
        if [ ! -f "./tracker-linux" ]; then
            echo "错误: tracker-linux 可执行文件不存在"
            exit 1
        fi
        
        # 检查配置文件
        if [ ! -f "$CONFIG_FILE" ]; then
            echo "错误: 配置文件不存在: $CONFIG_FILE"
            exit 1
        fi
        
        # 启动所有进程
        for i in "${!IPS[@]}"; do
            start_process "${IPS[i]}" $((i+1))
            sleep 1  # 错开启动时间
        done
        
        echo ""
        echo "所有进程已启动！"
        echo "查看日志: tail -f $LOG_DIR/query-account-*.log"
        echo "查看状态: $0 status"
        echo "停止所有: $0 stop"
        ;;
    stop)
        stop_all
        ;;
    status)
        status
        ;;
    restart)
        stop_all
        sleep 2
        "$0" "$CONFIG_FILE" start
        ;;
    *)
        echo "使用方法: $0 [配置文件] [start|stop|status|restart]"
        echo ""
        echo "示例:"
        echo "  $0 etc/tracker.yaml start    # 启动所有进程"
        echo "  $0 etc/tracker.yaml stop     # 停止所有进程"
        echo "  $0 etc/tracker.yaml status   # 查看进程状态"
        echo "  $0 etc/tracker.yaml restart  # 重启所有进程"
        exit 1
        ;;
esac
