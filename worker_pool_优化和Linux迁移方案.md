# Worker Pool 优化和 Linux 迁移方案

## 一、当前 Worker Pool 设置分析

### 1.1 当前配置

```go
const (
    maxConcurrency = 400  // 最大并发数（Worker 数量）
    batchSize      = 100  // 批量保存大小
    retryCount     = 3    // 重试次数
    retryDelay     = 2 * time.Second // 重试延迟
    rateLimit      = 200  // 速率限制：每10秒200个请求
)
```

### 1.2 架构分析

**当前架构：**
- **Worker Pool**: 400 个并发 Goroutine
- **Channel 缓冲**:
  - `addressCh`: 400 (与 worker 数量相同)
  - `resultCh`: 800 (worker 数量的 2 倍)
  - `errorCh`: 400
- **速率限制**: 每 10 秒 200 个请求 = 每秒 20 个请求
- **批量保存**: 每 100 条记录保存一次

### 1.3 性能瓶颈分析

| 项目 | 当前值 | 潜在问题 |
|------|--------|---------|
| **并发数** | 400 | 可能过高，导致资源竞争 |
| **速率限制** | 20 req/s | 400 个 worker 共享，可能造成等待 |
| **批量大小** | 100 | 可能偏小，数据库写入频繁 |
| **Channel 缓冲** | 400-800 | 可能造成内存压力 |

## 二、优化建议

### 2.1 动态配置化

**问题**: 当前参数硬编码在代码中，无法根据环境调整

**解决方案**: 将参数移到配置文件

```yaml
# etc/tracker.yaml
query_qysy:
  max_concurrency: 400    # 最大并发数
  batch_size: 100         # 批量保存大小
  retry_count: 3          # 重试次数
  retry_delay: 2          # 重试延迟（秒）
  rate_limit: 200         # 速率限制（每10秒）
```

### 2.2 性能优化建议

#### 优化 1: 调整并发数和速率限制比例

**当前问题**: 
- 400 个 worker，但速率限制只有 20 req/s
- 每个 worker 平均等待时间 = 400/20 = 20 秒

**建议**:
```go
// 方案 A: 降低并发数，提高效率
maxConcurrency = 200  // 降低到 200
rateLimit = 200       // 保持速率限制
// 每个 worker 平均等待 = 200/20 = 10 秒

// 方案 B: 提高速率限制（如果 API 支持）
maxConcurrency = 400  // 保持并发数
rateLimit = 400      // 提高到 400（每10秒400个请求 = 40 req/s）
```

#### 优化 2: 增加批量保存大小

**当前**: `batchSize = 100`

**建议**: 
```go
batchSize = 200  // 或 500，减少数据库写入次数
```

**优势**:
- 减少数据库连接开销
- 提高批量写入效率
- 降低数据库压力

#### 优化 3: 优化 Channel 缓冲

**当前**:
```go
addressCh := make(chan dao.AddressWithEquity, maxConcurrency)  // 400
resultCh := make(chan *dao.UserQysy, maxConcurrency*2)         // 800
```

**建议**:
```go
// 根据实际数据量动态调整
addressCh := make(chan dao.AddressWithEquity, maxConcurrency)
resultCh := make(chan *dao.UserQysy, maxConcurrency*3)  // 增加到 3 倍，减少阻塞
```

#### 优化 4: 添加自适应限流

**当前**: 固定速率限制

**建议**: 根据 API 响应时间和错误率动态调整

```go
// 伪代码示例
if errorRate > 0.1 {  // 错误率超过 10%
    rateLimit = rateLimit * 0.8  // 降低 20%
} else if avgResponseTime < 100ms {
    rateLimit = rateLimit * 1.1  // 提高 10%
}
```

### 2.3 代码优化实现

创建优化后的配置文件结构：

```go
// internal/config/config.go 添加
type QueryQysyConfig struct {
    MaxConcurrency int `yaml:"max_concurrency"`
    BatchSize      int `yaml:"batch_size"`
    RetryCount     int `yaml:"retry_count"`
    RetryDelay     int `yaml:"retry_delay"`  // 秒
    RateLimit      int `yaml:"rate_limit"`
}
```

## 三、Linux 服务器迁移方案

### 3.1 准备工作

#### 1. 编译 Linux 版本

```bash
# 在 Windows 上交叉编译（或直接在 Linux 上编译）
# 设置环境变量
export GOOS=linux
export GOARCH=amd64

# 编译
go build -buildvcs=false -o tracker-linux cmd/tracker/main.go

# 或者使用 Makefile
make build-linux
```

#### 2. 创建 Makefile（可选）

```makefile
# Makefile
.PHONY: build build-linux build-windows clean

build:
	go build -buildvcs=false -o tracker cmd/tracker/main.go

build-linux:
	GOOS=linux GOARCH=amd64 go build -buildvcs=false -o tracker-linux cmd/tracker/main.go

build-windows:
	GOOS=windows GOARCH=amd64 go build -buildvcs=false -o tracker.exe cmd/tracker/main.go

clean:
	rm -f tracker tracker-linux tracker.exe
```

### 3.2 部署步骤

#### 步骤 1: 传输文件到 Linux 服务器

```bash
# 使用 SCP 传输
scp tracker-linux user@server:/opt/tracker/
scp -r etc/ user@server:/opt/tracker/
scp -r logs/ user@server:/opt/tracker/  # 如果存在

# 或使用 rsync
rsync -avz tracker-linux user@server:/opt/tracker/
rsync -avz etc/ user@server:/opt/tracker/etc/
```

#### 步骤 2: 设置执行权限

```bash
ssh user@server
cd /opt/tracker
chmod +x tracker-linux
```

#### 步骤 3: 创建 systemd 服务文件

创建 `/etc/systemd/system/tracker.service`:

```ini
[Unit]
Description=Hyper Tracker Service
After=network.target postgresql.service

[Service]
Type=simple
User=tracker
Group=tracker
WorkingDirectory=/opt/tracker
ExecStart=/opt/tracker/tracker-linux query-qysy --config /opt/tracker/etc/tracker.yaml
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

# 资源限制
LimitNOFILE=65535
LimitNPROC=4096

# 环境变量
Environment="GOMAXPROCS=8"

[Install]
WantedBy=multi-user.target
```

#### 步骤 4: 创建用户和目录

```bash
# 创建 tracker 用户
sudo useradd -r -s /bin/false tracker

# 创建目录
sudo mkdir -p /opt/tracker/{etc,logs,data}
sudo chown -R tracker:tracker /opt/tracker

# 设置日志目录权限
sudo chmod 755 /opt/tracker/logs
```

#### 步骤 5: 配置数据库连接

编辑 `/opt/tracker/etc/tracker.yaml`:

```yaml
# 修改数据库连接（根据实际情况）
dsn: postgres://postgres:password@localhost:5432/tracker?sslmode=disable

# 修改日志路径
log:
  output: "/opt/tracker/logs/tracker.log"
```

#### 步骤 6: 启动服务

```bash
# 重新加载 systemd
sudo systemctl daemon-reload

# 启动服务
sudo systemctl start tracker

# 设置开机自启
sudo systemctl enable tracker

# 查看状态
sudo systemctl status tracker

# 查看日志
sudo journalctl -u tracker -f
```

### 3.3 监控和维护

#### 创建监控脚本

```bash
#!/bin/bash
# /opt/tracker/scripts/check_tracker.sh

SERVICE_NAME="tracker"
LOG_FILE="/opt/tracker/logs/tracker.log"

# 检查服务状态
if ! systemctl is-active --quiet $SERVICE_NAME; then
    echo "Tracker service is not running, restarting..."
    systemctl restart $SERVICE_NAME
    sleep 5
    
    if systemctl is-active --quiet $SERVICE_NAME; then
        echo "Tracker service restarted successfully"
    else
        echo "Failed to restart tracker service"
        # 发送告警（可以集成邮件、钉钉等）
    fi
fi

# 检查日志文件大小
if [ -f "$LOG_FILE" ]; then
    SIZE=$(du -m "$LOG_FILE" | cut -f1)
    if [ $SIZE -gt 1000 ]; then
        echo "Log file is too large: ${SIZE}MB"
        # 可以添加日志轮转逻辑
    fi
fi
```

#### 设置定时任务

```bash
# 添加到 crontab
crontab -e

# 每 5 分钟检查一次服务状态
*/5 * * * * /opt/tracker/scripts/check_tracker.sh >> /opt/tracker/logs/check.log 2>&1
```

### 3.4 性能调优（Linux 环境）

#### 1. 系统参数优化

```bash
# 编辑 /etc/sysctl.conf
sudo vim /etc/sysctl.conf

# 添加以下配置
net.core.somaxconn = 4096
net.ipv4.tcp_max_syn_backlog = 4096
net.ipv4.ip_local_port_range = 10000 65535
fs.file-max = 1000000

# 应用配置
sudo sysctl -p
```

#### 2. 用户限制优化

```bash
# 编辑 /etc/security/limits.conf
sudo vim /etc/security/limits.conf

# 添加以下配置
tracker soft nofile 65535
tracker hard nofile 65535
tracker soft nproc 4096
tracker hard nproc 4096
```

#### 3. 数据库连接优化

在 `tracker.yaml` 中配置连接池：

```yaml
# 如果支持，可以添加数据库连接池配置
database:
  max_open_conns: 100
  max_idle_conns: 10
  conn_max_lifetime: 3600  # 秒
```

## 四、推荐的优化配置

### 4.1 生产环境推荐配置

```yaml
query_qysy:
  max_concurrency: 300    # 根据服务器 CPU 核心数调整（建议 CPU核心数 * 50-100）
  batch_size: 200         # 增加批量大小
  retry_count: 3
  retry_delay: 2
  rate_limit: 300         # 根据 API 限制调整
```

### 4.2 计算公式

```
推荐并发数 = CPU核心数 × 50-100
速率限制 = API限制 / 安全系数（建议 0.8）
批量大小 = 根据数据库性能调整（建议 200-500）
```

### 4.3 监控指标

建议监控以下指标：
- CPU 使用率
- 内存使用率
- 数据库连接数
- API 响应时间
- 错误率
- 处理速度（条/秒）

## 五、迁移检查清单

- [ ] 编译 Linux 版本
- [ ] 传输文件到服务器
- [ ] 创建 systemd 服务文件
- [ ] 配置数据库连接
- [ ] 设置文件权限
- [ ] 测试服务启动
- [ ] 配置日志轮转
- [ ] 设置监控脚本
- [ ] 配置定时任务
- [ ] 性能测试和调优

## 六、常见问题

### Q1: 如何查看服务日志？
```bash
# 使用 journalctl
sudo journalctl -u tracker -f

# 或查看日志文件
tail -f /opt/tracker/logs/tracker.log
```

### Q2: 如何重启服务？
```bash
sudo systemctl restart tracker
```

### Q3: 如何修改配置？
```bash
# 编辑配置文件
sudo vim /opt/tracker/etc/tracker.yaml

# 重启服务使配置生效
sudo systemctl restart tracker
```

### Q4: 如何查看资源使用情况？
```bash
# 查看进程资源使用
top -p $(pgrep tracker-linux)

# 或使用 htop
htop -p $(pgrep tracker-linux)
```
