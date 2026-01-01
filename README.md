# Hyper Tracker

Hyper Tracker 是一个用于追踪和分析 Hyperliquid 交易数据的服务系统，支持数据采集、反向跟单、HTTP API 服务等功能。

## 系统要求

- Go 1.24.0 或更高版本
- PostgreSQL 12+ (推荐使用 TimescaleDB 扩展)
- 网络连接（用于访问 CMM API 和 Hyperliquid API）

## 快速开始

### 1. 安装 Go 依赖

```bash
go mod download
```

### 2. 准备数据库

#### 2.1 安装 PostgreSQL 和 TimescaleDB

确保已安装 PostgreSQL 并启用 TimescaleDB 扩展：

```sql
-- 在 PostgreSQL 中执行
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

#### 2.2 创建数据库

```sql
CREATE DATABASE hyper_tracker;
```

#### 2.3 初始化数据库表结构

```bash
# 使用 psql 执行 SQL 脚本
psql -U postgres -d hyper_tracker -f dao/tables.sql
```

或者直接在 PostgreSQL 客户端中执行 `dao/tables.sql` 文件的内容。

### 3. 配置应用

#### 3.1 创建配置文件

```bash
# 复制示例配置文件
mkdir -p etc
cp etc/tracker.yaml.example etc/tracker.yaml
```

#### 3.2 编辑配置文件

编辑 `etc/tracker.yaml`，至少需要配置：

- **dsn**: PostgreSQL 数据库连接字符串
- **cmm**: CMM API 的访问令牌和刷新令牌（如果需要使用 CMM 功能）

示例数据库连接字符串：
```
postgres://用户名:密码@localhost:5432/hyper_tracker?sslmode=disable
```

### 4. 构建应用

```bash
go build -o tracker.exe ./cmd/tracker
```

在 Linux/macOS 上：
```bash
go build -o tracker ./cmd/tracker
```

### 5. 运行应用

应用提供以下命令：

#### 启动完整服务（推荐）

启动 HTTP API 服务 + 数据采集服务：

```bash
./tracker server
# 或指定配置文件
./tracker server --config etc/tracker.yaml
```

#### 仅启动数据采集服务

```bash
./tracker collect
# 或指定配置文件
./tracker collect --config etc/tracker.yaml
```

#### 查看帮助

```bash
./tracker --help
./tracker server --help
./tracker collect --help
```

## 主要功能

### 数据采集服务

- CMM 钱包地址采集
- Hyperliquid Portfolio 历史数据采集
- 交易量统计
- 持仓数据更新

### HTTP API 服务

- 数据查询接口
- 地址管理
- 跟单控制

## 配置文件说明

配置文件使用 YAML 格式，默认路径为 `etc/tracker.yaml`。

主要配置项：

- **dsn**: 数据库连接字符串（必需）
- **server**: HTTP 服务器配置
- **cmm**: CMM API 配置
- **log**: 日志配置
- **collect**: 数据采集配置
- **follow**: 跟单配置（可选）

详细配置示例请参考 `etc/tracker.yaml.example`。

## 开发

### 项目结构

```
.
├── cmd/tracker/          # 主程序入口
│   ├── main.go          # 程序入口
│   └── commands/        # CLI 命令
├── dao/                  # 数据访问层
├── internal/             # 内部包
│   ├── config/          # 配置管理
│   └── service/          # 业务服务
└── pkg/                  # 公共包
```

### 运行测试

```bash
go test ./...
```

## 故障排除

### 数据库连接失败

- 检查 PostgreSQL 服务是否运行
- 验证数据库连接字符串是否正确
- 确认数据库用户有足够权限

### 配置文件未找到

- 确认配置文件路径正确
- 使用 `--config` 参数指定配置文件路径

### TimescaleDB 扩展未安装

如果遇到 `create_hypertable` 函数不存在的错误，需要安装 TimescaleDB 扩展：

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

## 许可证

[根据实际情况添加许可证信息]




