package commands

import (
	"context"
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"hyper-tracker/internal/service/api"
	"hyper-tracker/internal/service/collector"
	"hyper-tracker/pkg/lifecycle"
	"hyper-tracker/pkg/log"
	"log/slog"
	"time"

	"github.com/spf13/cobra"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "启动 HTTP API 服务",
	Long: `启动 HTTP API 服务，提供数据查询、地址管理、跟单控制等接口。

服务包含：
  - 数据采集服务（CMM + Hyperliquid）
  - HTTP API 服务（数据查询、管理界面）

使用示例：
  tracker server --config etc/tracker.yaml`,
	Run: func(cmd *cobra.Command, args []string) {
		runServer()
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
}

func runServer() {
	// 加载配置
	cfg, err := config.LoadConfig(GetConfigFile())
	if err != nil {
		exitWithError(err)
	}

	// 初始化日志
	log.Setup(&cfg.Log)

	// 初始化数据库
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = dao.InitDB(ctx, cfg.DSN)
	if err != nil {
		slog.Error("初始化数据库连接失败", "error", err)
		exitWithError(err)
	}

	// 创建数据采集服务
	collectorSvc := collector.NewService(ctx, cfg)
	collectorSvc.Start()

	// 创建 HTTP API 服务
	apiSvc := api.NewService(cfg, collectorSvc)
	apiSvc.Start()

	slog.Info("服务已启动",
		"api_listen", cfg.Server.Listen,
		"cmm_segments", cfg.Collect.Segments,
		"cmm_interval", cfg.Collect.Interval)

	// 等待退出信号
	lifecycle.Shutdown(time.Minute, func() {
		slog.Info("正在关闭服务...")
		cancel()
	})
}
