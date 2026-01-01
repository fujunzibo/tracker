package commands

import (
	"context"
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"hyper-tracker/internal/service/collector"
	"hyper-tracker/pkg/lifecycle"
	"hyper-tracker/pkg/log"
	"log/slog"
	"time"

	"github.com/spf13/cobra"
)

var collectCmd = &cobra.Command{
	Use:   "collect",
	Short: "启动数据采集服务",
	Long: `启动数据采集服务，包括 CMM 钱包数据采集和 Hyperliquid Portfolio 数据采集。

服务功能：
  - CMM 钱包地址采集
  - Hyperliquid Portfolio 历史数据采集
  - 交易量统计
  - 持仓数据更新

使用示例：
  tracker collect --config etc/tracker.yaml`,
	Run: func(cmd *cobra.Command, args []string) {
		runCollect()
	},
}

func init() {
	rootCmd.AddCommand(collectCmd)
}

func runCollect() {
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

	// 创建并启动采集服务
	svc := collector.NewService(ctx, cfg)
	svc.Start()

	slog.Info("数据采集服务已启动",
		"segments", cfg.Collect.Segments,
		"interval", cfg.Collect.Interval,
		"workerCount", cfg.Collect.WorkerCount)

	// 等待退出信号
	lifecycle.Shutdown(time.Minute, func() {
		slog.Info("正在关闭采集服务...")
		cancel()
	})
}
