package commands

import (
	"context"
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"hyper-tracker/internal/service/collector"
	"hyper-tracker/pkg/hyper"
	"log/slog"
	"runtime"
	"time"

	"github.com/spf13/cobra"
)

var (
	setID int
	days  int
)

var toolsCmd = &cobra.Command{
	Use:   "tools",
	Short: "维护工具命令",
	Long: `提供各种数据维护和修复工具。

可用命令：
  refresh   刷新数据缓存
  fix       修复数据问题`,
}

var refreshCmd = &cobra.Command{
	Use:   "refresh [target]",
	Short: "刷新数据缓存",
	Long: `刷新各种数据缓存和视图。

可用目标：
  summary         刷新汇总数据
  cache           刷新缓存视图
  skip-address    刷新排除地址列表
  vault-skips     刷新 vault 排除地址
  fills           刷新地址集成交记录（需要 --set-id）

使用示例：
  tracker tools refresh summary
  tracker tools refresh cache
  tracker tools refresh skip-address
  tracker tools refresh vault-skips
  tracker tools refresh fills --set-id 1`,
	ValidArgs: []string{"summary", "cache", "skip-address", "vault-skips", "fills"},
	Args:      cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		runRefresh(args[0])
	},
}

var fixCmd = &cobra.Command{
	Use:   "fix [target]",
	Short: "修复数据问题",
	Long: `修复各种数据问题。

可用目标：
  zero-portfolio  修复零值 Portfolio 数据

使用示例：
  tracker tools fix zero-portfolio --days 365`,
	ValidArgs: []string{"zero-portfolio"},
	Args:      cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		runFix(args[0])
	},
}

func init() {
	rootCmd.AddCommand(toolsCmd)
	toolsCmd.AddCommand(refreshCmd)
	toolsCmd.AddCommand(fixCmd)

	// refresh 命令的标志
	refreshCmd.Flags().IntVar(&setID, "set-id", 0, "地址集合 ID（用于 fills 目标）")

	// fix 命令的标志
	fixCmd.Flags().IntVar(&days, "days", 365, "回溯天数")
}

func runRefresh(target string) {
	cfg, err := config.LoadConfig(GetConfigFile())
	if err != nil {
		exitWithError(err)
	}

	ctx := context.Background()
	err = dao.InitDB(ctx, cfg.DSN)
	if err != nil {
		slog.Error("初始化数据库连接失败", "error", err)
		exitWithError(err)
	}

	switch target {
	case "summary":
		slog.Info("开始刷新汇总数据...")
		dao.RefreshAllSummary(runtime.NumCPU())
		slog.Info("汇总数据刷新完成")

	case "cache":
		slog.Info("开始刷新缓存视图...")
		collector.RefreshExec(cfg.Collect.HyperExecs)
		slog.Info("缓存视图刷新完成")

	case "skip-address":
		slog.Info("开始刷新排除地址列表...")
		err = dao.RefreshSkipAddress()
		if err != nil {
			slog.Error("刷新排除地址失败", "error", err)
			exitWithError(err)
		}
		slog.Info("排除地址列表刷新完成")

	case "vault-skips":
		slog.Info("开始刷新 vault 排除地址...")
		vaults, err := hyper.GetAvailableValuts()
		if err != nil {
			slog.Error("获取 vaults 失败", "error", err)
			exitWithError(err)
		}
		var values []dao.AddSkipAddress
		for _, vault := range vaults {
			values = append(values, dao.AddSkipAddress{
				Address: vault,
				Type:    1,
				Remark:  "vault",
			})
		}
		err = dao.BatchSaveSkipAddresses(values)
		if err != nil {
			slog.Error("保存 vaults 失败", "error", err)
			exitWithError(err)
		}
		slog.Info("vault 排除地址刷新完成", "count", len(vaults))

	case "fills":
		if setID == 0 {
			slog.Error("刷新 fills 需要指定 --set-id")
			exitWithError(err)
		}
		slog.Info("开始刷新地址集成交记录...", "setID", setID)
		hyperliquid := collector.NewHyperliquid(ctx, &cfg.Collect, &cfg.Network, nil)
		hyperliquid.UpdateAddressSetFills(setID)
		slog.Info("地址集成交记录刷新完成")

	default:
		slog.Error("未知的刷新目标", "target", target)
	}
}

func runFix(target string) {
	cfg, err := config.LoadConfig(GetConfigFile())
	if err != nil {
		exitWithError(err)
	}

	ctx := context.Background()
	err = dao.InitDB(ctx, cfg.DSN)
	if err != nil {
		slog.Error("初始化数据库连接失败", "error", err)
		exitWithError(err)
	}

	switch target {
	case "zero-portfolio":
		slog.SetLogLoggerLevel(slog.LevelDebug)
		slog.Info("开始修复零值 Portfolio...", "days", days)
		hyperliquid := collector.NewHyperliquid(ctx, &cfg.Collect, &cfg.Network, nil)
		startDate := time.Now().AddDate(0, 0, -days)
		hyperliquid.FixZeroPortfolio(startDate)
		slog.Info("零值 Portfolio 修复完成")

	default:
		slog.Error("未知的修复目标", "target", target)
	}
}
