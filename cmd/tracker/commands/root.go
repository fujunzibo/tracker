package commands

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	cfgFile string
)

// rootCmd 代表没有调用子命令时的基础命令
var rootCmd = &cobra.Command{
	Use:   "tracker",
	Short: "Hyper Tracker - 交易数据采集与反向跟单系统",
	Long: `Hyper Tracker 是一个用于追踪和分析 Hyperliquid 交易数据的服务系统，
支持数据采集、反向跟单、HTTP API 服务等功能。

使用示例：
  tracker server           # 启动 HTTP API 服务
  tracker collect          # 启动数据采集服务
  tracker tools refresh    # 刷新数据缓存`,
}

// Execute 添加所有子命令到 root 命令并适当设置标志
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// 全局配置文件标志
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "etc/tracker.yaml", "配置文件路径")
}

// GetConfigFile 返回配置文件路径
func GetConfigFile() string {
	return cfgFile
}

// exitWithError 打印错误信息并退出
func exitWithError(err error) {
	fmt.Fprintf(os.Stderr, "错误: %v\n", err)
	os.Exit(1)
}
