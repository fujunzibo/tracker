package commands

import (
	"context"
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"hyper-tracker/pkg/hyper"
	"hyper-tracker/pkg/log"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

var (
	updateAccountMaxConcurrency = 30   // 最大并发数
	updateAccountBatchSize      = 200  // 批量保存大小
	updateAccountRetryCount     = 5    // 重试次数
	updateAccountRetryDelay     = 2 * time.Second // 重试延迟
	updateAccountWorkerDelay    = 200 * time.Millisecond // Worker启动延迟
)

var updateAccountCmd = &cobra.Command{
	Use:   "update-account",
	Short: "更新用户账户数据",
	Long: `从 tb_user_account 表中读取所有地址，通过 Hyperliquid API 查询并更新以下字段：
  1. 用户总权益 (total_equity)
  2. 可用余额 (available_balance)
  3. 已用保证金 (used_margin)
  4. 累计盈亏 (total_pnl)
  5. 累计手续费 (total_fee)
  6. 7日内累计交易次数 (trade_count_7d)

使用示例：
  tracker update-account --config etc/tracker.yaml`,
	Run: func(cmd *cobra.Command, args []string) {
		runUpdateAccount()
	},
}

func init() {
	rootCmd.AddCommand(updateAccountCmd)
}

func runUpdateAccount() {
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

	// 创建表（如果不存在）
	err = dao.CreateUserAccountTable()
	if err != nil {
		slog.Error("创建表失败", "error", err)
		exitWithError(err)
	}
	slog.Info("表创建/检查完成")

	// 创建 Hyperliquid API 客户端
	var localAddrs []string
	// 注意：update-account 命令暂不支持 --local-ip 参数，使用配置文件中的设置
	localAddrs = cfg.Network.LocalAddrs
	if len(localAddrs) > 0 {
		slog.Info("使用配置文件中的本地IP地址列表", "ips", localAddrs)
	}

	hyperClient := hyper.WithLocalAddrsAndProxies(localAddrs, cfg.Network.Proxies)

	// 从数据库读取所有地址
	slog.Info("从 tb_user_account 表读取所有地址...")
	addresses, err := dao.GetAllAddressesFromUserAccount()
	if err != nil {
		slog.Error("读取地址失败", "error", err)
		exitWithError(err)
	}

	if len(addresses) == 0 {
		slog.Warn("表中没有地址，无需更新")
		return
	}

	slog.Info("成功读取地址", "count", len(addresses))

	// 创建 Worker Pool 来并发查询账户数据
	resultCh := make(chan *dao.UserAccount, updateAccountBatchSize*2)
	var wg sync.WaitGroup
	var processedCount int64
	var successCount int64
	var errorCount int64

	// 创建地址通道
	addressCh := make(chan string, updateAccountMaxConcurrency*2)
	go func() {
		defer close(addressCh)
		for _, addr := range addresses {
			addressCh <- addr
		}
	}()

	// 启动 Worker
	for i := 0; i < updateAccountMaxConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// 延迟启动，避免同时启动所有 worker
			time.Sleep(updateAccountWorkerDelay * time.Duration(workerID))

			for address := range addressCh {
				atomic.AddInt64(&processedCount, 1)
				
				// 查询账户数据（使用 Info API）
				data, err := queryAccountDataFromInfoAPI(hyperClient, address, slog.Default())
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					if err == hyper.ErrNull {
						slog.Debug("API 返回 null", "address", address, "worker", workerID)
					} else {
						slog.Warn("查询账户数据失败", 
							"address", address, 
							"error", err,
							"worker", workerID)
					}
					continue
				}

				// 发送结果
				resultCh <- data
				atomic.AddInt64(&successCount, 1)

				// 每处理一定数量后输出进度
				if processed := atomic.LoadInt64(&processedCount); processed%100 == 0 {
					slog.Info("更新进度",
						"processed", processed,
						"total", len(addresses),
						"success", atomic.LoadInt64(&successCount),
						"error", atomic.LoadInt64(&errorCount),
						"progress", float64(processed)/float64(len(addresses))*100)
				}
			}
		}(i)
	}

	// 等待所有 worker 完成
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// 批量保存结果
	saveUpdateResults(resultCh, len(addresses))

	slog.Info("账户数据更新完成",
		"total_addresses", len(addresses),
		"processed", atomic.LoadInt64(&processedCount),
		"success", atomic.LoadInt64(&successCount),
		"error", atomic.LoadInt64(&errorCount))
}

// saveUpdateResults 批量保存更新结果
func saveUpdateResults(resultCh <-chan *dao.UserAccount, total int) {
	var results []*dao.UserAccount
	var savedCount int

	for result := range resultCh {
		results = append(results, result)

		// 批量保存
		if len(results) >= updateAccountBatchSize {
			if err := dao.BatchSaveUserAccount(results); err != nil {
				slog.Error("批量保存失败", "error", err, "count", len(results))
			} else {
				savedCount += len(results)
				slog.Info("批量保存成功",
					"count", len(results),
					"total_saved", savedCount,
					"progress", float64(savedCount)/float64(total)*100)
			}
			results = results[:0]
		}
	}

	// 保存剩余数据
	if len(results) > 0 {
		if err := dao.BatchSaveUserAccount(results); err != nil {
			slog.Error("批量保存剩余数据失败", "error", err, "count", len(results))
		} else {
			savedCount += len(results)
			slog.Info("批量保存剩余数据成功",
				"count", len(results),
				"total_saved", savedCount)
		}
	}
}
