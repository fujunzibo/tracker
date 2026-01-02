package commands

import (
	"context"
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"hyper-tracker/pkg/hyper"
	"hyper-tracker/pkg/log"
	"hyper-tracker/pkg/utils"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

var queryQysyCmd = &cobra.Command{
	Use:   "query-qysy",
	Short: "查询用户权益损益数据",
	Long: `从数据库读取钱包地址，通过 Hyperliquid API 查询以下数据并保存到 tb_user_qysy 表：
  1. 用户权益 - 从 CrossMarginSummary.AccountValue 获取
  2. 用户损益(累计亏损) - 所有亏损订单的累计金额
  3. 整体盈亏比(所有盈利订单累计/所有亏损订单累计)
  4. 手续费占比(手续费累计/总亏损)
  5. 7日内平均交易次数

使用示例：
  tracker query-qysy --config etc/tracker.yaml`,
	Run: func(cmd *cobra.Command, args []string) {
		runQueryQysy()
	},
}

func init() {
	rootCmd.AddCommand(queryQysyCmd)
}

const (
	maxConcurrency = 100  // 最大并发数
	batchSize      = 50   // 批量保存大小
	retryCount     = 3    // 重试次数
	retryDelay     = 2 * time.Second // 重试延迟
	rateLimit      = 200  // 速率限制：每10秒200个请求
)

func runQueryQysy() {
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
	err = dao.CreateUserQysyTable()
	if err != nil {
		slog.Error("创建表失败", "error", err)
		exitWithError(err)
	}
	slog.Info("表创建/检查完成")

	// 获取所有地址
	addresses, err := dao.GetAllAddresses()
	if err != nil {
		slog.Error("获取地址列表失败", "error", err)
		exitWithError(err)
	}
	total := len(addresses)
	slog.Info("获取到地址列表", "count", total)

	if total == 0 {
		slog.Info("没有地址需要处理")
		return
	}

	// 创建 Hyperliquid 客户端
	client := hyper.WithLocalAddrsAndProxies(cfg.Network.LocalAddrs, cfg.Network.Proxies)

	// 创建速率限制器
	rateLimiter := utils.NewRateLimiter(rateLimit)

	// 创建 Worker Pool
	addressCh := make(chan string, maxConcurrency)
	resultCh := make(chan *dao.UserQysy, maxConcurrency*2)
	errorCh := make(chan error, maxConcurrency)

	// 统计变量
	var (
		processedCount int64
		successCount   int64
		errorCount     int64
		wg             sync.WaitGroup
	)

	// 启动 Worker Goroutines
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			worker(ctx, client, rateLimiter, addressCh, resultCh, errorCh, workerID, &processedCount, &successCount, &errorCount)
		}(i)
	}

	// 启动结果收集和批量保存 Goroutine
	var saveWg sync.WaitGroup
	saveWg.Add(1)
	go func() {
		defer saveWg.Done()
		saveResults(resultCh, total)
	}()

	// 启动错误收集 Goroutine
	var errorWg sync.WaitGroup
	errorWg.Add(1)
	go func() {
		defer errorWg.Done()
		collectErrors(errorCh)
	}()

	// 发送地址到 channel
	startTime := time.Now()
	go func() {
		defer close(addressCh)
		for _, address := range addresses {
			addressCh <- address
		}
	}()

	// 等待所有 Worker 完成
	wg.Wait()
	close(resultCh)
	close(errorCh)

	// 等待保存和错误收集完成
	saveWg.Wait()
	errorWg.Wait()

	duration := time.Since(startTime)
	slog.Info("查询完成",
		"total", total,
		"processed", atomic.LoadInt64(&processedCount),
		"success", atomic.LoadInt64(&successCount),
		"errors", atomic.LoadInt64(&errorCount),
		"duration", duration,
		"avg_time", duration/time.Duration(total))
}

// worker 工作协程，处理地址查询
func worker(
	ctx context.Context,
	client *hyper.Client,
	rateLimiter *utils.RateLimiter,
	addressCh <-chan string,
	resultCh chan<- *dao.UserQysy,
	errorCh chan<- error,
	workerID int,
	processedCount, successCount, errorCount *int64,
) {
	logger := slog.With("worker", workerID)
	defer utils.Recovery(logger)

	for address := range addressCh {
		// 等待速率限制
		if err := rateLimiter.Wait(ctx); err != nil {
			if err == context.Canceled {
				return
			}
			logger.Error("速率限制等待失败", "error", err)
		}

		// 查询用户数据（带重试）
		data, err := queryUserDataWithRetry(ctx, client, address, logger)
		atomic.AddInt64(processedCount, 1)

		if err != nil {
			atomic.AddInt64(errorCount, 1)
			logger.Error("查询用户数据失败", "address", address, "error", err)
			errorCh <- err
			continue
		}

		atomic.AddInt64(successCount, 1)
		
		// 调试：输出前几个地址的权益值
		if atomic.LoadInt64(successCount) <= 5 {
			logger.Info("查询结果", "address", address, "user_equity", data.UserEquity, "total_loss", data.TotalLoss)
		}
		
		resultCh <- data

		// 每处理100个地址输出一次进度
		if atomic.LoadInt64(processedCount)%100 == 0 {
			logger.Info("处理进度",
				"processed", atomic.LoadInt64(processedCount),
				"success", atomic.LoadInt64(successCount),
				"errors", atomic.LoadInt64(errorCount))
		}
	}
}

// queryUserDataWithRetry 带重试的查询用户数据
func queryUserDataWithRetry(ctx context.Context, client *hyper.Client, address string, logger *slog.Logger) (*dao.UserQysy, error) {
	var lastErr error
	for attempt := 0; attempt < retryCount; attempt++ {
		if attempt > 0 {
			// 重试前等待
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(retryDelay * time.Duration(attempt)):
			}
			logger.Debug("重试查询", "address", address, "attempt", attempt+1)
		}

		data, err := queryUserData(client, address)
		if err == nil {
			return data, nil
		}

		// 如果是 ErrNull，不需要重试
		if err == hyper.ErrNull {
			return data, nil
		}

		lastErr = err
		logger.Debug("查询失败", "address", address, "attempt", attempt+1, "error", err)
	}

	return nil, lastErr
}

// saveResults 批量保存结果
func saveResults(resultCh <-chan *dao.UserQysy, total int) {
	var results []*dao.UserQysy
	var savedCount int
	var skippedCount int

	for result := range resultCh {
		// 用户权益为0的数据不添加到保存列表
		if result.UserEquity == 0 {
			skippedCount++
			if skippedCount%100 == 0 {
				slog.Debug("跳过权益为0的数据", "address", result.Address, "skipped", skippedCount)
			}
			continue
		}
		
		results = append(results, result)

		// 批量保存（BatchSaveUserQysy 内部会过滤掉 user_equity = 0 的数据，只保存大于0的）
		if len(results) >= batchSize {
			beforeCount := len(results)
			if err := dao.BatchSaveUserQysy(results); err != nil {
				slog.Error("批量保存失败", "error", err, "count", beforeCount)
			} else {
				// 统计实际保存的数量（BatchSaveUserQysy 会过滤掉 user_equity = 0 的数据）
				actualSaved := 0
				for _, r := range results {
					if r.UserEquity > 0 {
						actualSaved++
					}
				}
				savedCount += actualSaved
				skippedInBatch := beforeCount - actualSaved
				if skippedInBatch > 0 {
					skippedCount += skippedInBatch
				}
				slog.Info("批量保存成功", "count", actualSaved, "skipped_in_batch", skippedInBatch, "total_saved", savedCount, "total_skipped", skippedCount, "progress", float64(savedCount)/float64(total)*100)
			}
			results = results[:0]
		}
	}

	// 保存剩余数据（只保存 user_equity > 0 的数据）
	if len(results) > 0 {
		beforeCount := len(results)
		if err := dao.BatchSaveUserQysy(results); err != nil {
			slog.Error("保存剩余数据失败", "error", err, "count", beforeCount)
		} else {
			// 统计实际保存的数量
			actualSaved := 0
			for _, r := range results {
				if r.UserEquity > 0 {
					actualSaved++
				}
			}
			savedCount += actualSaved
			skippedInBatch := beforeCount - actualSaved
			skippedCount += skippedInBatch
			slog.Info("保存剩余数据成功", "count", actualSaved, "skipped_in_batch", skippedInBatch, "total_saved", savedCount, "total_skipped", skippedCount)
		}
	}
}

// collectErrors 收集错误信息
func collectErrors(errorCh <-chan error) {
	errorMap := make(map[string]int)
	for err := range errorCh {
		if err != nil {
			errorMap[err.Error()]++
		}
	}

	if len(errorMap) > 0 {
		slog.Warn("错误统计", "unique_errors", len(errorMap))
		for errMsg, count := range errorMap {
			slog.Warn("错误详情", "error", errMsg, "count", count)
		}
	}
}

// queryUserData 查询单个用户的数据
func queryUserData(client *hyper.Client, address string) (*dao.UserQysy, error) {
	data := &dao.UserQysy{
		Address: address,
		UpdatedAt: time.Now(),
	}
	
	// 1. 查询用户权益（账户价值）- 使用 CrossMarginSummary.AccountValue
	summary, err := client.GetUserAccountSummary(address)
	if err != nil {
		if err == hyper.ErrNull {
			// 用户不存在或没有数据，返回默认值
			return data, nil
		}
		return nil, err
	}
	
	// 获取账户价值（用户权益）- 优先使用 CrossMarginSummary.AccountValue，如果为空则使用 MarginSummary.AccountValue
	// NUMERIC(32, 12) 最大值为 9999999999999999999999999999.999999999999
	const maxEquity = 9999999999999999999999999999.999999999999
	if summary.CrossMarginSummary.AccountValue != nil {
		accountValue, err := summary.CrossMarginSummary.AccountValue.Float64()
		if err == nil && accountValue > 0 {
			if accountValue > maxEquity {
				data.UserEquity = maxEquity
			} else {
				data.UserEquity = accountValue
			}
		}
	}
	
	// 如果 CrossMarginSummary.AccountValue 为空或0，回退到 MarginSummary.AccountValue
	if data.UserEquity == 0 && summary.MarginSummary.AccountValue != nil {
		accountValue, err := summary.MarginSummary.AccountValue.Float64()
		if err == nil && accountValue > 0 {
			if accountValue > maxEquity {
				data.UserEquity = maxEquity
			} else {
				data.UserEquity = accountValue
			}
		}
	}
	
	// 2. 查询用户交易记录
	fills, err := client.GetUserFills(address)
	if err != nil {
		if err == hyper.ErrNull {
			// 没有交易记录，返回已有数据
			return data, nil
		}
		return nil, err
	}
	
	// 计算7天前的时间戳
	sevenDaysAgo := time.Now().AddDate(0, 0, -7).UnixMilli()
	
	// NUMERIC(18, 6) 最大值为 999999999999.999999 (12位整数 + 6位小数)
	// 但为了安全，使用稍小的值避免溢出
	const maxRatio = 999999999999.0
	
	var (
		totalProfit    float64 // 所有盈利订单累计
		totalLoss      float64 // 所有亏损订单累计（总亏损）
		totalFee       float64 // 手续费累计
		tradeCount7d   int     // 7日内交易次数
	)
	
	// 遍历所有交易记录
	for _, fill := range fills {
		// 获取已实现盈亏
		closedPnl, err := fill.ClosedPnl.Float64()
		if err != nil {
			continue
		}
		
		// 获取手续费
		fee, err := fill.Fee.Float64()
		if err == nil {
			feeAbs := math.Abs(fee)
			// 限制手续费累加不超过最大值
			if totalFee + feeAbs > maxEquity {
				totalFee = maxEquity
			} else {
				totalFee += feeAbs
			}
		}
		
		// 统计盈亏，限制累加值不超过最大值
		if closedPnl > 0 {
			if totalProfit + closedPnl > maxEquity {
				totalProfit = maxEquity
			} else {
				totalProfit += closedPnl
			}
		} else if closedPnl < 0 {
			lossAbs := math.Abs(closedPnl)
			if totalLoss + lossAbs > maxEquity {
				totalLoss = maxEquity
			} else {
				totalLoss += lossAbs
			}
		}
		
		// 统计7日内的交易次数
		if fill.Time >= sevenDaysAgo {
			tradeCount7d++
		}
	}
	
	// 2. 用户损益(累计亏损)
	if totalLoss > maxEquity {
		data.TotalLoss = maxEquity
	} else {
		data.TotalLoss = totalLoss
	}
	
	// 3. 整体盈亏比(所有盈利订单累计/所有亏损订单累计)
	// NUMERIC(18, 6) 最大值为 999999999999.999999，但为了安全使用 999999999999.0
	if totalLoss > 0 {
		ratio := totalProfit / totalLoss
		if ratio > maxRatio {
			data.ProfitLossRatio = maxRatio
		} else if ratio < 0 {
			data.ProfitLossRatio = 0
		} else {
			data.ProfitLossRatio = ratio
		}
	} else if totalProfit > 0 {
		// 如果只有盈利没有亏损，设为最大允许值
		data.ProfitLossRatio = maxRatio
	} else {
		data.ProfitLossRatio = 0
	}
	
	// 4. 手续费占比(手续费累计/总亏损)
	if totalLoss > 0 {
		ratio := totalFee / totalLoss
		if ratio > maxRatio {
			data.FeeRatio = maxRatio
		} else {
			data.FeeRatio = ratio
		}
	} else {
		data.FeeRatio = 0
	}
	
	// 5. 7日内平均交易次数
	// NUMERIC(18, 6) 最大值为 999999999999.999999
	const maxCount = 999999999999.999999
	avgCount := float64(tradeCount7d) / 7.0
	if avgCount > maxCount {
		data.AvgTradeCount7d = maxCount
	} else {
		data.AvgTradeCount7d = avgCount
	}
	
	return data, nil
}
