package commands

import (
	"context"
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"hyper-tracker/pkg/hyper"
	"hyper-tracker/pkg/log"
	"hyper-tracker/pkg/rpc"
	"hyper-tracker/pkg/utils"
	"log/slog"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

var (
	marketFillsCoin      string
	marketFillsStartTime int64
	marketFillsEndTime   int64
	marketFillsTimeWindow time.Duration
	marketFillsMaxPerRequest int
)

var queryAccountCmd = &cobra.Command{
	Use:   "query-account",
	Short: "查询用户账户数据或市场交易记录",
	Long: `从数据库读取钱包地址，通过 Hyperliquid API 查询以下数据并保存到 tb_user_account 表：
  1. 用户总权益 (total_equity) - 从 CrossMarginSummary.AccountValue 获取
  2. 可用余额 (available_balance) - 从 Withdrawable 获取
  3. 已用保证金 (used_margin) - 从 CrossMarginSummary.TotalMarginUsed 获取
  4. 累计盈亏 (cumulative_pnl) - 从交易记录计算所有 ClosedPnl 的累计
  5. 累计手续费 (cumulative_fees) - 从交易记录计算所有 Fee 的累计
  6. 7日内累计交易次数 (trade_count_7d) - 统计7天内的交易次数

如果 API 失败，将使用 QuickNode RPC 作为备用数据源。

使用 --get-market-fills 标志可以查询市场交易记录（所有用户的交易）。

使用示例：
  tracker query-account --config etc/tracker.yaml
  tracker query-account --get-market-fills --coin BTC --config etc/tracker.yaml`,
	Run: func(cmd *cobra.Command, args []string) {
		if marketFillsCoin != "" {
			runGetAllMarketFills()
		} else {
			runQueryAccount()
		}
	},
}

func init() {
	rootCmd.AddCommand(queryAccountCmd)
	
	// 市场交易记录查询标志
	queryAccountCmd.Flags().StringVar(&marketFillsCoin, "get-market-fills", "", "查询市场交易记录，指定币种（如 BTC, ETH）")
	queryAccountCmd.Flags().Int64Var(&marketFillsStartTime, "market-start-time", 0, "市场数据查询起始时间戳（毫秒），0 表示从最早开始")
	queryAccountCmd.Flags().Int64Var(&marketFillsEndTime, "market-end-time", 0, "市场数据查询结束时间戳（毫秒），0 表示到当前时间")
	queryAccountCmd.Flags().DurationVar(&marketFillsTimeWindow, "market-time-window", time.Hour, "市场数据查询时间窗口（每次查询的时间段）")
	queryAccountCmd.Flags().IntVar(&marketFillsMaxPerRequest, "market-max-per-request", 1000, "市场数据每次请求的最大记录数")
}

const (
	accountMaxConcurrency = 400  // 最大并发数
	accountBatchSize     = 400   // 批量保存大小
	accountRetryCount    = 5     // 重试次数（包括首次尝试，总共5次，即再重试4次）
	accountRetryDelay    = 2 * time.Second // 重试延迟
	accountRateLimit     = 1000  // 速率限制：每10秒1000个请求
)

func runQueryAccount() {
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

	// 获取所有地址及其权益（用于备用数据源）
	addressesWithEquity, err := dao.GetAllAddressesWithEquity()
	if err != nil {
		slog.Error("获取地址列表失败", "error", err)
		exitWithError(err)
	}
	total := len(addressesWithEquity)
	slog.Info("获取到地址列表", "count", total)
	
	// 创建地址到权益的映射，用于备用数据源
	addressEquityMap := make(map[string]dao.AddressWithEquity, total)
	for _, addr := range addressesWithEquity {
		addressEquityMap[addr.Address] = addr
	}

	if total == 0 {
		slog.Info("没有地址需要处理")
		return
	}

	// 创建 Hyperliquid 客户端
	client := hyper.WithLocalAddrsAndProxies(cfg.Network.LocalAddrs, cfg.Network.Proxies)

	// 创建 QuickNode RPC 客户端（用于链上数据获取）
	var rpcClient *rpc.Client
	if cfg.Follow.RpcUrl != "" {
		rpcClient = rpc.NewClient(cfg.Follow.RpcUrl, cfg.Network.LocalAddrs, cfg.Network.Proxies)
		
		// 设置 Hyperliquid 合约地址（从配置文件读取，如果未配置则使用默认值）
		// 默认已设置 Clearinghouse 地址：0x8549fD7FFC092F8366e416E129a622EC060104ea
		clearinghouseAddr := cfg.Follow.ClearinghouseAddress
		if clearinghouseAddr == "" {
			// 使用默认地址
			clearinghouseAddr = "0x8549fD7FFC092F8366e416E129a622EC060104ea"
			slog.Info("使用默认 Hyperliquid Clearinghouse 合约地址", "address", clearinghouseAddr)
		} else {
			slog.Info("使用配置的 Hyperliquid Clearinghouse 合约地址", "address", clearinghouseAddr)
		}
		
		rpc.SetHyperliquidContractAddress(
			clearinghouseAddr,
			cfg.Follow.ExchangeAddress,
		)
		
		if cfg.Follow.ExchangeAddress != "" {
			slog.Info("已设置 Hyperliquid Exchange 合约地址", "address", cfg.Follow.ExchangeAddress)
		}
		
		slog.Info("QuickNode RPC 客户端已创建", "rpcUrl", cfg.Follow.RpcUrl)
	} else {
		slog.Warn("未配置 QuickNode RPC URL，链上数据获取功能将不可用")
	}

	// 创建速率限制器
	rateLimiter := utils.NewRateLimiter(accountRateLimit)

	// 创建 Worker Pool
	addressCh := make(chan dao.AddressWithEquity, accountMaxConcurrency)
	resultCh := make(chan *dao.UserAccount, accountMaxConcurrency*2)
	errorCh := make(chan error, accountMaxConcurrency)

	// 统计变量
	var (
		processedCount int64
		successCount   int64
		errorCount     int64
		wg             sync.WaitGroup
	)

	// 启动 Worker Goroutines
	for i := 0; i < accountMaxConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			accountWorker(ctx, client, rpcClient, rateLimiter, addressCh, resultCh, errorCh, workerID, &processedCount, &successCount, &errorCount)
		}(i)
	}

	// 启动结果收集和批量保存 Goroutine
	var saveWg sync.WaitGroup
	saveWg.Add(1)
	go func() {
		defer saveWg.Done()
		saveAccountResults(resultCh, total)
	}()

	// 启动错误收集 Goroutine
	var errorWg sync.WaitGroup
	errorWg.Add(1)
	go func() {
		defer errorWg.Done()
		collectAccountErrors(errorCh)
	}()

	// 发送地址到 channel
	startTime := time.Now()
	go func() {
		defer close(addressCh)
		for _, addr := range addressesWithEquity {
			addressCh <- addr
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

// accountWorker 工作协程，处理地址查询
func accountWorker(
	ctx context.Context,
	client *hyper.Client,
	rpcClient *rpc.Client,
	rateLimiter *utils.RateLimiter,
	addressCh <-chan dao.AddressWithEquity,
	resultCh chan<- *dao.UserAccount,
	errorCh chan<- error,
	workerID int,
	processedCount, successCount, errorCount *int64,
) {
	logger := slog.With("worker", workerID)
	defer utils.Recovery(logger)

	for addrInfo := range addressCh {
		// 等待速率限制
		if err := rateLimiter.Wait(ctx); err != nil {
			if err == context.Canceled {
				return
			}
			logger.Error("速率限制等待失败", "error", err)
		}

		// 查询用户账户数据（带重试）
		data, err := queryAccountDataWithRetry(ctx, client, rpcClient, addrInfo.Address, addrInfo.TotalEquity, addrInfo.PrepPnl, logger)
		atomic.AddInt64(processedCount, 1)

		if err != nil {
			atomic.AddInt64(errorCount, 1)
			logger.Error("查询用户账户数据失败", "address", addrInfo.Address, "error", err)
			errorCh <- err
			continue
		}

		atomic.AddInt64(successCount, 1)
		
		// 调试：输出前几个地址的数据
		if atomic.LoadInt64(successCount) <= 5 {
			logger.Info("查询结果",
				"address", addrInfo.Address,
				"total_equity", data.TotalEquity,
				"available_balance", data.AvailableBalance,
				"used_margin", data.UsedMargin,
				"total_pnl", data.TotalPnl,
				"total_fee", data.TotalFee,
				"trade_count_7d", data.TradeCount7d)
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

// queryAccountDataWithRetry 带重试的查询用户账户数据
// 对于 API 返回 null 的情况，会重试两次（总共尝试3次）
func queryAccountDataWithRetry(ctx context.Context, client *hyper.Client, rpcClient *rpc.Client, address string, totalEquity float64, prepPnl float64, logger *slog.Logger) (*dao.UserAccount, error) {
	var lastNullData *dao.UserAccount
	nullRetryCount := 0
	const maxNullRetries = 2 // API 返回 null 时，再重试2次（总共3次尝试）
	
	for attempt := 0; attempt < accountRetryCount; attempt++ {
		if attempt > 0 {
			// 重试前等待
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(accountRetryDelay * time.Duration(attempt)):
			}
			logger.Debug("重试查询", "address", address, "attempt", attempt+1)
		}

		data, err := queryAccountData(client, address, totalEquity, prepPnl)
		if err == nil {
			// API 成功，返回数据
			return data, nil
		}

		// 如果是 ErrNull，记录并重试
		if err == hyper.ErrNull {
			nullRetryCount++
			lastNullData = data
			
			if nullRetryCount <= maxNullRetries {
				logger.Debug("API 返回 null，重试中", 
					"address", address, 
					"null_retry", nullRetryCount,
					"max_null_retries", maxNullRetries)
				// 继续重试
				continue
			} else {
				// 已经重试了 maxNullRetries 次，仍然返回 null
				logger.Debug("API 返回 null，已重试多次，尝试从链上获取数据", 
					"address", address,
					"null_retry_count", nullRetryCount)
				return queryAccountDataFromQuickNode(rpcClient, address, totalEquity, prepPnl, logger)
			}
		}

		// 其他错误，继续重试
		logger.Debug("查询失败", "address", address, "attempt", attempt+1, "error", err)
	}

	// 如果所有重试都失败（非 null 错误），尝试使用 QuickNode RPC 作为备用
	if lastNullData != nil {
		// 如果最后一次是 null，使用链上获取
		logger.Debug("API 查询失败（null），尝试使用 QuickNode RPC", "address", address)
		return queryAccountDataFromQuickNode(rpcClient, address, totalEquity, prepPnl, logger)
	}
	
	// 其他错误，也尝试链上获取
	logger.Debug("API 查询失败，尝试使用 QuickNode RPC", "address", address)
	return queryAccountDataFromQuickNode(rpcClient, address, totalEquity, prepPnl, logger)
}

// queryAccountData 查询单个用户的账户数据
// 优先使用 Hyperliquid API 查询
func queryAccountData(client *hyper.Client, address string, totalEquity float64, prepPnl float64) (*dao.UserAccount, error) {
	data := &dao.UserAccount{
		Address:   address,
		UpdatedAt: time.Now(),
	}

	const maxValue = 9999999999999999999999999999.999999999999

	// 1. 获取账户摘要
	summary, err := client.GetUserAccountSummary(address)
	apiSummarySuccess := false
	if err != nil {
		if err == hyper.ErrNull {
			// API 返回 null，返回错误让调用者重试
			return data, hyper.ErrNull
		}
		// 其他错误，也返回错误让调用者重试
		return nil, err
	} else {
		// API 成功，提取总权益 (total_equity)
		// 注意：不要限制 accountValue > 0，因为即使是 0 也应该记录
		if summary.CrossMarginSummary.AccountValue != nil {
			accountValue, err := summary.CrossMarginSummary.AccountValue.Float64()
			if err == nil {
				// 移除 accountValue > 0 的限制，允许 0 值
				if accountValue > maxValue {
					data.TotalEquity = maxValue
				} else if accountValue < -maxValue {
					data.TotalEquity = -maxValue
				} else {
					data.TotalEquity = accountValue
				}
				apiSummarySuccess = true
			} else {
				// 提取失败，记录日志（仅前几个）
				// 这里不记录太多日志，避免影响性能
			}
		}
		
		if !apiSummarySuccess && summary.MarginSummary.AccountValue != nil {
			// 回退到 MarginSummary
			accountValue, err := summary.MarginSummary.AccountValue.Float64()
			if err == nil {
				// 移除 accountValue > 0 的限制，允许 0 值
				if accountValue > maxValue {
					data.TotalEquity = maxValue
				} else if accountValue < -maxValue {
					data.TotalEquity = -maxValue
				} else {
					data.TotalEquity = accountValue
				}
				apiSummarySuccess = true
			}
		}
		
		// 如果 API 没有获取到 total_equity，尝试使用 Withdrawable 或其他字段
		if !apiSummarySuccess {
			// 尝试使用 Withdrawable + UsedMargin 估算
			if summary.Withdrawable != nil {
				withdrawable, err := summary.Withdrawable.Float64()
				if err == nil {
					// 移除 withdrawable > 0 的限制
					var usedMargin float64
					if summary.CrossMarginSummary.TotalMarginUsed != nil {
						marginUsed, err := summary.CrossMarginSummary.TotalMarginUsed.Float64()
						if err == nil {
							usedMargin = marginUsed
						}
					} else if summary.MarginSummary.TotalMarginUsed != nil {
						marginUsed, err := summary.MarginSummary.TotalMarginUsed.Float64()
						if err == nil {
							usedMargin = marginUsed
						}
					}
					estimatedEquity := withdrawable + usedMargin
					// 移除 estimatedEquity > 0 的限制
					if estimatedEquity > maxValue {
						data.TotalEquity = maxValue
					} else if estimatedEquity < -maxValue {
						data.TotalEquity = -maxValue
					} else {
						data.TotalEquity = estimatedEquity
					}
					apiSummarySuccess = true
				}
			}
		}
		
		// 如果仍然没有获取到 total_equity，使用传入的 totalEquity（来自数据库）
		if !apiSummarySuccess && totalEquity != 0 {
			data.TotalEquity = totalEquity
		}
	}

	// 3. 提取可用余额 (available_balance)
	if summary.Withdrawable != nil {
		withdrawable, err := summary.Withdrawable.Float64()
		if err == nil {
			if withdrawable > maxValue {
				data.AvailableBalance = maxValue
			} else {
				data.AvailableBalance = withdrawable
			}
		}
	}

	// 4. 提取已用保证金 (used_margin)
	if summary.CrossMarginSummary.TotalMarginUsed != nil {
		marginUsed, err := summary.CrossMarginSummary.TotalMarginUsed.Float64()
		if err == nil {
			if marginUsed > maxValue {
				data.UsedMargin = maxValue
			} else {
				data.UsedMargin = marginUsed
			}
		}
	} else if summary.MarginSummary.TotalMarginUsed != nil {
		// 回退到 MarginSummary
		marginUsed, err := summary.MarginSummary.TotalMarginUsed.Float64()
		if err == nil {
			if marginUsed > maxValue {
				data.UsedMargin = maxValue
			} else {
				data.UsedMargin = marginUsed
			}
		}
	}

	// 5. 获取交易记录，计算累计盈亏和累计手续费
	// 注意：如果 GetUserFills 返回 null，也需要重试，所以这里不处理 null 错误
	// null 错误会在 queryAccountDataWithRetry 中统一处理
	fills, err := client.GetUserFills(address)
	if err != nil {
		if err == hyper.ErrNull {
			// API 返回 null，返回错误让调用者重试
			// 但在返回之前，如果 TotalEquity > 0，应该使用 prepPnl 作为 TotalPnl 的估算值
			// 这样即使 GetUserFills 失败，只要 TotalEquity > 0，记录也不会被过滤掉
			if data.TotalEquity != 0 && prepPnl != 0 {
				data.TotalPnl = prepPnl
			}
			return data, hyper.ErrNull
		}
		// 其他错误，返回错误让调用者重试
		// 同样，如果 TotalEquity > 0，使用 prepPnl 作为 TotalPnl 的估算值
		if data.TotalEquity != 0 && prepPnl != 0 {
			data.TotalPnl = prepPnl
		}
		return nil, err
	}
	
	// 计算7天前的时间戳
	sevenDaysAgo := time.Now().AddDate(0, 0, -7).UnixMilli()

	var (
		totalPnl      float64 // 累计盈亏
		totalFee      float64 // 累计手续费
		tradeCount7d  int     // 7日内交易次数
	)

	// 遍历所有交易记录
	for _, fill := range fills {
		// 计算累计盈亏（所有 ClosedPnl 的累计）
		closedPnl, err := fill.ClosedPnl.Float64()
		if err == nil {
			totalPnl += closedPnl
		}

		// 计算累计手续费（所有 Fee 的累计）
		fee, err := fill.Fee.Float64()
		if err == nil {
			// 手续费可能是负数，取绝对值
			if fee < 0 {
				fee = -fee
			}
			totalFee += fee
		}

		// 统计7日内的交易次数
		if fill.Time >= sevenDaysAgo {
			tradeCount7d++
		}
	}

	// 限制数值范围，防止溢出
	if totalPnl > maxValue {
		data.TotalPnl = maxValue
	} else if totalPnl < -maxValue {
		data.TotalPnl = -maxValue
	} else {
		data.TotalPnl = totalPnl
	}

	if totalFee > maxValue {
		data.TotalFee = maxValue
	} else {
		data.TotalFee = totalFee
	}

	data.TradeCount7d = tradeCount7d

	return data, nil
}

// queryAccountDataFromQuickNode 从 QuickNode RPC 查询账户数据（备用方案）
// 从链上获取交易数据并计算 total_pnl 和 total_fee
// 注意：不使用数据库作为备用数据源
func queryAccountDataFromQuickNode(rpcClient *rpc.Client, address string, totalEquity float64, prepPnl float64, logger *slog.Logger) (*dao.UserAccount, error) {
	data := &dao.UserAccount{
		Address:     address,
		TotalEquity: totalEquity, // 使用传入的 totalEquity
		UpdatedAt:   time.Now(),
	}

	// 如果 RPC 客户端未配置，返回数据（使用传入的 totalEquity 和 prepPnl）
	if rpcClient == nil {
		logger.Debug("QuickNode RPC 客户端未配置，无法从链上获取数据", "address", address)
		// 如果 TotalEquity > 0，使用 prepPnl 作为 TotalPnl 的估算值
		if data.TotalEquity != 0 && prepPnl != 0 {
			data.TotalPnl = prepPnl
		}
		return data, nil
	}

	// 1. 获取最新区块号
	latestBlock, err := rpcClient.GetBlockNumber()
	if err != nil {
		logger.Debug("获取最新区块号失败", "error", err, "address", address)
		// 如果 TotalEquity > 0，使用 prepPnl 作为 TotalPnl 的估算值
		if data.TotalEquity != 0 && prepPnl != 0 {
			data.TotalPnl = prepPnl
		}
		return data, nil
	}

	// 2. 计算查询范围（从最早到最新，或者最近一定数量的区块）
	// 为了性能，可以只查询最近的区块，比如最近 10000 个区块
	fromBlock := big.NewInt(0)
	if latestBlock.Cmp(big.NewInt(10000)) > 0 {
		fromBlock.Sub(latestBlock, big.NewInt(10000))
	}

	// 3. 从链上查询交易记录
	fills, err := rpcClient.QueryUserFillsFromChain(address, fromBlock, latestBlock, logger)
	if err != nil {
		logger.Debug("从链上查询交易记录失败", "error", err, "address", address)
		// 链上查询失败，如果 TotalEquity > 0，使用 prepPnl 作为 TotalPnl 的估算值
		if data.TotalEquity != 0 && prepPnl != 0 {
			data.TotalPnl = prepPnl
		}
		return data, nil
	}

	// 4. 计算累计盈亏和累计手续费
	const maxValue = 9999999999999999999999999999.999999999999
	sevenDaysAgo := time.Now().AddDate(0, 0, -7).UnixMilli()

	var (
		totalPnl     float64
		totalFee     float64
		tradeCount7d int
	)

	for _, fill := range fills {
		totalPnl += fill.ClosedPnl
		if fill.Fee < 0 {
			totalFee += -fill.Fee
		} else {
			totalFee += fill.Fee
		}

		// 统计7日内的交易次数
		if fill.Time >= sevenDaysAgo {
			tradeCount7d++
		}
	}

	// 限制数值范围
	if totalPnl > maxValue {
		data.TotalPnl = maxValue
	} else if totalPnl < -maxValue {
		data.TotalPnl = -maxValue
	} else {
		data.TotalPnl = totalPnl
	}

	if totalFee > maxValue {
		data.TotalFee = maxValue
	} else {
		data.TotalFee = totalFee
	}

	data.TradeCount7d = tradeCount7d

	logger.Debug("从链上获取数据完成",
		"address", address,
		"total_pnl", data.TotalPnl,
		"total_fee", data.TotalFee,
		"trade_count_7d", data.TradeCount7d,
		"fills_count", len(fills))

	// 注意：total_equity, available_balance, used_margin 需要调用合约函数获取
	// 这需要合约 ABI 和函数签名，目前暂时为 0
	// TODO: 实现合约调用获取账户状态

	return data, nil
}

// saveAccountResults 批量保存结果
func saveAccountResults(resultCh <-chan *dao.UserAccount, total int) {
	var results []*dao.UserAccount
	var savedCount int
	var skippedCount int

	for result := range resultCh {
		// 只有当用户总权益和累计盈亏都为0时，才不添加到保存列表
		// 其他情况（只要有一个不为0）都要写入数据库
		if result.TotalEquity == 0 && result.TotalPnl == 0 {
			skippedCount++
			if skippedCount <= 10 || skippedCount%100 == 0 {
				slog.Info("跳过总权益和累计盈亏都为0的数据",
					"address", result.Address,
					"total_equity", result.TotalEquity,
					"total_pnl", result.TotalPnl,
					"skipped", skippedCount)
			}
			continue
		}

		results = append(results, result)

		// 批量保存
		if len(results) >= accountBatchSize {
			// 调试：输出前几条要保存的数据
			if savedCount == 0 && len(results) > 0 {
				for i := 0; i < len(results) && i < 3; i++ {
					slog.Info("准备保存数据",
						"address", results[i].Address,
						"total_equity", results[i].TotalEquity,
						"total_pnl", results[i].TotalPnl)
				}
			}
			
			if err := dao.BatchSaveUserAccount(results); err != nil {
				slog.Error("批量保存失败", "error", err, "count", len(results))
			} else {
				savedCount += len(results)
				slog.Info("批量保存成功",
					"count", len(results),
					"total_saved", savedCount,
					"total_skipped", skippedCount,
					"progress", float64(savedCount)/float64(total)*100)
			}
			results = results[:0]
		}
	}

	// 保存剩余数据
	if len(results) > 0 {
		if err := dao.BatchSaveUserAccount(results); err != nil {
			slog.Error("保存剩余数据失败", "error", err, "count", len(results))
		} else {
			savedCount += len(results)
			slog.Info("保存剩余数据成功",
				"count", len(results),
				"total_saved", savedCount,
				"total_skipped", skippedCount)
		}
	} else if savedCount == 0 && skippedCount > 0 {
		slog.Warn("所有数据都被过滤掉了，没有数据写入数据库",
			"total_processed", skippedCount,
			"filter_condition", "total_equity == 0 AND total_pnl == 0")
	}
}

// collectAccountErrors 收集错误信息
func collectAccountErrors(errorCh <-chan error) {
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

// runGetAllMarketFills 查询市场所有交易记录
func runGetAllMarketFills() {
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

	// 创建 Hyperliquid 客户端
	client := hyper.WithLocalAddrsAndProxies(cfg.Network.LocalAddrs, cfg.Network.Proxies)

	slog.Info("开始查询市场交易记录",
		"coin", marketFillsCoin,
		"start_time", marketFillsStartTime,
		"end_time", marketFillsEndTime,
		"time_window", marketFillsTimeWindow,
		"max_per_request", marketFillsMaxPerRequest)

	// 查询市场交易记录
	startTime := time.Now()
	fills, err := client.GetAllMarketFills(
		marketFillsCoin,
		marketFillsStartTime,
		marketFillsEndTime,
		marketFillsTimeWindow,
		marketFillsMaxPerRequest,
	)
	if err != nil {
		slog.Error("查询市场交易记录失败", "error", err)
		exitWithError(err)
	}

	duration := time.Since(startTime)
	slog.Info("查询市场交易记录完成",
		"coin", marketFillsCoin,
		"total_fills", len(fills),
		"duration", duration)

	if len(fills) == 0 {
		slog.Info("没有查询到市场交易记录")
		return
	}

	// 转换数据格式并保存到数据库
	// 注意：市场交易记录没有 address 字段，需要特殊处理
	// 这里我们只输出统计信息，不保存到 hyper_fills 表（因为该表需要 address）
	slog.Info("市场交易记录统计",
		"coin", marketFillsCoin,
		"total_count", len(fills),
		"first_time", fills[0].Time,
		"last_time", fills[len(fills)-1].Time)

	// 如果需要保存市场数据，可以创建一个新的表或修改现有表结构
	// 目前先输出到日志，用户可以根据需要决定如何处理这些数据
	slog.Info("提示：市场交易记录已查询完成，当前版本仅输出统计信息")
	slog.Info("如需保存市场数据，请创建专门的市场交易记录表")
}
