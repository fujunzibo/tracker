package commands

import (
	"context"
	"fmt"
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"hyper-tracker/pkg/hyper"
	"hyper-tracker/pkg/log"
	"hyper-tracker/pkg/rpc"
	"hyper-tracker/pkg/utils"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

var (
	marketFillsCoin           string
	marketFillsStartTime      int64
	marketFillsEndTime       int64
	marketFillsTimeWindow    time.Duration
	marketFillsMaxPerRequest int
	localIP                  string // 指定使用的本地IP地址
	blockRange               int64 // 从链上获取地址的区块范围（最近N个区块，0表示查询全部）
	fromBlockNum             int64 // 手动指定起始区块号（0表示自动获取）
	toBlockNum               int64 // 手动指定结束区块号（0表示自动获取或使用最新）
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
	
	// 本地IP地址配置
	queryAccountCmd.Flags().StringVar(&localIP, "local-ip", "", "指定使用的本地IP地址（如果指定，将只使用此IP，忽略配置文件中的localAddrs）")
	
	// 区块范围配置
	queryAccountCmd.Flags().Int64Var(&blockRange, "block-range", 0, "从链上获取地址的区块范围（最近N个区块，0表示查询全部历史。建议：10000-50000以减少请求数，避免超过每日限制）")
	queryAccountCmd.Flags().Int64Var(&fromBlockNum, "from-block", 0, "手动指定起始区块号（0表示自动计算。如果遇到每日限制，可以手动指定以避免调用 GetBlockNumber）")
	queryAccountCmd.Flags().Int64Var(&toBlockNum, "to-block", 0, "手动指定结束区块号（0表示使用最新区块。如果遇到每日限制，可以手动指定以避免调用 GetBlockNumber）")
}

const (
	accountMaxConcurrency = 30   // 最大并发数（进一步降低以减少429错误）
	accountBatchSize     = 200   // 批量保存大小
	accountRetryCount    = 5     // 重试次数（包括首次尝试，总共5次，即再重试4次）
	accountRetryDelay    = 2 * time.Second // 重试延迟
	accountRateLimit     = 150   // 速率限制：每10秒150个请求（进一步降低以减少429错误，约15请求/秒）
	accountWorkerDelay   = 200 * time.Millisecond // Worker启动延迟，避免同时启动（增加到200ms）
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

	// 创建 RPC 客户端（优先使用 HyperEVM RPC，如果没有配置则使用 QuickNode RPC）
	var rpcClient *rpc.Client
	var hyperEvmClient *rpc.Client
	
	// 使用相同的 localAddrs 配置
	var localAddrs []string
	if localIP != "" {
		localAddrs = []string{localIP}
		slog.Info("使用指定的本地IP地址", "ip", localIP)
	} else {
		localAddrs = cfg.Network.LocalAddrs
		if len(localAddrs) > 0 {
			slog.Info("使用配置文件中的本地IP地址列表", "ips", localAddrs)
		}
	}
	
	// 优先创建 HyperEVM RPC 客户端（Hyperliquid 官方 RPC）
	hyperEvmRpcUrl := cfg.Follow.HyperEvmRpcUrl
	if hyperEvmRpcUrl == "" {
		hyperEvmRpcUrl = "https://rpc.hyperliquid.xyz/evm" // 默认 HyperEVM RPC URL
		slog.Info("使用默认 HyperEVM RPC URL", "url", hyperEvmRpcUrl)
	} else {
		slog.Info("使用配置的 HyperEVM RPC URL", "url", hyperEvmRpcUrl)
	}
	hyperEvmClient = rpc.NewClient(hyperEvmRpcUrl, localAddrs, cfg.Network.Proxies)
	
	// 创建 QuickNode RPC 客户端（作为备用）
	if cfg.Follow.RpcUrl != "" {
		rpcClient = rpc.NewClient(cfg.Follow.RpcUrl, localAddrs, cfg.Network.Proxies)
		slog.Info("已创建 QuickNode RPC 客户端（备用）", "url", cfg.Follow.RpcUrl)
	} else {
		slog.Warn("未配置 QuickNode RPC URL，将仅使用 HyperEVM RPC")
	}
	
	// 设置 Hyperliquid 合约地址（从配置文件读取，如果未配置则使用默认值）
	clearinghouseAddr := cfg.Follow.ClearinghouseAddress
	if clearinghouseAddr == "" {
		clearinghouseAddr = "0x3333333333333333333333333333333333333333"
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
	
	slog.Info("开始获取所有地址...")
	
	// 创建 Hyperliquid API 客户端
	hyperClient := hyper.WithLocalAddrsAndProxies(localAddrs, cfg.Network.Proxies)
	
	var addresses []string
	
	// 优先尝试通过 Hyperliquid Info API 获取地址（不消耗 RPC 请求）
	slog.Info("尝试通过 Hyperliquid Info API 获取地址...")
	apiAddresses, apiErr := hyperClient.GetAllAddressesFromMarketFills(nil, 0, 0, time.Hour, slog.Default())
	if apiErr == nil && len(apiAddresses) > 0 {
		addresses = apiAddresses
		slog.Info("成功通过 Hyperliquid Info API 获取地址", "count", len(addresses), "source", "API")
	} else {
		if apiErr != nil {
			slog.Warn("通过 Hyperliquid Info API 获取地址失败，尝试使用链上 RPC",
				"error", apiErr,
				"tip", "MarketFill 可能不包含用户地址字段，将使用链上 RPC 方法")
		} else {
			slog.Warn("通过 Hyperliquid Info API 未获取到地址，尝试使用链上 RPC",
				"tip", "MarketFill 可能不包含用户地址字段，将使用链上 RPC 方法")
		}
		
		// 如果 API 方法失败，回退到链上 RPC 方法
		slog.Info("开始从链上获取所有地址...")
		
		// 从链上获取所有地址
		var fromBlock *big.Int = nil
		var toBlock *big.Int = nil
		
		// 如果手动指定了区块号，直接使用
		if fromBlockNum > 0 {
			fromBlock = big.NewInt(fromBlockNum)
			slog.Info("使用手动指定的起始区块号", "from_block", fromBlock.String())
		}
		
		if toBlockNum > 0 {
			toBlock = big.NewInt(toBlockNum)
			slog.Info("使用手动指定的结束区块号", "to_block", toBlock.String())
		} else {
			// 需要获取最新区块号
			latestBlock, err := rpcClient.GetBlockNumber()
			if err != nil {
				// 检查是否是每日限制错误
				if strings.Contains(err.Error(), "daily request limit") || strings.Contains(err.Error(), "-32003") {
					slog.Error("QuickNode 已达到每日请求限制",
						"error", err,
						"tip", "请等待24小时后重试，或升级 QuickNode 账户，或使用 --block-range 参数限制查询范围以减少请求数")
					exitWithError(fmt.Errorf("QuickNode 每日请求限制已到达: %w\n提示：可以使用 --block-range 参数限制查询范围（例如：--block-range 10000 只查询最近10000个区块）", err))
				}
				slog.Error("获取最新区块号失败", "error", err)
				exitWithError(err)
			}
			
			// 如果指定了区块范围，只查询最近的区块
			if blockRange > 0 {
				slog.Info("限制查询区块范围", "recent_blocks", blockRange)
				if latestBlock.Cmp(big.NewInt(blockRange)) > 0 {
					fromBlock = new(big.Int).Sub(latestBlock, big.NewInt(blockRange))
					slog.Info("设置查询区块范围",
						"from_block", fromBlock.String(),
						"to_block", latestBlock.String(),
						"block_range", blockRange)
				}
				toBlock = latestBlock
			} else {
				// 查询全部历史
				// 估算请求数量：每5个区块需要1次请求（QuickNode限制）
				totalBlocks := latestBlock.Int64()
				estimatedRequests := totalBlocks / 5
				if totalBlocks%5 != 0 {
					estimatedRequests++
				}
				
				slog.Warn("将查询全部历史数据",
					"latest_block", latestBlock.String(),
					"total_blocks", totalBlocks,
					"estimated_requests", estimatedRequests,
					"warning", fmt.Sprintf("这将需要约 %d 次 RPC 请求，可能超过 QuickNode 每日限制", estimatedRequests),
					"tip", "如果遇到每日限制错误，建议使用 --block-range 参数分批查询（例如：--block-range 50000）")
				
				// 如果估算请求数超过 10000，给出强烈警告
				if estimatedRequests > 10000 {
					slog.Warn("⚠️  警告：查询全部历史将消耗大量请求",
						"estimated_requests", estimatedRequests,
						"recommendation", "强烈建议使用 --block-range 参数限制查询范围，例如：--block-range 50000")
				}
				
				toBlock = latestBlock
			}
		}
		
		// 优先使用 HyperEVM RPC 查询地址
		var chainErr error
		if hyperEvmClient != nil {
			slog.Info("使用 HyperEVM RPC 查询地址", "rpc_url", hyperEvmRpcUrl)
			addresses, chainErr = hyperEvmClient.QueryAllAddressesFromHyperEVM(fromBlock, toBlock, slog.Default())
			if chainErr != nil {
				slog.Warn("通过 HyperEVM RPC 获取地址失败，尝试使用 QuickNode RPC", "error", chainErr)
				// 如果 HyperEVM RPC 失败，回退到 QuickNode RPC
				if rpcClient != nil {
					addresses, chainErr = rpcClient.QueryAllAddressesFromChain(fromBlock, toBlock, slog.Default(), nil)
					if chainErr == nil {
						slog.Info("成功通过 QuickNode RPC 获取地址", "count", len(addresses), "source", "QuickNode RPC")
					}
				}
			} else {
				slog.Info("成功通过 HyperEVM RPC 获取地址", "count", len(addresses), "source", "HyperEVM RPC")
			}
		} else if rpcClient != nil {
			slog.Info("使用 QuickNode RPC 查询地址")
			addresses, chainErr = rpcClient.QueryAllAddressesFromChain(fromBlock, toBlock, slog.Default(), nil)
			if chainErr == nil {
				slog.Info("成功通过 QuickNode RPC 获取地址", "count", len(addresses), "source", "QuickNode RPC")
			}
		} else {
			exitWithError(fmt.Errorf("未配置任何 RPC 客户端，无法从链上获取地址"))
		}
		
		err = chainErr
		if err != nil {
			// 检查是否是每日限制错误
			if strings.Contains(err.Error(), "daily request limit") || strings.Contains(err.Error(), "-32003") {
				slog.Error("RPC 已达到每日请求限制",
					"error", err,
					"tip", "请等待24小时后重试，或升级 RPC 账户，或使用 --block-range 参数限制查询范围以减少请求数")
				exitWithError(fmt.Errorf("RPC 每日请求限制已到达: %w\n提示：可以使用 --block-range 参数限制查询范围（例如：--block-range 10000 只查询最近10000个区块）", err))
			}
			slog.Error("从链上获取地址列表失败", "error", err)
			exitWithError(err)
		}
	}
	
	total := len(addresses)
	slog.Info("从链上获取到地址列表", "count", total)
	
	if total == 0 {
		slog.Info("没有地址需要处理")
		return
	}
	
	// 转换为 AddressWithEquity 格式（权益信息初始化为 0，因为从链上无法直接获取）
	addressesWithEquity := make([]dao.AddressWithEquity, total)
	for i, addr := range addresses {
		addressesWithEquity[i] = dao.AddressWithEquity{
			Address:     addr,
			TotalEquity: 0,
			PrepPnl:     0,
		}
	}
	
	// 创建地址到权益的映射（虽然权益为0，但保留接口兼容性）
	addressEquityMap := make(map[string]dao.AddressWithEquity, total)
	for _, addr := range addressesWithEquity {
		addressEquityMap[addr.Address] = addr
	}

	// 使用之前创建的 Hyperliquid 客户端
	client := hyperClient

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

	// 启动 Worker Goroutines（错开启动时间，避免同时发起大量请求）
	for i := 0; i < accountMaxConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// 错开启动时间，避免所有Worker同时启动导致429错误
			if workerID > 0 {
				time.Sleep(accountWorkerDelay * time.Duration(workerID))
			}
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
		// 等待速率限制（确保不超过API限制）
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
			logger.Info("成功从 Hyperliquid API 获取数据", "address", address, "source", "API")
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
				logger.Warn("API 返回 null，已重试多次，尝试从 QuickNode 获取数据", 
					"address", address,
					"null_retry_count", nullRetryCount,
					"max_null_retries", maxNullRetries)
				quickNodeData, quickNodeErr := queryAccountDataFromQuickNode(rpcClient, address, totalEquity, prepPnl, logger)
				if quickNodeErr == nil {
					logger.Info("成功从 QuickNode 获取数据（API 返回 null）", "address", address)
					return quickNodeData, nil
				}
				logger.Error("从 QuickNode 获取数据失败（API 也返回 null）", "address", address, "error", quickNodeErr)
				return nil, fmt.Errorf("API 返回 null 且 QuickNode 获取失败: %w", quickNodeErr)
			}
		}

		// 其他错误，继续重试
		logger.Debug("查询失败", "address", address, "attempt", attempt+1, "error", err)
	}

	// 如果所有重试都失败（非 null 错误），尝试使用 QuickNode RPC 作为备用
	if lastNullData != nil {
		// 如果最后一次是 null，使用链上获取
		logger.Warn("API 查询失败（null），尝试使用 QuickNode RPC", "address", address)
		quickNodeData, quickNodeErr := queryAccountDataFromQuickNode(rpcClient, address, totalEquity, prepPnl, logger)
		if quickNodeErr == nil {
			logger.Info("成功从 QuickNode 获取数据（API 返回 null）", "address", address)
			return quickNodeData, nil
		}
		logger.Error("从 QuickNode 获取数据失败（API 也返回 null）", "address", address, "error", quickNodeErr)
		return nil, fmt.Errorf("API 返回 null 且 QuickNode 获取失败: %w", quickNodeErr)
	}
	
	// 其他错误，也尝试链上获取
	logger.Warn("API 查询失败，尝试使用 QuickNode RPC", "address", address)
	quickNodeData, quickNodeErr := queryAccountDataFromQuickNode(rpcClient, address, totalEquity, prepPnl, logger)
	if quickNodeErr == nil {
		logger.Info("成功从 QuickNode 获取数据（API 查询失败）", "address", address)
		return quickNodeData, nil
	}
	logger.Error("从 QuickNode 获取数据失败（API 也失败）", "address", address, "error", quickNodeErr)
	return nil, fmt.Errorf("API 查询失败且 QuickNode 获取失败: %w", quickNodeErr)
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
		
		// 如果仍然没有获取到 total_equity，不可以使用数据库数据
		// 所有数据必须来自 API 或 QuickNode RPC
		if !apiSummarySuccess {
			// 不设置 totalEquity，保持为 0
			// 后续如果 API 返回 null，会尝试从 QuickNode 获取
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
			// 不使用数据库数据（prepPnl），所有数据必须来自 API 或 QuickNode
			return data, hyper.ErrNull
		}
		// 其他错误，返回错误让调用者重试
		// 不使用数据库数据，所有数据必须来自 API 或 QuickNode
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

	// 标记数据来源为 QuickNode
	data.Source = "QuickNode"

	return data, nil
}

// calculateAccountDataFromFills 从交易记录计算账户数据
func calculateAccountDataFromFills(fills []rpc.ChainFill) (totalPnl, totalFee float64, tradeCount7d int) {
	const maxValue = 9999999999999999999999999999.999999999999
	sevenDaysAgo := time.Now().AddDate(0, 0, -7).UnixMilli()

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
		totalPnl = maxValue
	} else if totalPnl < -maxValue {
		totalPnl = -maxValue
	}

	if totalFee > maxValue {
		totalFee = maxValue
	}

	return totalPnl, totalFee, tradeCount7d
}

// queryAccountDataFromQuickNode 从 QuickNode RPC 查询账户数据（备用方案）
// 从链上获取交易数据并计算 total_pnl 和 total_fee
// 注意：不使用数据库作为备用数据源，所有数据必须来自 API 或 QuickNode RPC
func queryAccountDataFromQuickNode(rpcClient *rpc.Client, address string, totalEquity float64, prepPnl float64, logger *slog.Logger) (*dao.UserAccount, error) {
	logger.Info("尝试从 QuickNode RPC 获取数据", "address", address)
	
	data := &dao.UserAccount{
		Address:     address,
		TotalEquity: 0, // 不从数据库获取，初始化为 0
		UpdatedAt:   time.Now(),
		Source:      "QuickNode", // 标记数据来源
	}

	// 如果 RPC 客户端未配置，返回空数据（不使用数据库数据）
	if rpcClient == nil {
		logger.Warn("QuickNode RPC 客户端未配置，无法从链上获取数据", "address", address)
		return data, fmt.Errorf("QuickNode RPC 客户端未配置")
	}

	// 1. 获取最新区块号
	logger.Debug("从 QuickNode 获取最新区块号", "address", address)
	latestBlock, err := rpcClient.GetBlockNumber()
	if err != nil {
		logger.Warn("从 QuickNode 获取最新区块号失败", "error", err, "address", address)
		return data, fmt.Errorf("获取最新区块号失败: %w", err)
	}
	logger.Info("成功从 QuickNode 获取最新区块号", "address", address, "block", latestBlock.String())

	// 2. 计算查询范围
	// QuickNode 免费计划限制：每次查询最多 5 个区块
	// QueryUserFillsFromChain 函数内部会自动分批查询
	// 为了性能，可以只查询最近的区块，比如最近 1000 个区块（会分批查询）
	fromBlock := big.NewInt(0)
	if latestBlock.Cmp(big.NewInt(1000)) > 0 {
		fromBlock.Sub(latestBlock, big.NewInt(1000))
	}
	logger.Debug("计算查询区块范围", "address", address, "from_block", fromBlock.String(), "to_block", latestBlock.String(), "note", "将自动分批查询，每批最多5个区块")

	// 3. 从链上查询交易记录
	logger.Info("从 QuickNode 查询链上交易记录", "address", address, "from_block", fromBlock.String(), "to_block", latestBlock.String())
	fills, err := rpcClient.QueryUserFillsFromChain(address, fromBlock, latestBlock, logger)
	if err != nil {
		logger.Warn("从 QuickNode 查询链上交易记录失败", "error", err, "address", address)
		return data, fmt.Errorf("查询链上交易记录失败: %w", err)
	}
	
	logger.Info("成功从 QuickNode 获取链上交易记录", "address", address, "fills_count", len(fills))

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

	// 标记数据来源为 QuickNode
	data.Source = "QuickNode"

	logger.Info("成功从 QuickNode 计算账户数据",
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
