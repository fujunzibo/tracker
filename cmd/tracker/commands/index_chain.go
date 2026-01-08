package commands

import (
	"context"
	"fmt"
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"hyper-tracker/pkg/hyper"
	"hyper-tracker/pkg/log"
	"hyper-tracker/pkg/rpc"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

var (
	indexChainBlockRange   int64 // 从链上获取地址的区块范围（最近N个区块，0表示查询全部）
	indexChainFromBlock    int64 // 手动指定起始区块号（0表示自动计算）
	indexChainToBlock      int64 // 手动指定结束区块号（0表示使用最新区块）
	indexChainLocalIP      string // 指定使用的本地IP地址
	indexChainMaxConcurrency int  // 最大并发数
)

var indexChainCmd = &cobra.Command{
	Use:   "index-chain",
	Short: "通过 QuickNode RPC 建立 Hyperliquid 全链索引库",
	Long: `通过 QuickNode RPC 对 Hyperliquid 建立全链索引库，将获取的钱包地址、权益、损益、交易费等信息写入 tb_user_account 表。

该命令会：
1. 从链上获取所有钱包地址（通过 Hyperliquid Info API 或链上 RPC）
2. 对每个地址，通过 QuickNode RPC 查询链上交易记录
3. 计算累计盈亏、累计手续费、7日内交易次数等信息
4. 将数据批量写入 tb_user_account 表

使用示例：
  tracker index-chain --config etc/tracker.yaml
  tracker index-chain --block-range 50000 --config etc/tracker.yaml
  tracker index-chain --from-block 1000000 --to-block 2000000 --config etc/tracker.yaml`,
	Run: func(cmd *cobra.Command, args []string) {
		runIndexChain()
	},
}

func init() {
	rootCmd.AddCommand(indexChainCmd)

	// 区块范围配置
	indexChainCmd.Flags().Int64Var(&indexChainBlockRange, "block-range", 0, "从链上获取地址的区块范围（最近N个区块，0或不指定表示查询全部历史。建议：10000-50000以减少请求数）")
	indexChainCmd.Flags().Int64Var(&indexChainFromBlock, "from-block", 0, "手动指定起始区块号（0表示自动计算）")
	indexChainCmd.Flags().Int64Var(&indexChainToBlock, "to-block", 0, "手动指定结束区块号（0表示使用最新区块）")
	
	// 本地IP地址配置
	indexChainCmd.Flags().StringVar(&indexChainLocalIP, "local-ip", "", "指定使用的本地IP地址（如果指定，将只使用此IP，忽略配置文件中的localAddrs）")
	
	// 并发数配置
	indexChainCmd.Flags().IntVar(&indexChainMaxConcurrency, "max-concurrency", 10, "最大并发数（默认10，建议根据 RPC 限制调整，HyperEVM RPC 建议使用较小值）")
}

const (
	indexChainBatchSize     = 200   // 批量保存大小
	indexChainRetryCount    = 3     // 重试次数
	indexChainRetryDelay    = 2 * time.Second // 重试延迟
	indexChainWorkerDelay   = 500 * time.Millisecond // Worker启动延迟（增加到500ms，避免同时启动导致 rate limited）
)

func runIndexChain() {
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

	// 使用相同的 localAddrs 配置
	var localAddrs []string
	if indexChainLocalIP != "" {
		localAddrs = []string{indexChainLocalIP}
		slog.Info("使用指定的本地IP地址", "ip", indexChainLocalIP)
	} else {
		localAddrs = cfg.Network.LocalAddrs
		if len(localAddrs) > 0 {
			slog.Info("使用配置文件中的本地IP地址列表", "ips", localAddrs)
		}
	}

	// 创建 QuickNode RPC 客户端
	var rpcClient *rpc.Client
	if cfg.Follow.RpcUrl == "" {
		slog.Error("未配置 QuickNode RPC URL，无法建立索引")
		exitWithError(fmt.Errorf("必须配置 QuickNode RPC URL 才能建立索引"))
	}
	rpcClient = rpc.NewClient(cfg.Follow.RpcUrl, localAddrs, cfg.Network.Proxies)
	slog.Info("已创建 QuickNode RPC 客户端", "url", cfg.Follow.RpcUrl)

	// 创建 HyperEVM RPC 客户端（作为备用）
	// 如果 hyperEvmRpcUrl 设置为 "disable"，则不创建 HyperEVM 客户端，只使用 QuickNode
	var hyperEvmClient *rpc.Client
	hyperEvmRpcUrl := cfg.Follow.HyperEvmRpcUrl
	if strings.ToLower(hyperEvmRpcUrl) == "disable" {
		slog.Info("HyperEVM RPC 已禁用，将只使用 QuickNode RPC")
		hyperEvmClient = nil
	} else {
		if hyperEvmRpcUrl == "" {
			// 如果未配置，使用默认值（保持向后兼容）
			hyperEvmRpcUrl = "https://rpc.hyperliquid.xyz/evm"
			slog.Info("使用默认 HyperEVM RPC URL", "url", hyperEvmRpcUrl)
		} else {
			slog.Info("使用配置的 HyperEVM RPC URL", "url", hyperEvmRpcUrl)
		}
		hyperEvmClient = rpc.NewClient(hyperEvmRpcUrl, localAddrs, cfg.Network.Proxies)
	}

	// 设置 Hyperliquid 合约地址
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
		if indexChainFromBlock > 0 {
			fromBlock = big.NewInt(indexChainFromBlock)
			slog.Info("使用手动指定的起始区块号", "from_block", fromBlock.String())
		}

		if indexChainToBlock > 0 {
			toBlock = big.NewInt(indexChainToBlock)
			slog.Info("使用手动指定的结束区块号", "to_block", toBlock.String())
		} else {
			// 需要获取最新区块号
			var latestBlock *big.Int
			var getBlockErr error

			// 优先使用 HyperEVM RPC 获取最新区块号
			if hyperEvmClient != nil {
				latestBlock, getBlockErr = hyperEvmClient.GetBlockNumber()
				if getBlockErr == nil {
					slog.Info("成功通过 HyperEVM RPC 获取最新区块号", "latest_block", latestBlock.String())
				} else {
					slog.Warn("通过 HyperEVM RPC 获取最新区块号失败，尝试 QuickNode RPC", "error", getBlockErr)
				}
			}

			// 如果 HyperEVM RPC 失败，回退到 QuickNode RPC
			if getBlockErr != nil && rpcClient != nil {
				latestBlock, getBlockErr = rpcClient.GetBlockNumber()
				if getBlockErr == nil {
					slog.Info("成功通过 QuickNode RPC 获取最新区块号", "latest_block", latestBlock.String())
				} else {
					slog.Error("通过 QuickNode RPC 获取最新区块号失败", "error", getBlockErr)
					exitWithError(fmt.Errorf("获取最新区块号失败: %w", getBlockErr))
				}
			} else if hyperEvmClient == nil && rpcClient == nil {
				exitWithError(fmt.Errorf("未配置任何 RPC 客户端，无法获取最新区块号"))
			}

			if getBlockErr != nil {
				// 检查是否是每日限制错误
				if strings.Contains(getBlockErr.Error(), "daily request limit") || strings.Contains(getBlockErr.Error(), "-32003") {
					slog.Error("QuickNode 已达到每日请求限制",
						"error", getBlockErr,
						"tip", "请等待24小时后重试，或升级 QuickNode 账户，或使用 --block-range 参数限制查询范围以减少请求数")
					exitWithError(fmt.Errorf("QuickNode 每日请求限制已到达: %w\n提示：可以使用 --block-range 参数限制查询范围（例如：--block-range 10000 只查询最近10000个区块）", getBlockErr))
				}
				slog.Error("获取最新区块号失败", "error", getBlockErr)
				exitWithError(getBlockErr)
			}

			// 如果指定了区块范围
			if indexChainBlockRange > 0 {
				slog.Info("限制查询区块范围", "recent_blocks", indexChainBlockRange)
				if latestBlock.Cmp(big.NewInt(indexChainBlockRange)) > 0 {
					fromBlock = new(big.Int).Sub(latestBlock, big.NewInt(indexChainBlockRange))
					slog.Info("设置查询区块范围",
						"from_block", fromBlock.String(),
						"to_block", latestBlock.String(),
						"block_range", indexChainBlockRange)
				} else {
					fromBlock = big.NewInt(0)
				}
				toBlock = latestBlock
			} else {
				// 如果没有指定起始区块和区块范围，默认查询全部历史（从区块0开始）
				if fromBlock == nil {
					fromBlock = big.NewInt(0)
					slog.Info("未指定起始区块和区块范围，查询全部历史",
						"from_block", fromBlock.String(),
						"to_block", latestBlock.String(),
						"total_blocks", latestBlock.String(),
						"tip", "如需限制查询范围，请使用 --block-range 参数（例如：--block-range 10000 只查询最近10000个区块）")
				}
				toBlock = latestBlock
			}
		}

		// 如果指定了结束区块但没有指定起始区块，默认从区块0开始查询全部历史
		if indexChainToBlock > 0 && fromBlock == nil {
			fromBlock = big.NewInt(0)
			slog.Info("指定了结束区块但未指定起始区块，从区块0开始查询全部历史",
				"from_block", fromBlock.String(),
				"to_block", toBlock.String(),
				"tip", "如需限制查询范围，请使用 --block-range 参数")
		}

		// 优先使用 HyperEVM RPC 查询地址
		var chainErr error
		var collectedAddresses []string // 保存已收集到的地址
		if hyperEvmClient != nil {
			slog.Info("使用 HyperEVM RPC 查询地址（优先）", "rpc_url", hyperEvmRpcUrl)
			addresses, chainErr = hyperEvmClient.QueryAllAddressesFromHyperEVM(fromBlock, toBlock, slog.Default())
			if chainErr == nil {
				slog.Info("成功通过 HyperEVM RPC 获取地址", "count", len(addresses), "source", "HyperEVM RPC")
			} else {
				// 即使有错误，如果返回了地址，也要保留这些地址
				if len(addresses) > 0 {
					collectedAddresses = addresses
					slog.Info("HyperEVM RPC 部分成功，已收集到地址", 
						"count", len(addresses), 
						"error", chainErr,
						"tip", "将尝试使用 QuickNode RPC 继续查询")
				} else {
					slog.Warn("通过 HyperEVM RPC 获取地址失败，尝试使用 QuickNode RPC", "error", chainErr)
				}
				// 如果 HyperEVM RPC 失败，尝试 QuickNode RPC（作为备用）
				if rpcClient != nil {
					slog.Info("尝试使用 QuickNode RPC 查询地址（备用）", "rpc_url", cfg.Follow.RpcUrl)
					quickNodeAddresses, quickNodeErr := rpcClient.QueryAllAddressesFromChain(fromBlock, toBlock, slog.Default())
					if quickNodeErr == nil {
						// 合并已收集的地址和 QuickNode 返回的地址
						addressMap := make(map[string]bool)
						for _, addr := range collectedAddresses {
							addressMap[addr] = true
						}
						for _, addr := range quickNodeAddresses {
							addressMap[addr] = true
						}
						addresses = make([]string, 0, len(addressMap))
						for addr := range addressMap {
							addresses = append(addresses, addr)
						}
						slog.Info("成功通过 QuickNode RPC 获取地址", 
							"count", len(addresses),
							"hyperevm_collected", len(collectedAddresses),
							"quicknode_collected", len(quickNodeAddresses),
							"source", "QuickNode RPC + HyperEVM RPC")
						chainErr = nil // 合并成功，清除错误
					} else {
						// QuickNode 也失败，但如果有已收集的地址，使用它们
						if len(collectedAddresses) > 0 {
							addresses = collectedAddresses
							slog.Warn("QuickNode RPC 也失败，使用 HyperEVM RPC 已收集的地址",
								"count", len(addresses),
								"quicknode_error", quickNodeErr)
							chainErr = nil // 有部分数据，不清除错误但继续处理
						}
					}
				} else if len(collectedAddresses) > 0 {
					// 没有 QuickNode RPC，但 HyperEVM 收集到了地址，使用它们
					addresses = collectedAddresses
					slog.Info("未配置 QuickNode RPC，使用 HyperEVM RPC 已收集的地址",
						"count", len(addresses))
					chainErr = nil // 有部分数据，继续处理
				}
			}
		} else if rpcClient != nil {
			slog.Info("使用 QuickNode RPC 查询地址（仅此选项）", "rpc_url", cfg.Follow.RpcUrl)
			addresses, chainErr = rpcClient.QueryAllAddressesFromChain(fromBlock, toBlock, slog.Default())
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
				slog.Error("QuickNode 已达到每日请求限制",
					"error", err,
					"tip", "请等待24小时后重试，或升级 QuickNode 账户，或使用 --block-range 参数限制查询范围以减少请求数")
				exitWithError(fmt.Errorf("QuickNode 每日请求限制已到达: %w\n提示：可以使用 --block-range 参数限制查询范围（例如：--block-range 10000 只查询最近10000个区块）", err))
			}
			slog.Error("从链上获取地址列表失败", "error", err)
			exitWithError(err)
		}
	}

	if len(addresses) == 0 {
		slog.Warn("未获取到任何地址")
		return
	}

	slog.Info("开始通过 Hyperliquid Info API 获取账户数据并保存", "total_addresses", len(addresses))

	// 创建 Worker Pool 来并发查询账户数据
	resultCh := make(chan *dao.UserAccount, indexChainBatchSize*2)
	var wg sync.WaitGroup
	var processedCount int64
	var successCount int64
	var errorCount int64

	// 创建地址通道
	addressCh := make(chan string, indexChainMaxConcurrency*2)
	go func() {
		defer close(addressCh)
		for _, addr := range addresses {
			addressCh <- addr
		}
	}()

	// 启动 Worker
	for i := 0; i < indexChainMaxConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			time.Sleep(time.Duration(workerID) * indexChainWorkerDelay)

			for address := range addressCh {
				atomic.AddInt64(&processedCount, 1)
				current := atomic.LoadInt64(&processedCount)

				if current%100 == 0 || current == 1 {
					slog.Info("查询进度",
						"processed", current,
						"total", len(addresses),
						"success", atomic.LoadInt64(&successCount),
						"error", atomic.LoadInt64(&errorCount),
						"progress", fmt.Sprintf("%.2f%%", float64(current)/float64(len(addresses))*100))
				}

				// 三级回退策略：Info API -> HyperEVM RPC -> QuickNode RPC
				var data *dao.UserAccount
				var err error
				
				// 1. 优先使用 Info API
				data, err = queryAccountDataFromInfoAPI(hyperClient, address, slog.Default())
				if err != nil {
					// 2. Info API 失败，尝试 HyperEVM RPC
					if hyperEvmClient != nil {
						slog.Debug("Info API 失败，尝试 HyperEVM RPC",
							"worker", workerID,
							"address", address,
							"info_api_error", err)
						data, err = queryAccountDataFromHyperEVM(hyperEvmClient, address, slog.Default())
					}
					
					// 3. HyperEVM RPC 也失败，尝试 QuickNode RPC
					if err != nil && rpcClient != nil {
						slog.Debug("HyperEVM RPC 失败，尝试 QuickNode RPC",
							"worker", workerID,
							"address", address,
							"hyperevm_error", err)
						data, err = queryAccountDataFromQuickNodeForIndex(rpcClient, address, slog.Default())
					}
					
					// 4. 所有方法都失败，保存空数据
					if err != nil {
						atomic.AddInt64(&errorCount, 1)
						if atomic.LoadInt64(&errorCount) <= 10 || atomic.LoadInt64(&errorCount)%100 == 0 {
							slog.Warn("所有数据源查询失败，保存空数据",
								"worker", workerID,
								"address", address,
								"error", err,
								"tip", "即使查询失败，也会保存地址信息（数据为0）")
						}
						// 即使查询失败，也保存地址信息（数据为0），确保地址被记录
						emptyData := &dao.UserAccount{
							Address:          address,
							TotalEquity:      0,
							AvailableBalance: 0,
							UsedMargin:       0,
							TotalPnl:         0,
							TotalFee:         0,
							TradeCount7d:      0,
							UpdatedAt:        time.Now(),
							Source:           "All Sources Failed",
						}
						resultCh <- emptyData
						continue
					}
				}

				atomic.AddInt64(&successCount, 1)
				resultCh <- data
			}
		}(i)
	}

	// 等待所有 Worker 完成
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// 批量保存结果
	saveIndexResults(resultCh, len(addresses))

	slog.Info("账户数据保存完成",
		"total_addresses", len(addresses),
		"processed", atomic.LoadInt64(&processedCount),
		"success", atomic.LoadInt64(&successCount),
		"error", atomic.LoadInt64(&errorCount))
}

// queryAccountDataFromInfoAPI 通过 Hyperliquid Info API 查询账户数据
// 获取 address、total_equity、available_balance、used_margin、total_pnl、total_fee、trade_count_7d 等信息
func queryAccountDataFromInfoAPI(client *hyper.Client, address string, logger *slog.Logger) (*dao.UserAccount, error) {
	data := &dao.UserAccount{
		Address:   address,
		UpdatedAt: time.Now(),
		Source:    "Info API",
	}

	const maxValue = 9999999999999999999999999999.999999999999

	// 1. 获取账户摘要 (clearinghouseState)
	summary, err := client.GetUserAccountSummary(address)
	if err != nil {
		if err == hyper.ErrNull {
			return data, hyper.ErrNull
		}
		return nil, err
	}

	// 2. 提取总权益 (total_equity)
	// 优先使用 CrossMarginSummary.AccountValue，如果没有则使用 MarginSummary.AccountValue
	if summary.CrossMarginSummary.AccountValue != nil {
		accountValue, err := summary.CrossMarginSummary.AccountValue.Float64()
		if err == nil {
			if accountValue > maxValue {
				data.TotalEquity = maxValue
			} else if accountValue < -maxValue {
				data.TotalEquity = -maxValue
			} else {
				data.TotalEquity = accountValue
			}
		}
	} else if summary.MarginSummary.AccountValue != nil {
		accountValue, err := summary.MarginSummary.AccountValue.Float64()
		if err == nil {
			if accountValue > maxValue {
				data.TotalEquity = maxValue
			} else if accountValue < -maxValue {
				data.TotalEquity = -maxValue
			} else {
				data.TotalEquity = accountValue
			}
		}
	}

	// 3. 提取可用余额 (available_balance) - 从 Withdrawable 获取
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
	// 优先使用 CrossMarginSummary.TotalMarginUsed，如果没有则使用 MarginSummary.TotalMarginUsed
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
		marginUsed, err := summary.MarginSummary.TotalMarginUsed.Float64()
		if err == nil {
			if marginUsed > maxValue {
				data.UsedMargin = maxValue
			} else {
				data.UsedMargin = marginUsed
			}
		}
	}

	// 5. 获取交易记录，计算累计盈亏、累计手续费和7日内交易次数
	fills, err := client.GetUserFills(address)
	if err != nil {
		if err == hyper.ErrNull {
			// API 返回 null，但账户摘要已获取成功，返回部分数据
			return data, nil
		}
		// 其他错误，返回部分数据（账户摘要已获取）
		return data, nil
	}

	// 计算7天前的时间戳
	sevenDaysAgo := time.Now().AddDate(0, 0, -7).UnixMilli()

	var (
		totalPnl     float64 // 累计盈亏
		totalFee     float64 // 累计手续费
		tradeCount7d int     // 7日内交易次数
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

// queryAccountDataFromHyperEVM 从 HyperEVM RPC 查询账户数据（备用方案）
// 从链上获取交易数据并计算 total_pnl 和 total_fee
func queryAccountDataFromHyperEVM(rpcClient *rpc.Client, address string, logger *slog.Logger) (*dao.UserAccount, error) {
	logger.Info("尝试从 HyperEVM RPC 获取数据", "address", address)
	
	data := &dao.UserAccount{
		Address:     address,
		TotalEquity: 0, // 从链上无法直接获取，需要通过合约调用
		UpdatedAt:   time.Now(),
		Source:      "HyperEVM RPC", // 标记数据来源
	}

	// 如果 RPC 客户端未配置，返回错误
	if rpcClient == nil {
		logger.Warn("HyperEVM RPC 客户端未配置，无法从链上获取数据", "address", address)
		return data, fmt.Errorf("HyperEVM RPC 客户端未配置")
	}

	// 1. 获取最新区块号
	logger.Debug("从 HyperEVM 获取最新区块号", "address", address)
	latestBlock, err := rpcClient.GetBlockNumber()
	if err != nil {
		logger.Warn("从 HyperEVM 获取最新区块号失败", "error", err, "address", address)
		return data, fmt.Errorf("获取最新区块号失败: %w", err)
	}
	logger.Info("成功从 HyperEVM 获取最新区块号", "address", address, "block", latestBlock.String())

	// 2. 计算查询范围
	// HyperEVM RPC 限制：每次查询最多 50 个区块
	// QueryUserFillsFromChain 函数内部会自动分批查询
	// 为了性能，可以只查询最近的区块，比如最近 1000 个区块（会分批查询）
	fromBlock := big.NewInt(0)
	if latestBlock.Cmp(big.NewInt(1000)) > 0 {
		fromBlock.Sub(latestBlock, big.NewInt(1000))
	}
	logger.Debug("计算查询区块范围", "address", address, "from_block", fromBlock.String(), "to_block", latestBlock.String(), "note", "将自动分批查询，每批最多50个区块")

	// 3. 从链上查询交易记录
	logger.Info("从 HyperEVM 查询链上交易记录", "address", address, "from_block", fromBlock.String(), "to_block", latestBlock.String())
	fills, err := rpcClient.QueryUserFillsFromChain(address, fromBlock, latestBlock, logger)
	if err != nil {
		logger.Warn("从 HyperEVM 查询链上交易记录失败", "error", err, "address", address)
		return data, fmt.Errorf("查询链上交易记录失败: %w", err)
	}
	
	logger.Info("成功从 HyperEVM 获取链上交易记录", "address", address, "fills_count", len(fills))

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

	// 标记数据来源为 HyperEVM RPC
	data.Source = "HyperEVM RPC"

	logger.Info("成功从 HyperEVM 计算账户数据",
		"address", address,
		"total_pnl", data.TotalPnl,
		"total_fee", data.TotalFee,
		"trade_count_7d", data.TradeCount7d,
		"fills_count", len(fills))

	// 注意：total_equity, available_balance, used_margin 需要调用合约函数获取
	// 目前从 RPC 只能获取 total_pnl 和 total_fee
	return data, nil
}

// queryAccountDataFromQuickNodeForIndex 从 QuickNode RPC 查询账户数据（用于 index-chain 命令）
// 从链上获取交易数据并计算 total_pnl 和 total_fee
func queryAccountDataFromQuickNodeForIndex(rpcClient *rpc.Client, address string, logger *slog.Logger) (*dao.UserAccount, error) {
	logger.Info("尝试从 QuickNode RPC 获取数据", "address", address)
	
	data := &dao.UserAccount{
		Address:     address,
		TotalEquity: 0, // 从链上无法直接获取，需要通过合约调用
		UpdatedAt:   time.Now(),
		Source:      "QuickNode RPC", // 标记数据来源
	}

	// 如果 RPC 客户端未配置，返回错误
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
	// QuickNode 免费计划限制：每次查询最多 50 个区块
	// QueryUserFillsFromChain 函数内部会自动分批查询
	// 为了性能，可以只查询最近的区块，比如最近 1000 个区块（会分批查询）
	fromBlock := big.NewInt(0)
	if latestBlock.Cmp(big.NewInt(1000)) > 0 {
		fromBlock.Sub(latestBlock, big.NewInt(1000))
	}
	logger.Debug("计算查询区块范围", "address", address, "from_block", fromBlock.String(), "to_block", latestBlock.String(), "note", "将自动分批查询，每批最多50个区块")

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

	// 标记数据来源为 QuickNode RPC
	data.Source = "QuickNode RPC"

	logger.Info("成功从 QuickNode 计算账户数据",
		"address", address,
		"total_pnl", data.TotalPnl,
		"total_fee", data.TotalFee,
		"trade_count_7d", data.TradeCount7d,
		"fills_count", len(fills))

	// 注意：total_equity, available_balance, used_margin 需要调用合约函数获取
	// 目前从 RPC 只能获取 total_pnl 和 total_fee
	return data, nil
}

// queryAccountDataFromRPC 从 QuickNode RPC 查询账户数据（已废弃，保留用于兼容）
func queryAccountDataFromRPC(rpcClient *rpc.Client, address string, logger *slog.Logger) (*dao.UserAccount, error) {
	data := &dao.UserAccount{
		Address:     address,
		TotalEquity: 0, // 从链上无法直接获取，需要通过合约调用
		UpdatedAt:   time.Now(),
		Source:      "QuickNode RPC",
	}

	// 如果 RPC 客户端未配置，返回错误
	if rpcClient == nil {
		return data, fmt.Errorf("QuickNode RPC 客户端未配置")
	}

	// 1. 获取最新区块号
	latestBlock, err := rpcClient.GetBlockNumber()
	if err != nil {
		return data, fmt.Errorf("获取最新区块号失败: %w", err)
	}

	// 2. 计算查询范围（查询全部历史）
	fromBlock := big.NewInt(0)
	toBlock := latestBlock

	// 3. 从链上查询交易记录
	fills, err := rpcClient.QueryUserFillsFromChain(address, fromBlock, toBlock, logger)
	if err != nil {
		return data, fmt.Errorf("查询链上交易记录失败: %w", err)
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

	// 注意：total_equity, available_balance, used_margin 需要调用合约函数获取
	// 这需要合约 ABI 和函数签名，目前暂时为 0
	// TODO: 实现合约调用获取账户状态

	return data, nil
}

// saveIndexResults 批量保存索引结果
func saveIndexResults(resultCh <-chan *dao.UserAccount, total int) {
	var results []*dao.UserAccount
	var savedCount int

	for result := range resultCh {
		// 所有记录都保存，不跳过任何数据
		// 所有记录都保存，不跳过任何数据
		results = append(results, result)

		// 批量保存
		if len(results) >= indexChainBatchSize {
			err := dao.BatchSaveUserAccount(results)
			if err != nil {
				slog.Error("批量保存失败", "error", err, "count", len(results))
			} else {
				savedCount += len(results)
				slog.Info("批量保存成功",
					"batch_size", len(results),
					"total_saved", savedCount,
					"progress", fmt.Sprintf("%.2f%%", float64(savedCount)/float64(total)*100))
			}
			results = results[:0]
		}
	}

	// 保存剩余数据
	if len(results) > 0 {
		err := dao.BatchSaveUserAccount(results)
		if err != nil {
			slog.Error("批量保存剩余数据失败", "error", err, "count", len(results))
		} else {
			savedCount += len(results)
			slog.Info("批量保存剩余数据成功",
				"batch_size", len(results),
				"total_saved", savedCount)
		}
	}

	slog.Info("索引数据保存完成",
		"total_saved", savedCount)
}
