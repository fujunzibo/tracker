package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"hyper-tracker/pkg/log"
	"hyper-tracker/pkg/rpc"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

var subQueryCmd = &cobra.Command{
	Use:   "subquery",
	Short: "通过 Hyperliquid EVM SubQuery 方法收集所有链上交易数据",
	Long: `通过 Hyperliquid EVM 支持的 SubQuery 方法获取所有链上交易数据。
SubQuery 会自动返回包含 from（发送者）和 to（接收者）字段的所有交易数据，
包括区块高度、区块时间戳、日志与事件等信息，存储到 tb_subquery 表中。

SubQuery 方法会自动获取所有底层数据，无需指定 from/to 地址作为查询参数。

使用示例：
  tracker subquery --config etc/tracker.yaml`,
	Run: func(cmd *cobra.Command, args []string) {
		runSubQuery()
	},
}

func init() {
	rootCmd.AddCommand(subQueryCmd)
}

// 保存策略常量
const (
	subQuerySaveBatchSize = 500              // 批量保存大小（每500条记录保存一次）
	subQuerySaveInterval  = 300 * time.Second // 定期保存间隔（每300秒保存一次）
)

// processAndSendTransaction 处理交易并发送到保存通道
func processAndSendTransaction(tx *rpc.SubQueryTransaction, dataCh chan<- *dao.SubQueryData, totalProcessed *int64) {
	topicsJSON, _ := json.Marshal(tx.Topics)

	// 记录1：以 from 地址为主键（如果存在）
	if tx.FromAddress != "" {
		fromAddrLower := strings.ToLower(tx.FromAddress)
		toAddrLower := strings.ToLower(tx.ToAddress)
		data := &dao.SubQueryData{
			Address:         fromAddrLower,
			FromAddress:     fromAddrLower,
			ToAddress:       toAddrLower,
			BlockNumber:     tx.BlockNumber,
			BlockTime:       tx.BlockTime,
			TransactionHash: strings.ToLower(strings.TrimPrefix(tx.TransactionHash, "0x")),
			LogIndex:        tx.LogIndex,
			EventSignature:  tx.EventSignature,
			EventData:       tx.EventData,
			Topics:          string(topicsJSON),
			CreatedAt:       time.Now(),
			UpdatedAt:       time.Now(),
		}
		select {
		case dataCh <- data:
			*totalProcessed++
		default:
			// 通道已满，记录警告但不阻塞
			slog.Warn("保存通道已满，数据可能丢失", "tx_hash", tx.TransactionHash)
		}
	}

	// 记录2：以 to 地址为主键（如果存在且与 from 不同）
	if tx.ToAddress != "" && !strings.EqualFold(tx.ToAddress, tx.FromAddress) {
		fromAddrLower := strings.ToLower(tx.FromAddress)
		toAddrLower := strings.ToLower(tx.ToAddress)
		data := &dao.SubQueryData{
			Address:         toAddrLower,
			FromAddress:     fromAddrLower,
			ToAddress:       toAddrLower,
			BlockNumber:     tx.BlockNumber,
			BlockTime:       tx.BlockTime,
			TransactionHash: strings.ToLower(strings.TrimPrefix(tx.TransactionHash, "0x")),
			LogIndex:        tx.LogIndex,
			EventSignature:  tx.EventSignature,
			EventData:       tx.EventData,
			Topics:          string(topicsJSON),
			CreatedAt:       time.Now(),
			UpdatedAt:       time.Now(),
		}
		select {
		case dataCh <- data:
			*totalProcessed++
		default:
			slog.Warn("保存通道已满，数据可能丢失", "tx_hash", tx.TransactionHash)
		}
	}

	// 记录3：从 Topics 中提取的地址（如果与 from/to 不同）
	for _, topic := range tx.Topics {
		addr := rpc.TopicToAddress(topic)
		if addr != "" {
			addrLower := strings.ToLower(addr)
			if !strings.EqualFold(addr, tx.FromAddress) && !strings.EqualFold(addr, tx.ToAddress) {
				fromAddrLower := strings.ToLower(tx.FromAddress)
				toAddrLower := strings.ToLower(tx.ToAddress)
				data := &dao.SubQueryData{
					Address:         addrLower,
					FromAddress:     fromAddrLower,
					ToAddress:       toAddrLower,
					BlockNumber:     tx.BlockNumber,
					BlockTime:       tx.BlockTime,
					TransactionHash: strings.ToLower(strings.TrimPrefix(tx.TransactionHash, "0x")),
					LogIndex:        tx.LogIndex,
					EventSignature:  tx.EventSignature,
					EventData:       tx.EventData,
					Topics:          string(topicsJSON),
					CreatedAt:       time.Now(),
					UpdatedAt:       time.Now(),
				}
				select {
				case dataCh <- data:
					*totalProcessed++
				default:
					slog.Warn("保存通道已满，数据可能丢失", "tx_hash", tx.TransactionHash)
				}
			}
		}
	}
}

// saveSubQueryData 批量保存 SubQuery 数据（带定时保存）
func saveSubQueryData(ctx context.Context, dataCh <-chan *dao.SubQueryData, batchSize int, saveInterval time.Duration, totalSaved *int64) {
	var results []*dao.SubQueryData

	// 创建定时器，用于定期保存
	ticker := time.NewTicker(saveInterval)
	defer ticker.Stop()

	// 定期保存函数
	saveBatch := func(reason string) {
		if len(results) > 0 {
			err := dao.BatchSaveSubQueryData(results)
			if err != nil {
				slog.Error("批量保存失败", "error", err, "count", len(results), "reason", reason)
			} else {
				*totalSaved += int64(len(results))
				slog.Info("批量保存成功",
					"batch_size", len(results),
					"total_saved", *totalSaved,
					"reason", reason)
				results = results[:0] // 清空切片但保留容量
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			// 上下文取消，保存剩余数据
			saveBatch("context_cancelled")
			slog.Info("保存协程退出", "total_saved", *totalSaved)
			return

		case data, ok := <-dataCh:
			if !ok {
				// Channel 已关闭，保存剩余数据
				saveBatch("channel_closed")
				slog.Info("SubQuery 数据保存完成", "total_saved", *totalSaved)
				return
			}

			// 添加数据到批次
			results = append(results, data)

			// 达到批量大小时立即保存
			if len(results) >= batchSize {
				saveBatch("batch_size_reached")
			}

		case <-ticker.C:
			// 定期保存（即使未达到批量大小）
			if len(results) > 0 {
				saveBatch("time_interval")
			} else {
				slog.Debug("定期保存检查：ticker 触发但暂无数据",
					"results_count", len(results),
					"total_saved", *totalSaved)
			}
		}
	}
}

func runSubQuery() {
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
	err = dao.CreateSubQueryTable()
	if err != nil {
		slog.Error("创建表失败", "error", err)
		exitWithError(err)
	}
	slog.Info("表创建/检查完成")

	// 创建 RPC 客户端
	// 优先使用 HyperEVM RPC，如果没有配置则使用默认值
	var rpcClient *rpc.Client
	hyperEvmRpcUrl := cfg.Follow.HyperEvmRpcUrl
	if strings.ToLower(hyperEvmRpcUrl) == "disable" {
		slog.Info("HyperEVM RPC 已禁用，尝试使用 QuickNode RPC")
		if cfg.Follow.RpcUrl != "" {
			rpcClient = rpc.NewClient(cfg.Follow.RpcUrl, cfg.Network.LocalAddrs, cfg.Network.Proxies)
			slog.Info("已创建 QuickNode RPC 客户端", "url", cfg.Follow.RpcUrl)
		} else {
			slog.Error("未配置 RPC URL，无法查询链上数据")
			exitWithError(fmt.Errorf("未配置 RPC URL，请配置 Follow.RpcUrl 或 Follow.HyperEvmRpcUrl"))
		}
	} else {
		if hyperEvmRpcUrl == "" {
			// 如果未配置，使用默认值（保持向后兼容）
			hyperEvmRpcUrl = "https://rpc.hyperliquid.xyz/evm"
			slog.Info("使用默认 HyperEVM RPC URL", "url", hyperEvmRpcUrl)
		} else {
			slog.Info("使用配置的 HyperEVM RPC URL", "url", hyperEvmRpcUrl)
		}
		rpcClient = rpc.NewClient(hyperEvmRpcUrl, cfg.Network.LocalAddrs, cfg.Network.Proxies)
		slog.Info("已创建 HyperEVM RPC 客户端", "url", hyperEvmRpcUrl)
	}

	slog.Info("开始通过 SubQuery 方法获取所有链上交易数据",
		"note", "这将遍历所有区块，可能需要很长时间，请耐心等待...",
		"save_batch_size", subQuerySaveBatchSize,
		"save_interval", subQuerySaveInterval)

	// 用于在查询过程中保存数据的通道和状态
	dataCh := make(chan *dao.SubQueryData, 1000) // 缓冲通道，避免阻塞
	var totalProcessed int64
	var totalSaved int64

	// 启动后台保存协程
	saveCtx, saveCancel := context.WithCancel(context.Background())
	defer saveCancel()
	
	var saveWg sync.WaitGroup
	saveWg.Add(1)
	go func() {
		defer saveWg.Done()
		saveSubQueryData(saveCtx, dataCh, subQuerySaveBatchSize, subQuerySaveInterval, &totalSaved)
	}()

	// 通过 SubQuery 方法查询所有交易数据（带回调保存）
	transactions, err := rpcClient.QuerySubQueryAllTransactionsWithCallback(slog.Default(), func(tx *rpc.SubQueryTransaction) {
		// 实时处理每个交易，发送到保存通道
		processAndSendTransaction(tx, dataCh, &totalProcessed)
	})
	if err != nil {
		saveCancel() // 取消保存协程
		slog.Error("SubQuery 查询失败", "error", err)
		exitWithError(err)
	}

	// 关闭通道，等待保存完成
	close(dataCh)
	saveWg.Wait()

	slog.Info("SubQuery 查询和保存完成",
		"total_transactions", len(transactions),
		"total_processed", totalProcessed,
		"total_saved", totalSaved)

	if len(transactions) == 0 {
		slog.Info("未找到任何交易数据")
		return
	}
}
