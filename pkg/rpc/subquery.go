package rpc

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// SubQueryTransaction SubQuery 返回的交易数据
type SubQueryTransaction struct {
	FromAddress     string    // 发送者地址
	ToAddress       string    // 接收者地址
	BlockNumber     int64     // 区块高度
	BlockTime       time.Time // 区块时间戳
	TransactionHash string    // 交易哈希
	LogIndex        int       // 日志索引（0表示交易本身，>0表示日志）
	EventSignature  string    // 事件签名（日志的 Topics[0]）
	EventData       string    // 事件数据（JSON格式）
	Topics          []string  // Topics
}

// SubQueryCallback 回调函数类型，用于实时处理每个交易
type SubQueryCallback func(tx *SubQueryTransaction)

// QuerySubQueryAllTransactions 通过遍历所有区块获取所有链上交易数据
// 这个方法会遍历所有区块，获取每个区块中的所有交易，提取 from 和 to 地址
func (c *Client) QuerySubQueryAllTransactions(logger *slog.Logger) ([]SubQueryTransaction, error) {
	return c.QuerySubQueryAllTransactionsWithCallback(logger, nil)
}

// QuerySubQueryAllTransactionsWithCallback 通过遍历所有区块获取所有链上交易数据（带回调）
// callback: 可选的回调函数，每处理一个交易就调用一次
func (c *Client) QuerySubQueryAllTransactionsWithCallback(logger *slog.Logger, callback SubQueryCallback) ([]SubQueryTransaction, error) {
	if c == nil {
		return nil, fmt.Errorf("RPC client is nil")
	}

	// 获取最新区块号
	logger.Info("正在获取最新区块号...")
	latestBlock, err := c.GetBlockNumber()
	if err != nil {
		return nil, fmt.Errorf("获取最新区块号失败: %w", err)
	}
	logger.Info("成功获取最新区块号", "latest_block", latestBlock.String())

	startBlock := big.NewInt(0)
	endBlock := latestBlock

	totalBlocks := new(big.Int).Sub(endBlock, startBlock).Int64() + 1
	logger.Info("开始遍历所有区块获取交易数据",
		"from_block", startBlock.String(),
		"to_block", endBlock.String(),
		"total_blocks", totalBlocks,
		"direction", "从最新区块往前遍历到区块0",
		"note", "这将需要较长时间，请耐心等待...")

	// 使用 map 存储唯一的交易（避免重复）
	// key: txHash-logIndex
	txMap := make(map[string]*SubQueryTransaction)
	
	// 用于实时统计地址收集情况
	addressSet := make(map[string]bool)
	lastAddressLogTime := time.Now()
	const addressLogInterval = 10 * time.Second // 每10秒输出一次地址统计

	// 从最新区块往前遍历（提高效率）
	currentTo := new(big.Int).Set(endBlock)
	batchCount := 0
	const batchSize = 50 // 每批处理50个区块
	lastLogTime := time.Now()
	const logInterval = 5 * time.Second // 每5秒输出一次进度
	const blockDelay = 150 * time.Millisecond // 每个区块之间延迟150ms（提高到约6-7请求/秒）
	const batchDelay = 1 * time.Second // 每批之间延迟1秒（减少延迟）

	for currentTo.Cmp(startBlock) >= 0 {
		// 计算当前批次的起始区块
		currentFrom := new(big.Int).Sub(currentTo, big.NewInt(batchSize-1))
		if currentFrom.Cmp(startBlock) < 0 {
			currentFrom.Set(startBlock)
		}

		if currentTo.Cmp(currentFrom) < 0 {
			break
		}

		batchCount++
		
		// 每批都输出进度，或者每5秒输出一次
		now := time.Now()
		if batchCount == 1 || batchCount%10 == 0 || now.Sub(lastLogTime) >= logInterval {
			processedBlocks := new(big.Int).Sub(endBlock, currentFrom).Int64() + 1
			progress := float64(processedBlocks) / float64(totalBlocks) * 100
			logger.Info("处理进度",
				"batch", batchCount,
				"current_range", fmt.Sprintf("%s -> %s (从最新区块往前)", currentTo.String(), currentFrom.String()),
				"from_block", currentFrom.String(),
				"to_block", currentTo.String(),
				"processed_blocks", processedBlocks,
				"total_blocks", totalBlocks,
				"progress", fmt.Sprintf("%.2f%%", progress),
				"total_transactions", len(txMap),
				"total_unique_addresses", len(addressSet))
			lastLogTime = now
		}
		
		// 第一批开始时，明确输出从最新区块开始
		if batchCount == 1 {
			logger.Info("开始处理第一批区块（从最新区块开始）",
				"start_block", currentTo.String(),
				"end_block", currentFrom.String(),
				"batch_size", batchSize)
		}

		// 遍历当前批次的所有区块（从 currentTo 往前递减到 currentFrom）
		blockProcessedInBatch := 0
		rateLimitCount := 0 // 记录速率限制次数
		// 从 currentTo（较大的区块号）开始，递减到 currentFrom（较小的区块号）
		for blockNum := new(big.Int).Set(currentTo); blockNum.Cmp(currentFrom) >= 0; blockNum.Sub(blockNum, big.NewInt(1)) {
			// 获取区块信息
			block, err := c.GetBlockByNumberForSubQuery(blockNum.Int64(), logger)
			if err != nil {
				// 检查是否是速率限制错误
				if strings.Contains(err.Error(), "rate limit") || strings.Contains(err.Error(), "-32005") || strings.Contains(err.Error(), "-32007") {
					rateLimitCount++
					if rateLimitCount > 3 {
						// 如果连续遇到3次速率限制，增加延迟
						logger.Warn("连续遇到速率限制，增加延迟", "count", rateLimitCount)
						time.Sleep(5 * time.Second)
						rateLimitCount = 0 // 重置计数
					} else {
						time.Sleep(2 * time.Second) // 遇到速率限制时延迟2秒
					}
				}
				logger.Debug("获取区块信息失败", "block_number", blockNum.String(), "error", err)
				continue
			}
			blockProcessedInBatch++
			rateLimitCount = 0 // 成功获取后重置计数

			// 解析区块号和时间戳
			blockNumInt, err := parseHexToInt64(block.Number)
			if err != nil {
				logger.Debug("解析区块号失败", "block_number", block.Number, "error", err)
				continue
			}

			blockTime, err := parseBlockTimestamp(block.Timestamp)
			if err != nil {
				logger.Debug("解析区块时间戳失败", "timestamp", block.Timestamp, "error", err)
				continue
			}

			// 获取区块中的所有交易
			// 注意：eth_getBlockByNumber 需要 fullTx=true 才能获取完整交易信息
			blockData, err := c.GetBlockByNumber(blockNum, true)
			if err != nil {
				// 检查是否是速率限制错误
				if strings.Contains(err.Error(), "rate limit") || strings.Contains(err.Error(), "-32005") || strings.Contains(err.Error(), "-32007") {
					rateLimitCount++
					if rateLimitCount > 3 {
						logger.Warn("连续遇到速率限制，增加延迟", "count", rateLimitCount)
						time.Sleep(5 * time.Second)
						rateLimitCount = 0
					} else {
						time.Sleep(2 * time.Second)
					}
				}
				logger.Debug("获取区块交易失败", "block_number", blockNum.String(), "error", err)
				// 如果连续失败多次，输出警告
				if blockProcessedInBatch == 0 && batchCount <= 3 {
					logger.Warn("获取区块交易失败", "block_number", blockNum.String(), "error", err)
				}
				continue
			}
			rateLimitCount = 0 // 成功获取后重置计数
			
			// 每个区块处理完成后添加延迟（合并了之前的多个延迟）
			time.Sleep(blockDelay)

			// 解析区块中的交易
			transactions, ok := blockData["transactions"].([]interface{})
			if !ok {
				logger.Debug("区块中没有交易或格式错误", "block_number", blockNum.String())
				continue
			}

			// 处理每个交易
			txCountInBlock := 0
			newAddressCountInBlock := 0
			for txIndex, txInterface := range transactions {
				var err error
				txMap, err = c.processTransaction(txInterface, blockNumInt, blockTime, txIndex, txMap, logger)
				if err != nil {
					logger.Debug("处理交易失败", "block_number", blockNum.String(), "tx_index", txIndex, "error", err)
					continue
				}
				txCountInBlock++
				
				// 从交易中提取地址并统计
				txHash := getTxHashFromInterface(txInterface)
				if txHash != "" {
					txKey := fmt.Sprintf("%s-0", strings.ToLower(strings.TrimPrefix(txHash, "0x")))
					if tx, exists := txMap[txKey]; exists {
						// 如果提供了回调函数，实时调用
						if callback != nil {
							callback(tx)
						}
						
						if tx.FromAddress != "" {
							addrLower := strings.ToLower(tx.FromAddress)
							if !addressSet[addrLower] {
								addressSet[addrLower] = true
								newAddressCountInBlock++
							}
						}
						if tx.ToAddress != "" {
							addrLower := strings.ToLower(tx.ToAddress)
							if !addressSet[addrLower] {
								addressSet[addrLower] = true
								newAddressCountInBlock++
							}
						}
					}
				}
			}
			
			// 实时输出地址收集情况
			now := time.Now()
			if now.Sub(lastAddressLogTime) >= addressLogInterval {
				logger.Info("地址收集统计",
					"block_number", blockNum.String(),
					"total_unique_addresses", len(addressSet),
					"transactions_in_block", txCountInBlock,
					"new_addresses_in_block", newAddressCountInBlock)
				lastAddressLogTime = now
			}
		}

		// 移动到下一个批次
		currentTo.Sub(currentFrom, big.NewInt(1))

		// 输出批次完成信息
		if blockProcessedInBatch > 0 {
			logger.Info("批次处理完成",
				"batch", batchCount,
				"blocks_processed", blockProcessedInBatch,
				"next_from_block", currentTo.String(),
				"total_transactions", len(txMap))
		}

		// 每批之间添加延迟，避免请求过快
		time.Sleep(batchDelay)
	}

	// 转换为切片
	transactions := make([]SubQueryTransaction, 0, len(txMap))
	for _, tx := range txMap {
		transactions = append(transactions, *tx)
	}

	logger.Info("SubQuery 查询完成",
		"total_batches", batchCount,
		"total_transactions", len(transactions),
		"total_unique_addresses", len(addressSet))

	return transactions, nil
}

// getTxHashFromInterface 从交易接口中提取交易哈希（辅助函数）
func getTxHashFromInterface(txInterface interface{}) string {
	if txData, ok := txInterface.(map[string]interface{}); ok {
		if txHash, ok := txData["hash"].(string); ok {
			return txHash
		}
	}
	return ""
}

// processTransaction 处理单个交易，提取 from/to 地址和日志
func (c *Client) processTransaction(txInterface interface{}, blockNum int64, blockTime time.Time, txIndex int, resultMap map[string]*SubQueryTransaction, logger *slog.Logger) (map[string]*SubQueryTransaction, error) {
	txData, ok := txInterface.(map[string]interface{})
	if !ok {
		return resultMap, fmt.Errorf("交易格式错误")
	}

	// 提取交易基本信息
	txHash, _ := txData["hash"].(string)
	from, _ := txData["from"].(string)
	to, _ := txData["to"].(string)

	if txHash == "" {
		return resultMap, fmt.Errorf("交易哈希为空")
	}

	// 规范化地址
	from = strings.ToLower(strings.TrimPrefix(from, "0x"))
	if from != "" {
		from = "0x" + from
	}
	to = strings.ToLower(strings.TrimPrefix(to, "0x"))
	if to != "" {
		to = "0x" + to
	}

	// 创建交易记录（logIndex=0 表示交易本身）
	key := fmt.Sprintf("%s-0", strings.ToLower(strings.TrimPrefix(txHash, "0x")))
	if _, exists := resultMap[key]; !exists {
		resultMap[key] = &SubQueryTransaction{
			FromAddress:     from,
			ToAddress:       to,
			BlockNumber:     blockNum,
			BlockTime:       blockTime,
			TransactionHash: strings.ToLower(strings.TrimPrefix(txHash, "0x")),
			LogIndex:        0,
			EventSignature:  "",
			EventData:       "",
			Topics:          []string{},
		}
	}

	// 获取交易回执以获取日志
	receipt, err := c.GetTransactionReceipt(txHash, logger)
	if err != nil {
		// 如果获取回执失败，至少保存交易本身的信息
		return resultMap, nil
	}

	// 处理日志
	for logIndex, log := range receipt.Logs {
		logKey := fmt.Sprintf("%s-%d", strings.ToLower(strings.TrimPrefix(txHash, "0x")), logIndex)
		if _, exists := resultMap[logKey]; exists {
			continue
		}

		eventSig := ""
		if len(log.Topics) > 0 {
			eventSig = log.Topics[0]
		}

		// 从日志中提取地址（Topics 中可能包含地址）
		// 通常 Topics[1] 和 Topics[2] 可能是地址（如 Transfer 事件的 from 和 to）
		topics := make([]string, len(log.Topics))
		copy(topics, log.Topics)

		resultMap[logKey] = &SubQueryTransaction{
			FromAddress:     from, // 使用交易的 from
			ToAddress:       to,    // 使用交易的 to
			BlockNumber:     blockNum,
			BlockTime:       blockTime,
			TransactionHash: strings.ToLower(strings.TrimPrefix(txHash, "0x")),
			LogIndex:        logIndex,
			EventSignature:  eventSig,
			EventData:       log.Data,
			Topics:          topics,
		}
	}

	return resultMap, nil
}

// GetBlockByNumberForSubQuery 获取区块信息（用于 SubQuery）
func (c *Client) GetBlockByNumberForSubQuery(blockNumber int64, logger *slog.Logger) (*Block, error) {
	blockHex := "0x" + strconv.FormatInt(blockNumber, 16)
	if blockNumber == 0 {
		blockHex = "0x0"
	}

	params := []interface{}{blockHex, false} // false 表示不返回完整交易数据
	result, err := c.call("eth_getBlockByNumber", params)
	if err != nil {
		return nil, fmt.Errorf("获取区块信息失败: %w", err)
	}

	var block Block
	if err := json.Unmarshal(result, &block); err != nil {
		return nil, fmt.Errorf("解析区块信息失败: %w", err)
	}

	return &block, nil
}

// GetTransactionReceipt 获取交易回执
func (c *Client) GetTransactionReceipt(txHash string, logger *slog.Logger) (*TransactionReceipt, error) {
	params := []interface{}{txHash}
	result, err := c.call("eth_getTransactionReceipt", params)
	if err != nil {
		return nil, fmt.Errorf("获取交易回执失败: %w", err)
	}

	var receipt TransactionReceipt
	if err := json.Unmarshal(result, &receipt); err != nil {
		return nil, fmt.Errorf("解析交易回执失败: %w", err)
	}

	return &receipt, nil
}

// Block 区块信息
type Block struct {
	Number     string `json:"number"`
	Timestamp  string `json:"timestamp"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parentHash"`
}

// TransactionReceipt 交易回执
type TransactionReceipt struct {
	TransactionHash string `json:"transactionHash"`
	From             string `json:"from"`
	To               string `json:"to"`
	BlockNumber      string `json:"blockNumber"`
	BlockHash        string `json:"blockHash"`
	Logs             []Log  `json:"logs"`
	Status           string `json:"status"`
}

// parseHexToInt64 解析十六进制字符串为 int64
func parseHexToInt64(hexStr string) (int64, error) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	if hexStr == "" {
		return 0, nil
	}
	val, err := strconv.ParseInt(hexStr, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("解析十六进制失败: %w", err)
	}
	return val, nil
}

// parseBlockTimestamp 解析区块时间戳
func parseBlockTimestamp(timestampHex string) (time.Time, error) {
	timestamp, err := parseHexToInt64(timestampHex)
	if err != nil {
		return time.Time{}, fmt.Errorf("解析时间戳失败: %w", err)
	}
	return time.Unix(timestamp, 0), nil
}

// TopicToAddress 从 Topic 中提取地址（32字节格式，后20字节是地址）
func TopicToAddress(topic string) string {
	topic = strings.TrimPrefix(topic, "0x")
	if len(topic) < 40 {
		return ""
	}
	// 提取后40个字符（20字节 = 40个十六进制字符）
	address := topic[len(topic)-40:]
	return "0x" + strings.ToLower(address)
}
