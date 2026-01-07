package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hyper-tracker/pkg/utils"
	"io"
	"log/slog"
	"math"
	"math/big"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	// 全局 RPC 速率限制器（所有 RPC 客户端共享）
	// QuickNode 免费计划限制：15 请求/秒
	globalRPCRateLimiter *utils.RateLimiter
	rpcRateLimiterOnce   sync.Once
)

// Client EVM RPC 客户端
type Client struct {
	rpcURL string
	client *utils.HttpClient
}

// NewClient 创建新的 RPC 客户端
func NewClient(rpcURL string, addrs []string, proxies []string) *Client {
	if rpcURL == "" {
		return nil
	}
	client := utils.NewHttpClient(utils.WithLocalAddrPoolAndProxies(addrs, proxies), utils.WithTimeout(30*time.Second))
	return &Client{
		rpcURL: rpcURL,
		client: client,
	}
}

// RPCRequest RPC 请求结构
type RPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

// RPCResponse RPC 响应结构
type RPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage  `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

// RPCError RPC 错误结构
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// getGlobalRPCRateLimiter 获取全局 RPC 速率限制器
func getGlobalRPCRateLimiter() *utils.RateLimiter {
	rpcRateLimiterOnce.Do(func() {
		// QuickNode 免费计划限制：15 请求/秒
		// 考虑到可能有多个进程同时运行，进一步降低到 8 请求/秒（留更多余量）
		// 如果有 4 个进程，每个进程 8 请求/秒，总共 32 请求/秒，仍然可能超过限制
		// 但通过速率限制器，可以平滑请求分布
		globalRPCRateLimiter = utils.NewRateLimiter(80) // 每10秒80个请求 = 8请求/秒
	})
	return globalRPCRateLimiter
}

// call 执行 RPC 调用（带速率限制和重试）
func (c *Client) call(method string, params []interface{}) (json.RawMessage, error) {
	return c.callWithRetry(method, params, 0)
}

// callWithRetry 执行 RPC 调用（带速率限制和指数退避重试）
func (c *Client) callWithRetry(method string, params []interface{}, retryCount int) (json.RawMessage, error) {
	const (
		maxRetries     = 5              // 最大重试次数（增加到5次）
		baseDelay      = 2 * time.Second // 基础延迟时间：2秒（增加到2秒）
		maxDelay       = 30 * time.Second // 最大延迟时间：30秒（增加到30秒）
		jitterPercent  = 0.2            // 随机抖动百分比（±20%，增加随机性）
	)

	// 等待速率限制
	rateLimiter := getGlobalRPCRateLimiter()
	ctx := context.Background()
	if err := rateLimiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("速率限制等待失败: %w", err)
	}
	req := RPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      1,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, c.rpcURL, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	httpClient := c.client.Client()
	httpClient.Timeout = 30 * time.Second
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	// 检查响应是否为空
	if len(body) == 0 {
		return nil, fmt.Errorf("RPC 响应为空，状态码: %d", resp.StatusCode)
	}

	// 先解析 JSON 响应，检查是否是 RPC 错误
	var rpcResp RPCResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		// JSON 解析失败，可能是 HTTP 级别的错误
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("HTTP 状态码错误: %d, 响应: %s", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("unmarshal response: %w, 响应内容: %s", err, string(body))
	}

	// 检查是否是 RPC 级别的错误
	if rpcResp.Error != nil {
		// 检查是否是每日请求限制错误（-32003）
		if rpcResp.Error.Code == -32003 {
			return nil, fmt.Errorf("QuickNode 每日请求限制已到达: %s", rpcResp.Error.Message)
		}
		
		// 检查是否是速率限制错误（-32007）
		if rpcResp.Error.Code == -32007 {
			// RPC 429 错误，进行指数退避重试
			if retryCount >= maxRetries {
				return nil, fmt.Errorf("达到最大重试次数 (%d)，仍然收到 429 错误，响应: %s", maxRetries, string(body))
			}
			
			// 计算指数退避延迟时间
			delay := time.Duration(float64(baseDelay) * math.Pow(2, float64(retryCount)))
			if delay > maxDelay {
				delay = maxDelay
			}
			
			// 添加随机抖动，避免多个客户端同时重试
			jitter := time.Duration(float64(delay) * jitterPercent * (rand.Float64()*2 - 1)) // -20% 到 +20%
			delay = delay + jitter
			if delay < 0 {
				delay = baseDelay
			}
			
			slog.Warn("RPC 收到 429 错误（-32007），进行指数退避重试",
				"method", method,
				"retry_count", retryCount+1,
				"max_retries", maxRetries,
				"delay", delay,
				"message", rpcResp.Error.Message)
			
			// 等待后重试
			time.Sleep(delay)
			return c.callWithRetry(method, params, retryCount+1)
		}
		
		// 其他 RPC 错误，直接返回
		return nil, fmt.Errorf("rpc error [%d]: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	// 检查 HTTP 状态码
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == 429 {
		// HTTP 429 错误，进行指数退避重试
		if retryCount >= maxRetries {
			return nil, fmt.Errorf("达到最大重试次数 (%d)，仍然收到 429 错误，响应: %s", maxRetries, string(body))
		}
		
		// 计算指数退避延迟时间
		delay := time.Duration(float64(baseDelay) * math.Pow(2, float64(retryCount)))
		if delay > maxDelay {
			delay = maxDelay
		}
		
		// 添加随机抖动，避免多个客户端同时重试
		jitter := time.Duration(float64(delay) * jitterPercent * (rand.Float64()*2 - 1)) // -20% 到 +20%
		delay = delay + jitter
		if delay < 0 {
			delay = baseDelay
		}
		
		slog.Warn("RPC 收到 HTTP 429 错误，进行指数退避重试",
			"method", method,
			"retry_count", retryCount+1,
			"max_retries", maxRetries,
			"delay", delay)
		
		// 等待后重试
		time.Sleep(delay)
		return c.callWithRetry(method, params, retryCount+1)
	}
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP 状态码错误: %d, 响应: %s", resp.StatusCode, string(body))
	}

	// 检查其他 RPC 错误
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("rpc error [%d]: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	// 检查 Result 是否为空
	if len(rpcResp.Result) == 0 {
		return nil, fmt.Errorf("RPC 响应 Result 为空，完整响应: %s", string(body))
	}

	return rpcResp.Result, nil
}

// GetLogsRequest eth_getLogs 请求参数
type GetLogsRequest struct {
	FromBlock string   `json:"fromBlock,omitempty"` // "latest", "earliest", 或 "0x" + 十六进制区块号
	ToBlock   string   `json:"toBlock,omitempty"`   // "latest", "earliest", 或 "0x" + 十六进制区块号
	Address   string   `json:"address,omitempty"`   // 合约地址（可选）
	Topics    []string `json:"topics,omitempty"`    // 事件主题（可选）
}

// Log 事件日志
type Log struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockNumber      string   `json:"blockNumber"`
	BlockHash        string   `json:"blockHash"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

// GetLogs 查询事件日志
func (c *Client) GetLogs(req GetLogsRequest) ([]Log, error) {
	params := []interface{}{req}
	result, err := c.call("eth_getLogs", params)
	if err != nil {
		return nil, err
	}

	var logs []Log
	if err := json.Unmarshal(result, &logs); err != nil {
		return nil, fmt.Errorf("unmarshal logs: %w", err)
	}

	return logs, nil
}

// CallRequest eth_call 请求参数
type CallRequest struct {
	To   string `json:"to"`   // 合约地址
	Data string `json:"data"` // 函数调用数据（十六进制）
}

// Call 调用合约函数
func (c *Client) Call(req CallRequest, blockNumber string) (string, error) {
	if blockNumber == "" {
		blockNumber = "latest"
	}
	params := []interface{}{req, blockNumber}
	result, err := c.call("eth_call", params)
	if err != nil {
		return "", err
	}

	var resultStr string
	if err := json.Unmarshal(result, &resultStr); err != nil {
		return "", fmt.Errorf("unmarshal result: %w", err)
	}

	return resultStr, nil
}

// GetBlockNumber 获取最新区块号
func (c *Client) GetBlockNumber() (*big.Int, error) {
	result, err := c.call("eth_blockNumber", []interface{}{})
	if err != nil {
		return nil, fmt.Errorf("调用 eth_blockNumber 失败: %w", err)
	}

	// 检查 result 是否为空
	if len(result) == 0 {
		return nil, fmt.Errorf("eth_blockNumber 返回结果为空")
	}

	var blockNumberHex string
	if err := json.Unmarshal(result, &blockNumberHex); err != nil {
		return nil, fmt.Errorf("unmarshal block number: %w, 原始结果: %s", err, string(result))
	}

	// 检查 blockNumberHex 是否为空
	if blockNumberHex == "" {
		return nil, fmt.Errorf("区块号字符串为空，原始结果: %s", string(result))
	}

	blockNumber, err := hexToBigInt(blockNumberHex)
	if err != nil {
		return nil, fmt.Errorf("parse block number: %w, hex: %s", err, blockNumberHex)
	}

	return blockNumber, nil
}

// GetBlockByNumber 根据区块号获取区块信息
func (c *Client) GetBlockByNumber(blockNumber *big.Int, fullTx bool) (map[string]interface{}, error) {
	blockNumberHex := "0x" + blockNumber.Text(16)
	params := []interface{}{blockNumberHex, fullTx}
	result, err := c.call("eth_getBlockByNumber", params)
	if err != nil {
		return nil, err
	}

	var block map[string]interface{}
	if err := json.Unmarshal(result, &block); err != nil {
		return nil, fmt.Errorf("unmarshal block: %w", err)
	}

	return block, nil
}

// hexToBigInt 将十六进制字符串转换为 big.Int
func hexToBigInt(hexStr string) (*big.Int, error) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	if hexStr == "" {
		return big.NewInt(0), nil
	}
	value := new(big.Int)
	_, ok := value.SetString(hexStr, 16)
	if !ok {
		return nil, fmt.Errorf("invalid hex string: %s", hexStr)
	}
	return value, nil
}

// hexToInt64 将十六进制字符串转换为 int64
func hexToInt64(hexStr string) (int64, error) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	if hexStr == "" {
		return 0, nil
	}
	return strconv.ParseInt(hexStr, 16, 64)
}

// hexToFloat64 将十六进制字符串转换为 float64（假设是 wei 单位，转换为 ETH）
func hexToFloat64(hexStr string, decimals int) (float64, error) {
	value, err := hexToBigInt(hexStr)
	if err != nil {
		return 0, err
	}
	
	// 转换为 float64，考虑小数位
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	valueFloat := new(big.Float).SetInt(value)
	result, _ := new(big.Float).Quo(valueFloat, divisor).Float64()
	return result, nil
}

// AddressToTopic 将地址转换为事件主题（32字节，左补0）
func AddressToTopic(address string) string {
	address = strings.TrimPrefix(address, "0x")
	// 地址是20字节（40个十六进制字符），需要补齐到32字节（64个十六进制字符）
	if len(address) < 64 {
		address = strings.Repeat("0", 64-len(address)) + address
	}
	return "0x" + address
}

// Hyperliquid 合约地址
var (
	// HyperliquidClearinghouseAddress Hyperliquid 清算所合约地址
	// 默认地址：0x8549fD7FFC092F8366e416E129a622EC060104ea
	HyperliquidClearinghouseAddress = "0x8549fD7FFC092F8366e416E129a622EC060104ea"
	
	// HyperliquidExchangeAddress Hyperliquid 交易所合约地址（可选）
	HyperliquidExchangeAddress = ""
)

// SetHyperliquidContractAddress 设置 Hyperliquid 合约地址
func SetHyperliquidContractAddress(clearinghouse, exchange string) {
	HyperliquidClearinghouseAddress = clearinghouse
	HyperliquidExchangeAddress = exchange
}

// QueryUserFillsFromChain 从链上查询用户的交易记录
// 注意：需要先设置合约地址和事件签名
// QuickNode 免费计划限制：每次查询最多 5 个区块范围，需要分批查询
func (c *Client) QueryUserFillsFromChain(userAddress string, fromBlock, toBlock *big.Int, logger *slog.Logger) ([]ChainFill, error) {
	if c == nil {
		return nil, fmt.Errorf("RPC client is nil")
	}

	// 检查合约地址是否已设置
	if HyperliquidClearinghouseAddress == "" && HyperliquidExchangeAddress == "" {
		logger.Debug("Hyperliquid 合约地址未设置，无法查询链上数据", "address", userAddress)
		return nil, fmt.Errorf("contract address not set, please call SetHyperliquidContractAddress first")
	}

	// 使用 Clearinghouse 合约地址（如果设置了）
	contractAddress := HyperliquidClearinghouseAddress
	if contractAddress == "" {
		contractAddress = HyperliquidExchangeAddress
	}

	// QuickNode 免费计划限制：每次查询最多 5 个区块
	const maxBlockRange = 5

	// 计算查询范围
	startBlock := fromBlock
	if startBlock == nil {
		startBlock = big.NewInt(0)
	}
	endBlock := toBlock
	if endBlock == nil {
		// 获取最新区块号
		latest, err := c.GetBlockNumber()
		if err != nil {
			return nil, fmt.Errorf("获取最新区块号失败: %w", err)
		}
		endBlock = latest
	}

	// 如果查询范围小于等于 5 个区块，直接查询
	blockRange := new(big.Int).Sub(endBlock, startBlock)
	if blockRange.Cmp(big.NewInt(maxBlockRange)) <= 0 {
		return c.queryUserFillsInRange(userAddress, contractAddress, startBlock, endBlock, logger)
	}

	// 分批查询：每次查询 5 个区块
	var allFills []ChainFill
	currentFrom := new(big.Int).Set(startBlock)

	logger.Info("开始分批查询链上交易记录",
		"address", userAddress,
		"from_block", startBlock.String(),
		"to_block", endBlock.String(),
		"total_blocks", blockRange.String(),
		"batch_size", maxBlockRange)

	batchCount := 0
	for currentFrom.Cmp(endBlock) <= 0 { // 改为 <= 确保包含 endBlock
		// 计算当前批次的结束区块（最多 5 个区块）
		currentTo := new(big.Int).Add(currentFrom, big.NewInt(maxBlockRange-1))
		if currentTo.Cmp(endBlock) > 0 {
			currentTo.Set(endBlock)
		}
		
		// 确保 currentTo >= currentFrom
		if currentTo.Cmp(currentFrom) < 0 {
			logger.Warn("批次区块范围无效，跳过",
				"batch", batchCount+1,
				"from_block", currentFrom.String(),
				"to_block", currentTo.String(),
				"address", userAddress)
			break
		}

		batchCount++
		logger.Debug("查询批次",
			"batch", batchCount,
			"from_block", currentFrom.String(),
			"to_block", currentTo.String(),
			"address", userAddress)

		// 查询当前批次
		fills, err := c.queryUserFillsInRange(userAddress, contractAddress, currentFrom, currentTo, logger)
		if err != nil {
			// 如果是范围限制错误，记录并继续
			if strings.Contains(err.Error(), "limited to a 5 range") {
				logger.Warn("批次查询超出限制，尝试缩小范围",
					"batch", batchCount,
					"from_block", currentFrom.String(),
					"to_block", currentTo.String(),
					"address", userAddress)
				// 尝试查询更小的范围（3个区块）
				currentTo = new(big.Int).Add(currentFrom, big.NewInt(2))
				if currentTo.Cmp(endBlock) > 0 {
					currentTo.Set(endBlock)
				}
				fills, err = c.queryUserFillsInRange(userAddress, contractAddress, currentFrom, currentTo, logger)
			}
			if err != nil {
				logger.Warn("批次查询失败",
					"batch", batchCount,
					"error", err,
					"address", userAddress)
				// 继续下一个批次，不中断整个查询
				currentFrom.Add(currentTo, big.NewInt(1))
				continue
			}
		}

		allFills = append(allFills, fills...)

		// 移动到下一个批次
		currentFrom.Add(currentTo, big.NewInt(1))

		// 添加延迟，避免请求过快（QuickNode 限制：15 请求/秒）
		// 每个批次之间延迟 200ms，确保不超过速率限制
		// 考虑到可能有多个进程同时运行，增加延迟时间
		time.Sleep(200 * time.Millisecond)
	}

	logger.Info("分批查询完成",
		"address", userAddress,
		"total_batches", batchCount,
		"total_fills", len(allFills))

	return allFills, nil
}

// QueryAllAddressesFromChain 从链上查询所有唯一的用户地址
// 通过查询所有 Fill 事件，提取唯一的用户地址
// fromBlock: 起始区块号，如果为 nil 则从 0 开始
// toBlock: 结束区块号，如果为 nil 则查询到最新区块
func (c *Client) QueryAllAddressesFromChain(fromBlock, toBlock *big.Int, logger *slog.Logger) ([]string, error) {
	if c == nil {
		return nil, fmt.Errorf("RPC client is nil")
	}

	// 检查合约地址是否已设置
	if HyperliquidClearinghouseAddress == "" && HyperliquidExchangeAddress == "" {
		logger.Debug("Hyperliquid 合约地址未设置，无法查询链上数据")
		return nil, fmt.Errorf("contract address not set, please call SetHyperliquidContractAddress first")
	}

	// 使用 Clearinghouse 合约地址（如果设置了）
	contractAddress := HyperliquidClearinghouseAddress
	if contractAddress == "" {
		contractAddress = HyperliquidExchangeAddress
	}

	// 计算查询范围
	startBlock := fromBlock
	if startBlock == nil {
		startBlock = big.NewInt(0)
	}
	endBlock := toBlock
	if endBlock == nil {
		// 获取最新区块号
		latest, err := c.GetBlockNumber()
		if err != nil {
			return nil, fmt.Errorf("获取最新区块号失败: %w", err)
		}
		endBlock = latest
	}

	// QuickNode 免费计划限制：每次查询最多 5 个区块
	const maxBlockRange = 5

	// 使用 map 存储唯一的地址
	addressSet := make(map[string]bool)
	
	// 构建查询参数（只查询 Fill 事件，不指定用户地址）
	fillEventSig := GetFillEventSignature()
	
	logger.Info("开始从链上查询所有地址",
		"from_block", startBlock.String(),
		"to_block", endBlock.String(),
		"contract_address", contractAddress)

	// 分批查询：每次查询 5 个区块
	currentFrom := new(big.Int).Set(startBlock)
	batchCount := 0
	
	for currentFrom.Cmp(endBlock) <= 0 {
		// 计算当前批次的结束区块（最多 5 个区块）
		currentTo := new(big.Int).Add(currentFrom, big.NewInt(maxBlockRange-1))
		if currentTo.Cmp(endBlock) > 0 {
			currentTo.Set(endBlock)
		}
		
		// 确保 currentTo >= currentFrom
		if currentTo.Cmp(currentFrom) < 0 {
			break
		}

		batchCount++
		if batchCount%100 == 0 {
			logger.Info("查询进度",
				"batch", batchCount,
				"from_block", currentFrom.String(),
				"to_block", currentTo.String(),
				"unique_addresses", len(addressSet))
		}

		// 查询当前批次的所有 Fill 事件（不指定用户地址）
		fromBlockHex := "0x" + currentFrom.Text(16)
		toBlockHex := "0x" + currentTo.Text(16)
		if currentFrom.Sign() == 0 {
			fromBlockHex = "0x0"
		}
		if currentTo.Sign() == 0 {
			toBlockHex = "0x0"
		}

		req := GetLogsRequest{
			FromBlock: fromBlockHex,
			ToBlock:   toBlockHex,
			Address:   contractAddress,
			Topics: []string{
				fillEventSig, // 只指定事件签名，不指定用户地址
			},
		}

		logs, err := c.GetLogs(req)
		if err != nil {
			// 如果是范围限制错误，尝试缩小范围
			if strings.Contains(err.Error(), "limited to a 5 range") || strings.Contains(err.Error(), "Invalid params") {
				logger.Debug("批次查询超出限制，尝试缩小范围",
					"batch", batchCount,
					"from_block", currentFrom.String(),
					"to_block", currentTo.String())
				// 尝试查询更小的范围（3个区块）
				currentTo = new(big.Int).Add(currentFrom, big.NewInt(2))
				if currentTo.Cmp(endBlock) > 0 {
					currentTo.Set(endBlock)
				}
				toBlockHex = "0x" + currentTo.Text(16)
				if currentTo.Sign() == 0 {
					toBlockHex = "0x0"
				}
				req.ToBlock = toBlockHex
				logs, err = c.GetLogs(req)
			}
			if err != nil {
				logger.Warn("批次查询失败",
					"batch", batchCount,
					"error", err,
					"from_block", currentFrom.String(),
					"to_block", currentTo.String())
				// 继续下一个批次，不中断整个查询
				currentFrom.Add(currentTo, big.NewInt(1))
				continue
			}
		}

		// 从事件日志中提取用户地址
		// Fill 事件的 Topics[1] 是用户地址（indexed）
		for _, log := range logs {
			if len(log.Topics) >= 2 {
				// Topics[1] 是用户地址（32字节格式，需要提取后20字节）
				userTopic := log.Topics[1]
				// 移除 0x 前缀
				userTopic = strings.TrimPrefix(userTopic, "0x")
				// 提取后40个字符（20字节 = 40个十六进制字符）
				if len(userTopic) >= 40 {
					address := "0x" + userTopic[len(userTopic)-40:]
					addressSet[strings.ToLower(address)] = true
				}
			}
		}

		// 移动到下一个批次
		currentFrom.Add(currentTo, big.NewInt(1))

		// 添加延迟，避免请求过快
		time.Sleep(200 * time.Millisecond)
	}

	// 转换为切片
	addresses := make([]string, 0, len(addressSet))
	for addr := range addressSet {
		addresses = append(addresses, addr)
	}

	logger.Info("从链上查询地址完成",
		"total_batches", batchCount,
		"unique_addresses", len(addresses))

	return addresses, nil
}

// queryUserFillsInRange 在指定区块范围内查询用户的交易记录
func (c *Client) queryUserFillsInRange(userAddress, contractAddress string, fromBlock, toBlock *big.Int, logger *slog.Logger) ([]ChainFill, error) {
	// 参数验证
	if fromBlock == nil || toBlock == nil {
		return nil, fmt.Errorf("区块号不能为空: fromBlock=%v, toBlock=%v", fromBlock, toBlock)
	}
	
	// 检查区块号是否有效（不能为负数）
	if fromBlock.Sign() < 0 || toBlock.Sign() < 0 {
		return nil, fmt.Errorf("区块号不能为负数: fromBlock=%s, toBlock=%s", fromBlock.String(), toBlock.String())
	}
	
	// 检查 fromBlock 是否大于 toBlock
	if fromBlock.Cmp(toBlock) > 0 {
		return nil, fmt.Errorf("起始区块号不能大于结束区块号: fromBlock=%s, toBlock=%s", fromBlock.String(), toBlock.String())
	}
	
	// 检查合约地址格式
	if contractAddress == "" {
		return nil, fmt.Errorf("合约地址不能为空")
	}
	
	// 检查用户地址格式
	if userAddress == "" {
		return nil, fmt.Errorf("用户地址不能为空")
	}
	
	// 构建查询参数
	fillEventSig := GetFillEventSignature()
	
	// 确保区块号格式正确（十六进制，小写）
	fromBlockHex := "0x" + fromBlock.Text(16)
	toBlockHex := "0x" + toBlock.Text(16)
	
	// 如果区块号为 0，使用 "0x0" 而不是 "0x"
	if fromBlock.Sign() == 0 {
		fromBlockHex = "0x0"
	}
	if toBlock.Sign() == 0 {
		toBlockHex = "0x0"
	}
	
	req := GetLogsRequest{
		FromBlock: fromBlockHex,
		ToBlock:   toBlockHex,
		Address:   contractAddress,
		Topics: []string{
			fillEventSig,                    // 事件签名（第一个主题）
			AddressToTopic(userAddress),     // 用户地址（第二个主题，如果事件中 user 是 indexed）
		},
	}
	
	logger.Debug("查询链上交易记录",
		"user_address", userAddress,
		"contract_address", contractAddress,
		"from_block", fromBlockHex,
		"to_block", toBlockHex,
		"block_range", new(big.Int).Sub(toBlock, fromBlock).String())

	// 查询事件日志
	logs, err := c.GetLogs(req)
	if err != nil {
		return nil, err
	}

	// 3. 解析事件日志
	var fills []ChainFill
	for _, log := range logs {
		// 验证事件签名
		if len(log.Topics) < 2 {
			logger.Debug("事件日志 Topics 数量不足", "topics_count", len(log.Topics), "address", userAddress)
			continue
		}
		
		// 验证事件签名是否匹配
		expectedSig := GetFillEventSignature()
		if log.Topics[0] != expectedSig {
			logger.Debug("事件签名不匹配", 
				"expected", expectedSig, 
				"actual", log.Topics[0],
				"address", userAddress)
			continue
		}
		
		// 验证用户地址是否匹配（Topics[1] 应该是用户地址）
		userTopic := AddressToTopic(userAddress)
		if log.Topics[1] != userTopic {
			logger.Debug("用户地址不匹配", 
				"expected", userTopic,
				"actual", log.Topics[1],
				"address", userAddress)
			continue
		}
		
		// 解析 Data 字段
		if log.Data == "" {
			logger.Debug("事件日志 Data 为空", "address", userAddress)
			continue
		}
		
		eventData, err := DecodeFillEvent(log.Data)
		if err != nil {
			logger.Debug("解析事件数据失败", 
				"error", err, 
				"address", userAddress,
				"data", log.Data[:min(100, len(log.Data))])
			continue
		}
		
		// 转换为 ChainFill
		fill := ChainFill{
			Time:      eventData.Time,
			ClosedPnl: eventData.ClosedPnl,
			Fee:       eventData.Fee,
		}
		
		fills = append(fills, fill)
	}

	logger.Debug("从链上查询到交易记录", "address", userAddress, "count", len(fills), "logs_count", len(logs))
	return fills, nil
}

// ChainFill 链上交易记录
type ChainFill struct {
	Time      int64   // 交易时间（毫秒）
	ClosedPnl float64 // 已实现盈亏
	Fee       float64 // 手续费
}
