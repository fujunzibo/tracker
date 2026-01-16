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
		// 在安全范围内提高速度
		// 每10秒50个请求 = 5请求/秒（在限制范围内，留有余量）
		// 如果遇到速率限制，会自动退避重试
		globalRPCRateLimiter = utils.NewRateLimiter(50) // 每10秒50个请求 = 5请求/秒
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
		baseDelay      = 5 * time.Second // 基础延迟时间：5秒（增加到5秒，避免 rate limited）
		maxDelay       = 60 * time.Second // 最大延迟时间：60秒（增加到60秒）
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
	// 增加超时时间，因为查询大量区块可能需要更长时间
	// 但也要注意，如果查询范围太大，即使增加超时也可能失败
	httpClient.Timeout = 60 * time.Second
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
		
		// 检查是否是速率限制错误（-32005 或 -32007）
		// -32005: rate limited (HyperEVM RPC)
		// -32007: rate limit (QuickNode)
		if rpcResp.Error.Code == -32005 || rpcResp.Error.Code == -32007 {
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
			
			errorCode := rpcResp.Error.Code
			errorName := "rate limit"
			if errorCode == -32005 {
				errorName = "rate limited (HyperEVM)"
			} else if errorCode == -32007 {
				errorName = "rate limit (QuickNode)"
			}
			
			slog.Warn(fmt.Sprintf("RPC 收到 %s 错误（%d），进行指数退避重试", errorName, errorCode),
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
	// 504 Gateway Timeout - 查询范围太大或服务器超时
	if resp.StatusCode == http.StatusGatewayTimeout || resp.StatusCode == 504 {
		// 504 错误，进行指数退避重试
		if retryCount >= maxRetries {
			return nil, fmt.Errorf("达到最大重试次数 (%d)，仍然收到 504 错误（Gateway Timeout）", maxRetries)
		}
		
		// 计算指数退避延迟时间
		delay := time.Duration(float64(baseDelay) * math.Pow(2, float64(retryCount)))
		if delay > maxDelay {
			delay = maxDelay
		}
		
		// 添加随机抖动
		jitter := time.Duration(float64(delay) * jitterPercent * (rand.Float64()*2 - 1))
		delay = delay + jitter
		if delay < 0 {
			delay = baseDelay
		}
		
		slog.Warn("RPC 收到 504 错误（Gateway Timeout），进行指数退避重试",
			"method", method,
			"retry_count", retryCount+1,
			"max_retries", maxRetries,
			"delay", delay,
			"tip", "查询范围可能太大，建议缩小区块范围")
		
		// 等待后重试
		time.Sleep(delay)
		return c.callWithRetry(method, params, retryCount+1)
	}
	
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
	// 构建参数，确保格式符合 Ethereum JSON-RPC 规范
	// eth_getLogs 的参数应该是一个对象
	paramsMap := make(map[string]interface{})
	
	// 确保区块号格式正确（必须是十六进制字符串）
	if req.FromBlock != "" {
		paramsMap["fromBlock"] = req.FromBlock
	}
	if req.ToBlock != "" {
		paramsMap["toBlock"] = req.ToBlock
	}
	
	// 确保地址格式正确（必须是有效的以太坊地址格式）
	if req.Address != "" {
		// 移除 0x 前缀，转换为小写
		address := strings.TrimPrefix(strings.ToLower(req.Address), "0x")
		// 确保地址是 40 个十六进制字符（20 字节）
		if len(address) != 40 {
			return nil, fmt.Errorf("地址格式错误：必须是 40 个十六进制字符（20 字节），实际长度: %d, 地址: %s", len(address), req.Address)
		}
		// 验证是否为有效的十六进制字符
		for _, c := range address {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
				return nil, fmt.Errorf("地址格式错误：包含无效的十六进制字符，地址: %s", req.Address)
			}
		}
		paramsMap["address"] = "0x" + address
	}
	
	// Topics 格式：根据 Ethereum JSON-RPC 规范
	// topics 是一个数组，每个元素可以是：
	// - null（匹配任何值）
	// - 字符串（匹配该值）
	// - 字符串数组（匹配数组中的任何值）
	// 重要：只传入非空的 topic，不要传入空字符串或 null
	// 对于查询所有 Fill 事件，只传入 Topic 0（事件签名），不传入 Topic 1（用户地址）
	if len(req.Topics) > 0 {
		topicsArray := make([]interface{}, 0)
		for _, topic := range req.Topics {
			// 跳过空字符串
			if topic == "" {
				continue
			}
			// 确保 topic 格式正确
			topicStr := strings.ToLower(strings.TrimSpace(topic))
			if !strings.HasPrefix(topicStr, "0x") {
				topicStr = "0x" + topicStr
			}
			// 移除 0x 前缀，检查长度
			topicHex := strings.TrimPrefix(topicStr, "0x")
			// 确保 topic 是 64 个十六进制字符（32 字节）
			if len(topicHex) < 64 {
				// 左补 0 到 64 个字符
				topicHex = strings.Repeat("0", 64-len(topicHex)) + topicHex
			} else if len(topicHex) > 64 {
				// 如果超过 64 个字符，截取前 64 个
				topicHex = topicHex[:64]
			}
			topicsArray = append(topicsArray, "0x"+topicHex)
		}
		// 只有当 topicsArray 不为空时才添加到 paramsMap
		if len(topicsArray) > 0 {
			paramsMap["topics"] = topicsArray
		}
	}
	
	// 构建完整的 RPC 请求用于调试
	debugReq := RPCRequest{
		JSONRPC: "2.0",
		Method:  "eth_getLogs",
		Params:  []interface{}{paramsMap},
		ID:      1,
	}
	debugData, _ := json.MarshalIndent(debugReq, "", "  ")
	slog.Info("eth_getLogs 请求详情",
		"request_json", string(debugData),
		"from_block", req.FromBlock,
		"to_block", req.ToBlock,
		"address", paramsMap["address"],
		"topics", paramsMap["topics"],
		"topics_count", len(req.Topics))
	
	// 使用 map 格式的参数（标准格式）
	params := []interface{}{paramsMap}
	result, err := c.call("eth_getLogs", params)
	if err != nil {
		// 如果失败，记录详细的错误信息
		slog.Warn("eth_getLogs 调用失败",
			"error", err,
			"from_block", req.FromBlock,
			"to_block", req.ToBlock,
			"address", req.Address,
			"topics", req.Topics)
		
		// 如果失败，尝试不同的格式
		if strings.Contains(err.Error(), "Invalid params") {
			// 尝试1：使用结构体格式（向后兼容）
			params2 := []interface{}{req}
			result2, err2 := c.call("eth_getLogs", params2)
			if err2 == nil {
				var logs []Log
				if err := json.Unmarshal(result2, &logs); err != nil {
					return nil, fmt.Errorf("unmarshal logs: %w", err)
				}
				return logs, nil
			}
			
			// 尝试2：如果 Topics 只有一个元素，尝试二维数组格式
			if len(req.Topics) == 1 {
				paramsMap3 := make(map[string]interface{})
				if req.FromBlock != "" {
					paramsMap3["fromBlock"] = req.FromBlock
				}
				if req.ToBlock != "" {
					paramsMap3["toBlock"] = req.ToBlock
				}
				if req.Address != "" {
					paramsMap3["address"] = strings.ToLower(req.Address)
				}
				// 尝试将单个 topic 包装成二维数组：[[topic]]
				topicStr := strings.ToLower(req.Topics[0])
				if !strings.HasPrefix(topicStr, "0x") {
					topicStr = "0x" + topicStr
				}
				paramsMap3["topics"] = []interface{}{[]interface{}{topicStr}}
				params3 := []interface{}{paramsMap3}
				result3, err3 := c.call("eth_getLogs", params3)
				if err3 == nil {
					var logs []Log
					if err := json.Unmarshal(result3, &logs); err != nil {
						return nil, fmt.Errorf("unmarshal logs: %w", err)
					}
					return logs, nil
				}
			}
		}
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
	// 默认地址：0x3333333333333333333333333333333333333333
	HyperliquidClearinghouseAddress = "0x3333333333333333333333333333333333333333"
	
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
	// Hyperliquid RPC 硬性要求：每次查询最多 50 个区块（to_block - from_block ≤ 50），需要分批查询
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

	// Hyperliquid RPC 硬性要求：每次查询最多 50 个区块（to_block - from_block ≤ 50）
	const maxBlockRange = 50

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

	// 如果查询范围小于等于 50 个区块，直接查询
	blockRange := new(big.Int).Sub(endBlock, startBlock)
	if blockRange.Cmp(big.NewInt(maxBlockRange)) <= 0 {
		return c.queryUserFillsInRange(userAddress, contractAddress, startBlock, endBlock, logger)
	}

	// 分批查询：每次查询 50 个区块（Hyperliquid RPC 硬性要求）
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
		// 计算当前批次的结束区块（最多 50 个区块，Hyperliquid RPC 硬性要求）
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

// AddressCallback 地址回调函数类型
// 当找到新地址时，会调用此函数
// addresses: 当前已找到的所有地址
// newCount: 本次新增的地址数量
type AddressCallback func(addresses []string, newCount int)

// QueryAllAddressesFromChain 从链上查询所有唯一的用户地址
// 通过查询所有 Fill 事件，提取唯一的用户地址
// fromBlock: 起始区块号，如果为 nil 则从 0 开始
// toBlock: 结束区块号，如果为 nil 则查询到最新区块
// callback: 可选的回调函数，每找到一定数量的地址或每过一定时间就调用一次
func (c *Client) QueryAllAddressesFromChain(fromBlock, toBlock *big.Int, logger *slog.Logger, callback AddressCallback) ([]string, error) {
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
		startBlock = big.NewInt(1) // QuickNode 可能不支持区块 0，从区块 1 开始
	} else if startBlock.Sign() == 0 {
		startBlock = big.NewInt(1) // 如果指定了区块 0，改为从区块 1 开始
		logger.Info("QuickNode 不支持区块 0，从区块 1 开始查询")
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

	// QuickNode 免费计划限制：每次查询最多 4 个区块（to_block - from_block ≤ 4）
	// 注意：虽然文档说 5 个区块，但实际测试发现 4 个区块更稳定
	const maxBlockRange = 4

	// 使用 map 存储唯一的地址
	addressSet := make(map[string]bool)
	
	// 获取 Fill 事件签名
	fillEventSig := GetFillEventSignature()
	
	logger.Info("开始从链上查询所有地址",
		"from_block", startBlock.String(),
		"to_block", endBlock.String(),
		"contract_address", contractAddress,
		"event_signature", fillEventSig,
		"max_block_range", maxBlockRange,
		"note", "QuickNode 免费计划限制：每次查询最多 4 个区块")

	// 分批查询：从最新区块开始往前查询（每次查询 4 个区块，QuickNode 免费计划限制）
	// 这样可以先获取最新的地址，更快看到结果
	currentTo := new(big.Int).Set(endBlock)
	batchCount := 0
	
	// 用于定时保存：每300秒保存一次已找到的地址（即使没有新地址）
	lastSaveTime := time.Now()
	const saveInterval = 300 * time.Second
	
	for currentTo.Cmp(startBlock) >= 0 {
		// 计算当前批次的起始区块（最多 4 个区块，QuickNode 免费计划限制）
		currentFrom := new(big.Int).Sub(currentTo, big.NewInt(maxBlockRange-1))
		if currentFrom.Cmp(startBlock) < 0 {
			currentFrom.Set(startBlock)
		}
		
		// 确保 currentTo >= currentFrom
		if currentTo.Cmp(currentFrom) < 0 {
			break
		}
		
		// 确保至少查询 1 个区块
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

		// 注意：不指定 Topics，返回所有事件
		// 这样可以确保即使某些区块没有 Fill 事件，也能从其他事件中提取地址
		// 与 HyperEVM RPC 保持一致
		req := GetLogsRequest{
			FromBlock: fromBlockHex,
			ToBlock:   toBlockHex,
			Address:   contractAddress,
			// Topics 留空，返回所有事件（与 HyperEVM RPC 保持一致）
			Topics: []string{},
		}

		logs, err := c.GetLogs(req)
		if err != nil {
			// 如果是范围限制错误或 invalid block range，尝试缩小范围
			if strings.Contains(err.Error(), "limited to a 5 range") || 
			   strings.Contains(err.Error(), "limited to a 4 range") ||
			   strings.Contains(err.Error(), "invalid block range") ||
			   strings.Contains(err.Error(), "Invalid params") {
				logger.Debug("批次查询超出限制，尝试缩小范围",
					"batch", batchCount,
					"from_block", currentFrom.String(),
					"to_block", currentTo.String(),
					"error", err)
				// 尝试查询更小的范围（逐步缩小：3 -> 2 -> 1）
				blockDiff := new(big.Int).Sub(currentTo, currentFrom).Int64()
				if blockDiff >= 3 {
					// 尝试 3 个区块
					currentTo = new(big.Int).Add(currentFrom, big.NewInt(2))
				} else if blockDiff >= 2 {
					// 尝试 2 个区块
					currentTo = new(big.Int).Add(currentFrom, big.NewInt(1))
				} else {
					// 尝试 1 个区块（from 和 to 相同）
					currentTo = new(big.Int).Set(currentFrom)
				}
				if currentTo.Cmp(endBlock) > 0 {
					currentTo.Set(endBlock)
				}
				// 确保至少查询 1 个区块（from 和 to 可以相同）
				if currentTo.Cmp(currentFrom) < 0 {
					currentTo.Set(currentFrom)
				}
				toBlockHex = "0x" + currentTo.Text(16)
				if currentTo.Sign() == 0 {
					toBlockHex = "0x0"
				}
				req.ToBlock = toBlockHex
				logs, err = c.GetLogs(req)
				// 如果仍然失败，跳过这个批次，继续往前查询
				if err != nil && strings.Contains(err.Error(), "invalid block range") {
					logger.Debug("跳过无效的区块范围，继续往前查询",
						"from_block", currentFrom.String(),
						"to_block", currentTo.String())
					currentTo.Sub(currentFrom, big.NewInt(1))
					if currentTo.Cmp(startBlock) < 0 {
						break
					}
					continue
				}
			}
			if err != nil {
				logger.Warn("批次查询失败",
					"batch", batchCount,
					"error", err,
					"from_block", currentFrom.String(),
					"to_block", currentTo.String())
				// 继续下一个批次（往前查询），不中断整个查询
				currentTo.Sub(currentFrom, big.NewInt(1))
				if currentTo.Cmp(startBlock) < 0 {
					break
				}
				continue
			}
		}

		// 记录返回的日志数量
		logger.Info("eth_getLogs 返回结果",
			"batch", batchCount,
			"logs_count", len(logs),
			"from_block", currentFrom.String(),
			"to_block", currentTo.String())

		// 调试：打印前几个日志的 Topics[0]，看看实际返回了什么事件
		if len(logs) > 0 {
			maxDebugLogs := 5
			if len(logs) < maxDebugLogs {
				maxDebugLogs = len(logs)
			}
			for i := 0; i < maxDebugLogs; i++ {
				if len(logs[i].Topics) > 0 {
					logger.Info("调试：日志事件签名",
						"batch", batchCount,
						"log_index", i,
						"topic0", logs[i].Topics[0],
						"topics_count", len(logs[i].Topics),
						"expected_fill_sig", fillEventSig)
				} else {
					logger.Info("调试：日志无 Topics",
						"batch", batchCount,
						"log_index", i)
				}
			}
		} else {
			logger.Debug("批次无日志返回",
				"batch", batchCount,
				"from_block", currentFrom.String(),
				"to_block", currentTo.String(),
				"contract_address", contractAddress)
		}

		// 从事件日志中提取用户地址和  地址
		// 注意：与 HyperEVM RPC 保持一致，不验证事件签名，直接从所有日志中提取地址
		// 这样可以确保即使某些区块没有 Fill 事件，也能从其他事件中提取地址
		batchAddressCount := 0
		fillEventCount := 0
		for _, log := range logs {
			// 提取 Topics[1] 中的地址（通常是用户地址）
			if len(log.Topics) >= 2 {
				// Topics[1] 是用户地址（32字节格式，需要提取后20字节）
				userTopic := log.Topics[1]
				// 移除 0x 前缀
				userTopic = strings.TrimPrefix(userTopic, "0x")
				// 提取后40个字符（20字节 = 40个十六进制字符）
				if len(userTopic) >= 40 {
					address := "0x" + userTopic[len(userTopic)-40:]
					addressLower := strings.ToLower(address)
					if !addressSet[addressLower] {
						addressSet[addressLower] = true
						batchAddressCount++
						logger.Info("提取到新地址（Topics[1]）",
							"batch", batchCount,
							"address", address,
							"topic", log.Topics[1],
							"event_sig", log.Topics[0])
					}
				}
			}
			
			// 对 Fill 事件的 Data 字段进行 ABI 解码，提取  地址
			// 尝试处理所有有 Data 字段的日志，不仅限于 Fill 事件签名匹配的
			// 因为可能事件签名不匹配，但数据格式相同
			if len(log.Topics) > 0 && log.Data != "" && len(log.Data) > 0 {
				// 检查是否是 Fill 事件
				isFillEvent := log.Topics[0] == fillEventSig
				
				// 尝试解码 Data 字段（无论事件签名是否匹配）
				fillData, err := DecodeFillEvent(log.Data)
				if err == nil {
					fillEventCount++
					// 记录解码结果，用于调试
					if fillData.Counterparty != "" {
						// 成功解码，提取 地址
						counterpartyAddress := strings.ToLower(fillData.Counterparty)
						if !addressSet[counterpartyAddress] {
							addressSet[counterpartyAddress] = true
							batchAddressCount++
							
						}
					} else {
						// 不再记录未找到 地址的调试信息
					}
				} else if isFillEvent {
					// 只有 Fill 事件签名匹配时才记录解码失败（避免日志过多）
					fillEventCount++
					if fillEventCount <= 10 || fillEventCount%100 == 0 {
						dataPreview := log.Data
						if len(dataPreview) > 200 {
							dataPreview = dataPreview[:200] + "..."
						}
						logger.Info("Fill 事件 Data 解码失败",
							"batch", batchCount,
							"fill_event_index", fillEventCount,
							"error", err,
							"event_sig", log.Topics[0],
							"data_length", len(log.Data),
							"data_preview", dataPreview)
					}
				}
			}
		}
		
		// 记录本批次 Fill 事件的数量
		if fillEventCount > 0 {
			logger.Info("本批次 Fill 事件统计",
				"batch", batchCount,
				"fill_events", fillEventCount,
				"total_logs", len(logs))
		}

		// 记录本批次提取到的地址数量
		if batchAddressCount > 0 {
			logger.Info("批次提取地址完成",
				"batch", batchCount,
				"new_addresses", batchAddressCount,
				"total_unique_addresses", len(addressSet),
				"from_block", currentFrom.String(),
				"to_block", currentTo.String())
			
			// 如果提供了回调函数，且找到了新地址，调用回调函数
			if callback != nil && batchAddressCount > 0 {
				// 转换为切片
				currentAddresses := make([]string, 0, len(addressSet))
				for addr := range addressSet {
					currentAddresses = append(currentAddresses, addr)
				}
				callback(currentAddresses, batchAddressCount)
				lastSaveTime = time.Now() // 更新保存时间
			}
		}
		
		// 定时保存：即使没有找到新地址，也每300秒保存一次已找到的地址
		if callback != nil && len(addressSet) > 0 {
			if time.Since(lastSaveTime) >= saveInterval {
				// 转换为切片
				currentAddresses := make([]string, 0, len(addressSet))
				for addr := range addressSet {
					currentAddresses = append(currentAddresses, addr)
				}
				logger.Info("触发定时保存", 
					"batch", batchCount,
					"total_addresses", len(addressSet),
					"time_since_last_save", time.Since(lastSaveTime),
					"save_interval", saveInterval)
				callback(currentAddresses, 0) // newCount=0 表示定时保存，不是新找到的地址
				lastSaveTime = time.Now()
			}
		}

		// 移动到下一个批次（往前查询）
		currentTo.Sub(currentFrom, big.NewInt(1))
		if currentTo.Cmp(startBlock) < 0 {
			break
		}

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

// QueryAllAddressesFromHyperEVM 通过 HyperEVM JSON-RPC API 从链上查询所有唯一的用户地址
// 使用 Hyperliquid 官方的 RPC 端点（https://rpc.hyperliquid.xyz/evm）
// 这个方法与 QueryAllAddressesFromChain 功能相同，但使用不同的 RPC 端点
// fromBlock: 起始区块号，如果为 nil 则从 0 开始
// toBlock: 结束区块号，如果为 nil 则查询到最新区块
// 注意：HyperEVM RPC 可能没有区块范围限制，可以查询更大的区块范围
func (c *Client) QueryAllAddressesFromHyperEVM(fromBlock, toBlock *big.Int, logger *slog.Logger, callback AddressCallback) ([]string, error) {
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

	// Hyperliquid RPC 硬性要求：每次查询最多 50 个区块（to_block - from_block ≤ 50）
	const maxBlockRange = 50

	// 使用 map 存储唯一的地址
	addressSet := make(map[string]bool)
	
	// 用于定时保存：每300秒保存一次已找到的地址（即使没有新地址）
	lastSaveTime := time.Now()
	const saveInterval = 300 * time.Second
	
	// 构建查询参数（只查询 Fill 事件，不指定用户地址）
	fillEventSig := GetFillEventSignature()
	
	// 先测试 RPC 连接和基本功能
	logger.Info("开始通过 HyperEVM RPC 查询所有地址",
		"from_block", startBlock.String(),
		"to_block", endBlock.String(),
		"contract_address", contractAddress,
		"rpc_url", c.rpcURL,
		"event_signature", fillEventSig)
	
	// 先测试获取最新区块号，确保 RPC 连接正常
	testBlock, err := c.GetBlockNumber()
	if err != nil {
		logger.Warn("测试 HyperEVM RPC 连接失败（获取区块号）",
			"error", err,
			"rpc_url", c.rpcURL)
		return nil, fmt.Errorf("HyperEVM RPC 连接测试失败: %w", err)
	}
	logger.Info("HyperEVM RPC 连接测试成功",
		"latest_block", testBlock.String(),
		"rpc_url", c.rpcURL)

	// 分批查询：从最新区块开始往前查询（每次查询 maxBlockRange 个区块）
	// 这样可以先获取最新的地址，更快看到结果
	currentTo := new(big.Int).Set(endBlock)
	batchCount := 0
	
	for currentTo.Cmp(startBlock) >= 0 { // 从最新区块往前查询到起始区块
		// 计算当前批次的起始区块（最多 maxBlockRange 个区块）
		currentFrom := new(big.Int).Sub(currentTo, big.NewInt(maxBlockRange-1))
		if currentFrom.Cmp(startBlock) < 0 {
			currentFrom.Set(startBlock)
		}
		
		// 确保 currentTo >= currentFrom
		if currentTo.Cmp(currentFrom) < 0 {
			logger.Warn("批次区块范围无效，跳过",
				"batch", batchCount+1,
				"from_block", currentFrom.String(),
				"to_block", currentTo.String())
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
				// fillEventSig, // 只指定事件签名，不指定用户地址
			},
		}

		logs, err := c.GetLogs(req)
		if err != nil {
			// 记录详细的错误信息
			logger.Warn("GetLogs 调用失败",
				"batch", batchCount,
				"from_block", currentFrom.String(),
				"to_block", currentTo.String(),
				"contract_address", contractAddress,
				"event_sig", fillEventSig,
				"error", err)
			
			// 如果是 rate limited 错误，前3个批次都失败则快速回退到 QuickNode RPC
			if strings.Contains(err.Error(), "rate limited") ||
			   strings.Contains(err.Error(), "-32005") {
				logger.Warn("HyperEVM RPC 返回 rate limited 错误",
					"batch", batchCount,
					"error", err,
					"rpc_url", c.rpcURL,
					"tip", "HyperEVM RPC 速率限制较严格，将回退到 QuickNode RPC")
				// 对于 HyperEVM RPC，如果前3个批次都遇到 rate limited，返回已收集到的地址并返回错误
				// 让调用方知道 HyperEVM RPC 不可用，自动回退到 QuickNode RPC
				// 但不要丢弃已收集到的地址
				if batchCount <= 3 {
					// 返回已收集到的地址，而不是 nil
					addresses := make([]string, 0, len(addressSet))
					for addr := range addressSet {
						addresses = append(addresses, addr)
					}
					logger.Info("HyperEVM RPC 前3个批次遇到 rate limited，返回已收集的地址",
						"collected_addresses", len(addresses),
						"tip", "将回退到 QuickNode RPC 继续查询")
					return addresses, fmt.Errorf("HyperEVM RPC 速率限制: %w\n提示：程序将自动回退到 QuickNode RPC", err)
				}
				// 继续尝试，但增加延迟（往前查询）
				currentTo.Sub(currentFrom, big.NewInt(1))
				time.Sleep(5 * time.Second) // rate limited 时增加延迟到 5 秒，确保不会再次触发
				continue
			}
			
			// 如果是 "Invalid params" 或 "invalid block range" 错误，可能是 HyperEVM RPC 不支持某些参数格式或范围太大
			if strings.Contains(err.Error(), "Invalid params") ||
			   strings.Contains(err.Error(), "invalid block range") ||
			   strings.Contains(err.Error(), "block range") {
				logger.Warn("HyperEVM RPC 返回错误",
					"batch", batchCount,
					"error", err,
					"rpc_url", c.rpcURL,
					"tip", "HyperEVM RPC 可能不支持当前的查询方式或区块范围太大")
				// 对于 HyperEVM RPC，如果前3个批次都失败，返回已收集到的地址并返回错误
				// 让调用方知道 HyperEVM RPC 不可用，自动回退到 QuickNode RPC
				// 但不要丢弃已收集到的地址
				if batchCount <= 3 {
					// 前3个批次都失败，说明 HyperEVM RPC 不支持此查询方式
					// 返回已收集到的地址，而不是 nil
					addresses := make([]string, 0, len(addressSet))
					for addr := range addressSet {
						addresses = append(addresses, addr)
					}
					logger.Info("HyperEVM RPC 前3个批次遇到 Invalid params，返回已收集的地址",
						"collected_addresses", len(addresses),
						"tip", "将回退到 QuickNode RPC 继续查询")
					return addresses, fmt.Errorf("HyperEVM RPC 不支持当前的查询方式: %w\n提示：程序将自动回退到 QuickNode RPC", err)
				}
				// 继续尝试，但记录警告（往前查询）
				currentTo.Sub(currentFrom, big.NewInt(1))
				time.Sleep(200 * time.Millisecond)
				continue
			} else if strings.Contains(err.Error(), "limited to a") || 
			   strings.Contains(err.Error(), "query returned more than") ||
			   strings.Contains(err.Error(), "block range too large") {
				// 如果是范围限制错误，尝试缩小范围
				logger.Debug("批次查询超出限制，尝试缩小范围",
					"batch", batchCount,
					"from_block", currentFrom.String(),
					"to_block", currentTo.String(),
					"original_range", maxBlockRange)
				
				// 逐步缩小范围：先尝试 50 个区块（Hyperliquid RPC 最大限制）
				// 从当前 currentTo 往前计算 currentFrom
				smallerRange := int64(50)
				currentFrom = new(big.Int).Sub(currentTo, big.NewInt(smallerRange-1))
				if currentFrom.Cmp(startBlock) < 0 {
					currentFrom.Set(startBlock)
				}
				toBlockHex = "0x" + currentTo.Text(16)
				if currentTo.Sign() == 0 {
					toBlockHex = "0x0"
				}
				req.ToBlock = toBlockHex
				logs, err = c.GetLogs(req)
			}
			
			if err != nil {
				// 检查是否是 504 Gateway Timeout 错误
				if strings.Contains(err.Error(), "504") ||
				   strings.Contains(err.Error(), "Gateway Timeout") {
					logger.Debug("批次查询超时（504），尝试缩小范围",
						"batch", batchCount,
						"from_block", currentFrom.String(),
						"to_block", currentTo.String(),
						"original_range", maxBlockRange)
					
					// 超时说明范围太大，尝试缩小到 50 个区块（Hyperliquid RPC 最大限制）
					// 从当前 currentTo 往前计算 currentFrom
					smallerRange := int64(50)
					currentFrom = new(big.Int).Sub(currentTo, big.NewInt(smallerRange-1))
					if currentFrom.Cmp(startBlock) < 0 {
						currentFrom.Set(startBlock)
					}
					toBlockHex = "0x" + currentTo.Text(16)
					if currentTo.Sign() == 0 {
						toBlockHex = "0x0"
					}
					req.ToBlock = toBlockHex
					logs, err = c.GetLogs(req)
					
					// 如果还是超时，尝试更小的范围（25个区块）
					if err != nil && (strings.Contains(err.Error(), "504") ||
					   strings.Contains(err.Error(), "Gateway Timeout")) {
						logger.Debug("50个区块仍然超时，尝试25个区块")
						smallerRange = 25
						currentFrom = new(big.Int).Sub(currentTo, big.NewInt(smallerRange-1))
						if currentFrom.Cmp(startBlock) < 0 {
							currentFrom.Set(startBlock)
						}
						toBlockHex = "0x" + currentTo.Text(16)
						if currentTo.Sign() == 0 {
							toBlockHex = "0x0"
						}
						req.ToBlock = toBlockHex
						logs, err = c.GetLogs(req)
					}
				}
				
				// 检查是否是 rate limited 错误
				if err != nil && (strings.Contains(err.Error(), "rate limited") ||
				   strings.Contains(err.Error(), "-32005")) {
					logger.Debug("批次查询被限流，增加延迟",
						"batch", batchCount,
						"from_block", currentFrom.String(),
						"to_block", currentTo.String())
					// 增加延迟，避免请求过快
					time.Sleep(5 * time.Second) // rate limited 时增加延迟到 5 秒，确保不会再次触发
					// 继续下一个批次（往前查询）
					currentTo.Sub(currentFrom, big.NewInt(1))
					continue
				}
				
				if err != nil {
					logger.Warn("批次查询失败",
						"batch", batchCount,
						"error", err,
						"from_block", currentFrom.String(),
						"to_block", currentTo.String(),
						"rpc_url", c.rpcURL)
					// 继续下一个批次，不中断整个查询（往前查询）
					currentTo.Sub(currentFrom, big.NewInt(1))
					time.Sleep(200 * time.Millisecond) // 失败也延迟
					continue
				}
			}
		}
		
		// 记录返回的日志数量
		logger.Info("eth_getLogs 返回结果",
			"batch", batchCount,
			"logs_count", len(logs),
			"from_block", currentFrom.String(),
			"to_block", currentTo.String())

		// 从事件日志中提取用户地址和 地址
		// Fill 事件的 Topics[1] 是用户地址（indexed）
		batchAddressCount := 0
		fillEventCount := 0
		for _, log := range logs {
			// 打印每个日志的 Topic[0]（事件签名）
			if len(log.Topics) > 0 {
				logger.Debug("日志 Topic[0]",
					"batch", batchCount,
					"topic0", log.Topics[0],
					"topics_count", len(log.Topics))
			}
			
			// 提取 Topics[1] 中的地址（通常是用户地址）
			if len(log.Topics) >= 2 {
				// Topics[1] 是用户地址（32字节格式，需要提取后20字节）
				userTopic := log.Topics[1]
				// 移除 0x 前缀
				userTopic = strings.TrimPrefix(userTopic, "0x")
				// 提取后40个字符（20字节 = 40个十六进制字符）
				if len(userTopic) >= 40 {
					address := "0x" + userTopic[len(userTopic)-40:]
					addressLower := strings.ToLower(address)
					if !addressSet[addressLower] {
						addressSet[addressLower] = true
						batchAddressCount++
						logger.Debug("提取到新地址（Topics[1]）",
							"batch", batchCount,
							"address", address,
							"topic", log.Topics[1])
					}
				}
			}
			
			// 对 Fill 事件的 Data 字段进行 ABI 解码，提取  地址
			// 尝试处理所有有 Data 字段的日志，不仅限于 Fill 事件签名匹配的
			// 因为可能事件签名不匹配，但数据格式相同
			if len(log.Topics) > 0 && log.Data != "" && len(log.Data) > 0 {
				// 检查是否是 Fill 事件
				isFillEvent := log.Topics[0] == fillEventSig
				
				// 尝试解码 Data 字段（无论事件签名是否匹配）
				fillData, err := DecodeFillEvent(log.Data)
				if err == nil {
					fillEventCount++
					// 记录解码结果，用于调试
					if fillData.Counterparty != "" {
						// 成功解码，提取 地址
						counterpartyAddress := strings.ToLower(fillData.Counterparty)
						if !addressSet[counterpartyAddress] {
							addressSet[counterpartyAddress] = true
							batchAddressCount++
						}
					} else {
						// 不再记录未找到 地址的调试信息
					}
				} else if isFillEvent {
					// 只有 Fill 事件签名匹配时才记录解码失败（避免日志过多）
					fillEventCount++
					if fillEventCount <= 10 || fillEventCount%100 == 0 {
						dataPreview := log.Data
						if len(dataPreview) > 200 {
							dataPreview = dataPreview[:200] + "..."
						}
						logger.Info("Fill 事件 Data 解码失败",
							"batch", batchCount,
							"fill_event_index", fillEventCount,
							"error", err,
							"event_sig", log.Topics[0],
							"data_length", len(log.Data),
							"data_preview", dataPreview)
					}
				}
			}
		}
		
		// 记录本批次 Fill 事件的数量
		if fillEventCount > 0 {
			logger.Info("本批次 Fill 事件统计",
				"batch", batchCount,
				"fill_events", fillEventCount,
				"total_logs", len(logs))
		}

		// 记录本批次提取到的地址数量
		if batchAddressCount > 0 {
			logger.Info("批次提取地址完成",
				"batch", batchCount,
				"new_addresses", batchAddressCount,
				"total_unique_addresses", len(addressSet),
				"from_block", currentFrom.String(),
				"to_block", currentTo.String())
			
			// 如果提供了回调函数，且找到了新地址，调用回调函数
			if callback != nil && batchAddressCount > 0 {
				// 转换为切片
				currentAddresses := make([]string, 0, len(addressSet))
				for addr := range addressSet {
					currentAddresses = append(currentAddresses, addr)
				}
				callback(currentAddresses, batchAddressCount)
				lastSaveTime = time.Now() // 更新保存时间
			}
		}
		
		// 定时保存：即使没有找到新地址，也每300秒保存一次已找到的地址
		if callback != nil && len(addressSet) > 0 {
			if time.Since(lastSaveTime) >= saveInterval {
				// 转换为切片
				currentAddresses := make([]string, 0, len(addressSet))
				for addr := range addressSet {
					currentAddresses = append(currentAddresses, addr)
				}
				logger.Info("触发定时保存", 
					"batch", batchCount,
					"total_addresses", len(addressSet),
					"time_since_last_save", time.Since(lastSaveTime),
					"save_interval", saveInterval)
				callback(currentAddresses, 0) // newCount=0 表示定时保存，不是新找到的地址
				lastSaveTime = time.Now()
			}
		}

		// 移动到下一个批次（往前查询）
		currentTo.Sub(currentFrom, big.NewInt(1))

		// 添加延迟，避免请求过快（HyperEVM RPC 有严格的速率限制，增加延迟以避免 rate limited 错误）
		time.Sleep(2 * time.Second) // 增加到 2 秒，确保不会触发 rate limited
	}

	// 转换为切片
	addresses := make([]string, 0, len(addressSet))
	for addr := range addressSet {
		addresses = append(addresses, addr)
	}

	logger.Info("通过 HyperEVM RPC 查询地址完成",
		"total_batches", batchCount,
		"unique_addresses", len(addresses),
		"rpc_url", c.rpcURL)

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
			// fillEventSig,                    // 事件签名（第一个主题）
			// AddressToTopic(userAddress),     // 用户地址（第二个主题，如果事件中 user 是 indexed）
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
		// 打印每个日志的 Topic[0]（事件签名）
		if len(log.Topics) > 0 {
			logger.Debug("日志 Topic[0]",
				"topic0", log.Topics[0],
				"topics_count", len(log.Topics),
				"address", userAddress)
		}
		
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
