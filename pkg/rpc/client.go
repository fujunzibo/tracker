package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hyper-tracker/pkg/utils"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"
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

// call 执行 RPC 调用
func (c *Client) call(method string, params []interface{}) (json.RawMessage, error) {
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

	var rpcResp RPCResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("rpc error [%d]: %s", rpcResp.Error.Code, rpcResp.Error.Message)
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
		return nil, err
	}

	var blockNumberHex string
	if err := json.Unmarshal(result, &blockNumberHex); err != nil {
		return nil, fmt.Errorf("unmarshal block number: %w", err)
	}

	blockNumber, err := hexToBigInt(blockNumberHex)
	if err != nil {
		return nil, fmt.Errorf("parse block number: %w", err)
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

// Hyperliquid 合约地址（需要根据实际情况填入）
// 这些是占位符，需要替换为实际的合约地址
var (
	// HyperliquidClearinghouseAddress Hyperliquid 清算所合约地址
	// TODO: 需要填入实际的合约地址
	HyperliquidClearinghouseAddress = ""
	
	// HyperliquidExchangeAddress Hyperliquid 交易所合约地址
	// TODO: 需要填入实际的合约地址
	HyperliquidExchangeAddress = ""
)

// SetHyperliquidContractAddress 设置 Hyperliquid 合约地址
func SetHyperliquidContractAddress(clearinghouse, exchange string) {
	HyperliquidClearinghouseAddress = clearinghouse
	HyperliquidExchangeAddress = exchange
}

// QueryUserFillsFromChain 从链上查询用户的交易记录
// 注意：需要先设置合约地址和事件签名
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

	// 1. 构建查询参数
	// 使用 Fill 事件签名（需要根据实际合约调整）
	fillEventSig := GetFillEventSignature()
	
	req := GetLogsRequest{
		FromBlock: "0x0",
		ToBlock:   "latest",
		Address:   contractAddress,
		Topics: []string{
			fillEventSig,                    // 事件签名（第一个主题）
			AddressToTopic(userAddress),     // 用户地址（第二个主题，如果事件中 user 是 indexed）
		},
	}

	if fromBlock != nil {
		req.FromBlock = "0x" + fromBlock.Text(16)
	}
	if toBlock != nil {
		req.ToBlock = "0x" + toBlock.Text(16)
	}

	// 2. 查询事件日志
	logs, err := c.GetLogs(req)
	if err != nil {
		logger.Debug("查询链上事件日志失败", "error", err, "address", userAddress)
		return nil, err
	}

	// 3. 解析事件日志
	var fills []ChainFill
	for _, log := range logs {
		// 解析事件数据
		// 注意：这需要根据 Hyperliquid 的实际事件结构来解析
		// 当前是示例实现，需要根据实际事件定义调整
		
		// 假设事件结构：Fill(address indexed user, string coin, uint256 price, uint256 size, int256 closedPnl, uint256 fee, uint256 time)
		// Topics[0] = 事件签名
		// Topics[1] = user 地址
		// Data = coin, price, size, closedPnl, fee, time
		
		if len(log.Topics) < 2 {
			continue
		}
		
		// 解析 Data 字段（需要根据实际事件结构解码）
		// 这里只是示例，实际需要根据 ABI 解码
		fill := ChainFill{
			// TODO: 从 log.Data 解析 closedPnl, fee, time
			// 需要使用 ABI 解码工具
			ClosedPnl: 0, // 需要从 log.Data 解析
			Fee:       0, // 需要从 log.Data 解析
			Time:      0, // 需要从 log.Data 解析
		}
		
		// 暂时跳过，等待实现完整的解析逻辑
		_ = fill
		_ = log
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
