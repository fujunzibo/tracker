package hyper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hyper-tracker/pkg/utils"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const DefaultRpcURL = "https://api.hyperliquid.xyz"

var (
	// 全局 Hyperliquid API 速率限制器（所有 API 客户端共享）
	// 为了安全，设置为 12 请求/秒（留有余量）
	globalHyperRateLimiter *utils.RateLimiter
	hyperRateLimiterOnce   sync.Once
)

type H = map[string]any

type Client struct {
	rpcURL string
	client *utils.HttpClient
}

// getGlobalHyperRateLimiter 获取全局 Hyperliquid API 速率限制器
func getGlobalHyperRateLimiter() *utils.RateLimiter {
	hyperRateLimiterOnce.Do(func() {
		// 设置为 12 请求/秒（每10秒120个请求）
		globalHyperRateLimiter = utils.NewRateLimiter(120)
	})
	return globalHyperRateLimiter
}

func NewClient(rpcURL string, addrs []string, proxies []string) *Client {
	client := utils.NewHttpClient(utils.WithLocalAddrPoolAndProxies(addrs, proxies), utils.WithTimeout(10*time.Second))
	return &Client{
		rpcURL: rpcURL,
		client: client,
	}
}

func DefaultClient() *Client {
	return NewClient(DefaultRpcURL, nil, nil)
}

func WithLocalAddrsAndProxies(addrs []string, proxies []string) *Client {
	return NewClient(DefaultRpcURL, addrs, proxies)
}

// postJson 发送 POST JSON 请求，支持指数退避重试
func (c *Client) postJson(fullUrl string, params H) ([]byte, error) {
	return c.postJsonWithRetry(fullUrl, params, 0)
}

// postJsonWithRetry 带指数退避重试的 POST JSON 请求
func (c *Client) postJsonWithRetry(fullUrl string, params H, retryCount int) ([]byte, error) {
	const (
		maxRetries     = 2              // 最大重试次数（总共尝试3次：初始1次 + 重试2次）
		baseDelay      = time.Second    // 基础延迟时间：1秒
		maxDelay       = 10 * time.Second      // 最大延迟时间：10秒
		jitterPercent  = 0.1            // 随机抖动百分比（±10%）
	)

	// 等待全局速率限制（仅在首次请求时等待，重试时不等待，避免重复限制）
	if retryCount == 0 {
		rateLimiter := getGlobalHyperRateLimiter()
		ctx := context.Background()
		if err := rateLimiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("速率限制等待失败: %w", err)
		}
	}

	data, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, fullUrl, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	client := c.client.Client()
	client.Timeout = time.Minute * 5
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()
	
	// 检查是否是 429 Too Many Requests 错误
	if resp.StatusCode == http.StatusTooManyRequests {
		if retryCount >= maxRetries {
			return nil, fmt.Errorf("达到最大重试次数 (%d)，仍然收到 429 错误", maxRetries)
		}
		
		// 计算指数退避延迟时间
		delay := time.Duration(float64(baseDelay) * math.Pow(2, float64(retryCount)))
		if delay > maxDelay {
			delay = maxDelay
		}
		
		// 添加随机抖动，避免多个客户端同时重试
		jitter := time.Duration(float64(delay) * jitterPercent * (rand.Float64()*2 - 1)) // -10% 到 +10%
		delay = delay + jitter
		if delay < 0 {
			delay = baseDelay
		}
		
		slog.Warn("收到 429 错误，进行指数退避重试",
			"url", fullUrl,
			"retry_count", retryCount+1,
			"max_retries", maxRetries,
			"delay", delay)
		
		// 等待后重试
		time.Sleep(delay)
		return c.postJsonWithRetry(fullUrl, params, retryCount+1)
	}
	
	// 检查其他 HTTP 错误状态码
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return body, nil
}

type Value []byte

func (v *Value) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if len(data) >= 2 && data[0] == '"' && data[len(data)-1] == '"' {
		*v = Value(data[1 : len(data)-1])
		return nil
	}
	*v = Value(data)
	return nil
}

func (v *Value) Int64() (int64, error) {
	return strconv.ParseInt(string(*v), 10, 64)
}

func (v *Value) Float64() (float64, error) {
	return strconv.ParseFloat(string(*v), 64)
}

type Point struct {
	Time  int64 `json:"time"`
	Value Value `json:"value"`
}

func (p *Point) UnmarshalJSON(data []byte) error {
	var values [2]Value
	err := json.Unmarshal(data, &values)
	if err != nil {
		return fmt.Errorf("point values unmarshal: %w", err)
	}
	p.Time, err = values[0].Int64()
	if err != nil {
		return fmt.Errorf("point time unmarshal: %w", err)
	}
	p.Value = values[1]
	return nil
}

type Series struct {
	AccountValueHistory []Point `json:"accountValueHistory"` // 账户价值历史
	PnlHistory          []Point `json:"pnlHistory"`          // 盈亏历史
	Vlm                 Value   `json:"vlm"`                 // 交易量
}

type PortfolioItem struct {
	Type   string `json:"type"`
	Series Series `json:"series"`
}

func printData(prefix string, data []byte) {
	if len(data) > 120 {
		slog.Debug(prefix + string(data[:60]) + "..." + string(data[len(data)-60:]))
	} else {
		slog.Debug(prefix + string(data))
	}
}

func (p *PortfolioItem) UnmarshalJSON(data []byte) error {
	var values [2]Value
	err := json.Unmarshal(data, &values)
	if err != nil {
		return fmt.Errorf("portfolio item values unmarshal: %w", err)
	}
	var series Series
	err = json.Unmarshal(values[1], &series)
	if err != nil {
		return fmt.Errorf("portfolio series unmarshal: %w", err)
	}
	p.Type = string(values[0])
	p.Series = series
	return nil
}

var ErrNull = errors.New("null")

func (c *Client) GetPortfolio(user string) ([]PortfolioItem, error) {
	fullUrl := c.rpcURL + "/info"
	params := H{
		"type": "portfolio",
		"user": user,
	}
	body, err := c.postJson(fullUrl, params)
	if err != nil {
		return nil, err
	}
	if len(body) == 4 && string(body) == "null" {
		return nil, ErrNull
	}
	var Portfolio []PortfolioItem
	err = json.Unmarshal(body, &Portfolio)
	if err != nil {
		printData("GetPortfolio "+user+": ", body)
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	if len(Portfolio) == 0 {
		printData("GetPortfolio "+user+": ", body)
	}
	return Portfolio, nil
}

func (c *Client) GetUserRole(user string) (string, error) {
	fullUrl := c.rpcURL + "/info"
	params := H{
		"type": "userRole",
		"user": user,
	}
	body, err := c.postJson(fullUrl, params)
	if err != nil {
		return "", err
	}
	if len(body) == 4 && string(body) == "null" {
		return "", ErrNull
	}
	var ret struct {
		Role string `json:"role"`
	}
	err = json.Unmarshal(body, &ret)
	if err != nil {
		printData("GetUserRole "+user+": ", body)
		return "", fmt.Errorf("json unmarshal: %w", err)
	}
	if ret.Role == "" {
		printData("GetUserRole "+user+": ", body)
	}
	return ret.Role, nil
}

type Vault struct {
	Summary struct {
		VaultAddress string `json:"vaultAddress"`
		Leader       string `json:"leader"`
		IsClosed     bool   `json:"isClosed"`
	} `json:"summary"`
}

func GetAvailableValuts() ([]string, error) {
	dataUrl := `https://stats-data.hyperliquid.xyz/Mainnet/vaults`
	resp, err := http.Get(dataUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var vaults []Vault
	err = json.Unmarshal(body, &vaults)
	if err != nil {
		return nil, err
	}
	var ret []string
	for _, vault := range vaults {
		if vault.Summary.IsClosed {
			continue
		}
		ret = append(ret, vault.Summary.VaultAddress)
	}
	return ret, nil
}

type Liquidation struct {
	LiquidatedUser string `json:"liquidatedUser"`
	MarkPrice      Value  `json:"markPx"`
	Method         string `json:"method"`
}

type Fill struct {
	Coin          string       `json:"coin"`                  // 币种
	Price         Value        `json:"px"`                    // 价格
	Size          Value        `json:"sz"`                    // 仓位大小
	Side          string       `json:"side"`                  // 方向
	Time          int64        `json:"time"`                  // 时间
	StartPosition Value        `json:"startPosition"`         // 起始仓位
	Direction     string       `json:"dir"`                   // 方向
	ClosedPnl     Value        `json:"closedPnl"`             // 已实现盈亏
	Hash          string       `json:"hash"`                  // 哈希
	OrderID       int64        `json:"oid"`                   // 订单ID
	Crossed       bool         `json:"crossed"`               // 是否交叉
	Fee           Value        `json:"fee"`                   // 手续费
	TradeID       int64        `json:"tid"`                   // 交易ID
	FeeToken      string       `json:"feeToken"`              // 手续费币种
	TwapID        *string      `json:"twapId,omitempty"`      // 交易策略ID
	Liquidation   *Liquidation `json:"liquidation,omitempty"` // 强平清算信息
}

const (
	FillDirOpenLong   = "Open Long"
	FillDirOpenShort  = "Open Short"
	FillDirCloseLong  = "Close Long"
	FillDirCloseShort = "Close Short"
	FillDirShort2Long = "Short > Long"
	FillDirLong2Short = "Long > Short"
)

func (c *Client) GetUserFills(user string) ([]Fill, error) {
	fullUrl := c.rpcURL + "/info"
	params := H{
		"type": "userFills",
		"user": user,
	}
	body, err := c.postJson(fullUrl, params)
	if err != nil {
		return nil, err
	}
	if len(body) == 4 && string(body) == "null" {
		return nil, ErrNull
	}
	var fills []Fill
	err = json.Unmarshal(body, &fills)
	if err != nil {
		printData("GetUserFills "+user+": ", body)
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	return fills, nil
}

// AccountSummary represents the user's account summary
type AccountSummary struct {
	AssetPositions             []AssetPosition `json:"assetPositions"`
	CrossMaintenanceMarginUsed Value           `json:"crossMaintenanceMarginUsed"`
	CrossMarginSummary         MarginSummary   `json:"crossMarginSummary"`
	MarginSummary              MarginSummary   `json:"marginSummary"`
	Time                       int64           `json:"time"`
	Withdrawable               Value           `json:"withdrawable"`
}

type AssetPosition struct {
	Position Position `json:"position"`
	Type     string   `json:"type"`
}

type Position struct {
	Coin           string     `json:"coin"`           // 币种
	CumFunding     CumFunding `json:"cumFunding"`     // 累计资金
	EntryPx        Value      `json:"entryPx"`        // 开仓价格
	Leverage       Leverage   `json:"leverage"`       // 杠杆
	LiquidationPx  Value      `json:"liquidationPx"`  // 清算价格
	MarginUsed     Value      `json:"marginUsed"`     // 保证金使用量
	MaxLeverage    int        `json:"maxLeverage"`    // 最大杠杆
	PositionValue  Value      `json:"positionValue"`  // 仓位价值
	ReturnOnEquity Value      `json:"returnOnEquity"` // 回报率
	Szi            Value      `json:"szi"`            // 仓位大小
	UnrealizedPnl  Value      `json:"unrealizedPnl"`  // 未实现盈亏
}

type CumFunding struct {
	AllTime     Value `json:"allTime"`     // 累计资金
	SinceChange Value `json:"sinceChange"` // 自从上次变化
	SinceOpen   Value `json:"sinceOpen"`   // 自从开仓
}

type Leverage struct {
	RawUsd Value  `json:"rawUsd"` // 原始美元价值
	Type   string `json:"type"`   // 杠杆类型
	Value  int    `json:"value"`  // 杠杆倍数
}

type MarginSummary struct {
	AccountValue    Value `json:"accountValue"`    // 账户价值
	TotalMarginUsed Value `json:"totalMarginUsed"` // 总保证金使用量
	TotalNtlPos     Value `json:"totalNtlPos"`     // 总名义价值
	TotalRawUsd     Value `json:"totalRawUsd"`     // 总原始美元价值
}

func (c *Client) GetUserAccountSummary(user string) (*AccountSummary, error) {
	fullUrl := c.rpcURL + "/info"
	params := H{
		"type": "clearinghouseState",
		"user": user,
	}
	body, err := c.postJson(fullUrl, params)
	if err != nil {
		return nil, err
	}
	if len(body) == 4 && string(body) == "null" {
		return nil, ErrNull
	}
	var summary AccountSummary
	err = json.Unmarshal(body, &summary)
	if err != nil {
		printData("GetUserAccountSummary "+user+": ", body)
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	return &summary, nil
}

// MarketFill 市场交易记录
type MarketFill struct {
	Coin          string  `json:"coin"`          // 币种
	Price         Value   `json:"px"`            // 价格
	Size          Value   `json:"sz"`            // 仓位大小
	Side          string  `json:"side"`          // 方向 (A: Ask, B: Bid)
	Time          int64   `json:"time"`          // 时间戳（毫秒）
	Hash          string  `json:"hash"`          // 交易哈希
	ClosedPnl     Value   `json:"closedPnl"`     // 已实现盈亏
	Fee           Value   `json:"fee"`           // 手续费
	StartPosition Value   `json:"startPosition"` // 起始仓位
	Direction     string  `json:"dir"`           // 方向
	OrderID       int64   `json:"oid"`           // 订单ID
	TradeID       int64   `json:"tid"`           // 交易ID
	User          string  `json:"user,omitempty"` // 用户地址（如果 API 返回）
}

// GetMarketFills 获取市场交易记录
// coin: 币种名称，如 "BTC", "ETH" 等
// startTime: 起始时间戳（毫秒），如果为 0 则从最早开始
// endTime: 结束时间戳（毫秒），如果为 0 则到当前时间
// 注意：API 可能对单次查询返回的数据量有限制（例如 1000 条），需要分批查询
func (c *Client) GetMarketFills(coin string, startTime, endTime int64) ([]MarketFill, error) {
	fullUrl := c.rpcURL + "/info"
	params := H{
		"type": "marketFills",
		"coin": coin,
	}
	
	// 如果指定了时间范围，添加时间参数
	if startTime > 0 {
		params["startTime"] = startTime
	}
	if endTime > 0 {
		params["endTime"] = endTime
	}
	
	body, err := c.postJson(fullUrl, params)
	if err != nil {
		return nil, err
	}
	if len(body) == 4 && string(body) == "null" {
		return nil, ErrNull
	}
	var fills []MarketFill
	err = json.Unmarshal(body, &fills)
	if err != nil {
		printData("GetMarketFills "+coin+": ", body)
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	return fills, nil
}

// GetAllMarketFills 分批获取市场所有交易记录
// coin: 币种名称，如 "BTC", "ETH" 等
// startTime: 起始时间戳（毫秒），如果为 0 则从最早开始
// endTime: 结束时间戳（毫秒），如果为 0 则到当前时间
// timeWindow: 每次查询的时间窗口（毫秒），建议设置为 1 小时或更短，避免单次查询数据量过大
// maxFillsPerRequest: 每次请求的最大记录数，如果返回的记录数达到此值，说明需要缩小时间窗口
func (c *Client) GetAllMarketFills(coin string, startTime, endTime int64, timeWindow time.Duration, maxFillsPerRequest int) ([]MarketFill, error) {
	if timeWindow <= 0 {
		timeWindow = time.Hour // 默认 1 小时
	}
	if maxFillsPerRequest <= 0 {
		maxFillsPerRequest = 1000 // 默认 1000 条
	}
	
	var allFills []MarketFill
	currentStart := startTime
	if endTime == 0 {
		endTime = time.Now().UnixMilli()
	}
	
	windowMs := timeWindow.Milliseconds()
	
	slog.Info("开始分批查询市场交易记录",
		"coin", coin,
		"start_time", startTime,
		"end_time", endTime,
		"time_window", timeWindow,
		"max_fills_per_request", maxFillsPerRequest)
	
	for currentStart < endTime {
		currentEnd := currentStart + windowMs
		if currentEnd > endTime {
			currentEnd = endTime
		}
		
		slog.Debug("查询市场交易记录",
			"coin", coin,
			"start_time", currentStart,
			"end_time", currentEnd,
			"collected_count", len(allFills))
		
		fills, err := c.GetMarketFills(coin, currentStart, currentEnd)
		if err != nil {
			if err == ErrNull {
				// 返回 null 表示该时间段没有数据，继续下一个时间段
				slog.Debug("该时间段没有数据", "coin", coin, "start_time", currentStart, "end_time", currentEnd)
				currentStart = currentEnd
				continue
			}
			return nil, fmt.Errorf("查询市场交易记录失败 [%d-%d]: %w", currentStart, currentEnd, err)
		}
		
		// 如果返回的记录数达到最大值，说明需要缩小时间窗口
		if len(fills) >= maxFillsPerRequest {
			// 将时间窗口缩小一半
			newWindow := windowMs / 2
			if newWindow < 1000 { // 最小 1 秒
				newWindow = 1000
			}
			slog.Warn("返回记录数达到最大值，缩小时间窗口",
				"coin", coin,
				"fills_count", len(fills),
				"old_window_ms", windowMs,
				"new_window_ms", newWindow)
			windowMs = newWindow
			// 不更新 currentStart，重新查询当前时间段
			continue
		}
		
		allFills = append(allFills, fills...)
		
		// 如果返回的记录数为 0，说明该时间段没有数据，可以跳过更大的时间窗口
		if len(fills) == 0 {
			currentStart = currentEnd
		} else {
			// 使用最后一条记录的时间作为下一个时间段的起始时间
			// 这样可以避免数据遗漏，但可能会有少量重复（需要在调用方去重）
			lastTime := fills[len(fills)-1].Time
			if lastTime > currentStart {
				currentStart = lastTime + 1 // +1 避免重复
			} else {
				currentStart = currentEnd
			}
		}
		
		// 添加小延迟，避免请求过于频繁
		time.Sleep(100 * time.Millisecond)
	}
	
	slog.Info("分批查询完成",
		"coin", coin,
		"total_fills", len(allFills),
		"start_time", startTime,
		"end_time", endTime)
	
	return allFills, nil
}

// GetAllAddressesFromMarketFills 通过 Hyperliquid Info API 的 marketFills 获取所有唯一的用户地址
// 通过查询多个币种的市场交易记录，提取唯一的用户地址
// coins: 要查询的币种列表，如果为空则查询所有主要币种
// startTime: 起始时间戳（毫秒），如果为 0 则从最近开始
// endTime: 结束时间戳（毫秒），如果为 0 则到当前时间
// timeWindow: 每次查询的时间窗口（毫秒），建议设置为 1 小时或更短
func (c *Client) GetAllAddressesFromMarketFills(coins []string, startTime, endTime int64, timeWindow time.Duration, logger *slog.Logger) ([]string, error) {
	// 如果没有指定币种，使用默认的主要币种列表
	if len(coins) == 0 {
		coins = []string{"BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "HYPE", "SUI"}
		if logger != nil {
			logger.Info("使用默认币种列表", "coins", coins)
		}
	}
	
	// 使用 map 存储唯一的地址
	addressSet := make(map[string]bool)
	
	// 如果未指定时间范围，查询最近的数据（例如最近 7 天）
	if startTime == 0 && endTime == 0 {
		endTime = time.Now().UnixMilli()
		startTime = endTime - 7*24*60*60*1000 // 最近 7 天
		if logger != nil {
			logger.Info("未指定时间范围，查询最近7天的数据",
				"start_time", startTime,
				"end_time", endTime)
		}
	}
	
	if endTime == 0 {
		endTime = time.Now().UnixMilli()
	}
	
	if timeWindow <= 0 {
		timeWindow = time.Hour // 默认 1 小时
	}
	
	if logger != nil {
		logger.Info("开始通过 Hyperliquid Info API 获取地址",
			"coins", coins,
			"start_time", startTime,
			"end_time", endTime,
			"time_window", timeWindow)
	}
	
	// 遍历每个币种
	for _, coin := range coins {
		if logger != nil {
			logger.Info("查询币种的市场交易记录", "coin", coin)
		}
		
		// 分批查询该币种的市场交易记录
		fills, err := c.GetAllMarketFills(coin, startTime, endTime, timeWindow, 1000)
		if err != nil {
			if logger != nil {
				logger.Warn("查询币种市场交易记录失败",
					"coin", coin,
					"error", err)
			}
			continue
		}
		
		if logger != nil {
			logger.Info("获取到币种市场交易记录",
				"coin", coin,
				"fills_count", len(fills))
		}
		
		// 从交易记录中提取用户地址
		// 注意：MarketFill 结构可能不包含用户地址，需要通过交易哈希查询
		// 但为了效率，我们先尝试从 MarketFill 中提取（如果 API 返回了 user 字段）
		for _, fill := range fills {
			if fill.User != "" {
				addressSet[strings.ToLower(fill.User)] = true
			}
		}
		
		// 添加延迟，避免请求过快
		time.Sleep(100 * time.Millisecond)
	}
	
	// 转换为切片
	addresses := make([]string, 0, len(addressSet))
	for addr := range addressSet {
		addresses = append(addresses, addr)
	}
	
	if logger != nil {
		logger.Info("通过 Hyperliquid Info API 获取地址完成",
			"total_addresses", len(addresses),
			"coins_queried", len(coins))
		
		// 如果通过 marketFills 没有获取到地址（因为 MarketFill 可能不包含 user 字段）
		// 返回空列表，让调用方知道需要其他方法
		if len(addresses) == 0 {
			logger.Warn("通过 marketFills 未获取到地址",
				"tip", "MarketFill 结构可能不包含用户地址字段，建议使用链上 RPC 方法")
		}
	}
	
	return addresses, nil
}
