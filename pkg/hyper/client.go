package hyper

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hyper-tracker/pkg/utils"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

const DefaultRpcURL = "https://api.hyperliquid.xyz"

type H = map[string]any

type Client struct {
	rpcURL string
	client *utils.HttpClient
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

func (c *Client) postJson(fullUrl string, params H) ([]byte, error) {
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
