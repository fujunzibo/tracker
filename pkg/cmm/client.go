package cmm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"hyper-tracker/pkg/utils"
)

var BaseUrl = "https://ht.coinmarketman.com/api"
var TokenExpiredKey = `"statusCode":498`
var ErrTokenExpired = errors.New("token expired")
var TokenFile = "token.json"

type Client struct {
	cfg    *Config
	client *utils.HttpClient
}

type Config struct {
	AccessToken  string `json:"accessToken" yaml:"accessToken"`
	RefreshToken string `json:"refreshToken" yaml:"refreshToken"`
	TokenTTL     int    `json:"tokenTTL" yaml:"tokenTTL"`
	Timeout      int    `json:"timeout" yaml:"timeout"`
}

func NewClient(cfg *Config, addrs []string, proxies []string) *Client {
	if cfg.TokenTTL == 0 {
		cfg.TokenTTL = 12 * 60 * 60
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10
	}

	cli := &Client{
		cfg:    cfg,
		client: utils.NewHttpClient(utils.WithLocalAddrPoolAndProxies(addrs, proxies), utils.WithTimeout(time.Duration(cfg.Timeout)*time.Second)),
	}

	err := cli.loadToken()
	if err != nil {
		slog.Error("loadToken: " + err.Error())
	}

	cli.keepAlive()

	return cli
}

func (c *Client) keepAlive() {
	go func() {
		interval := time.Duration(c.cfg.TokenTTL) * time.Second
		for {
			time.Sleep(interval)
			err := c.RefreshToken()
			if err != nil {
				slog.Error("RefreshToken: " + err.Error())
			} else {
				slog.Info("RefreshToken success")
				err = c.saveToken()
				if err != nil {
					slog.Error("SaveToken: " + err.Error())
				}
			}
		}
	}()
}

func (c *Client) loadToken() error {
	if _, err := os.Stat(TokenFile); err != nil {
		return nil
	}
	content, err := os.ReadFile(TokenFile)
	if err != nil {
		return err
	}
	var token struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
	}
	err = json.Unmarshal(content, &token)
	if err != nil {
		return err
	}
	c.cfg.AccessToken = token.AccessToken
	c.cfg.RefreshToken = token.RefreshToken
	return nil
}

func (c *Client) saveToken() error {
	token := struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
	}{
		AccessToken:  c.cfg.AccessToken,
		RefreshToken: c.cfg.RefreshToken,
	}
	content, err := json.Marshal(token)
	if err != nil {
		return err
	}
	return os.WriteFile(TokenFile, content, 0644)
}

func (c *Client) Do(req *http.Request) (int, []byte, error) {
	if req.Header.Get("Authorization") == "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.AccessToken)
	}
	req.Header.Set("Origin", "https://app.coinmarketman.com")
	req.Header.Set("Referer", "https://app.coinmarketman.com/")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36")

	resp, err := c.client.Client().Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("Do: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, fmt.Errorf("ReadAll: %w", err)
	}

	// 如果Token过期，则刷新Token后重试
	if bytes.Contains(body, []byte(TokenExpiredKey)) {
		err = c.RefreshToken()
		if err != nil {
			slog.Error("RefreshToken: " + err.Error())
			return 0, nil, ErrTokenExpired
		}
		return c.Do(req)
	}

	return resp.StatusCode, body, nil
}

type Authorization struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
}

func (c *Client) RefreshToken() error {
	fullURL := "https://dashboard.coinmarketman.com/auth/refreshToken"
	req, _ := http.NewRequest("GET", fullURL, nil)
	req.Header.Set("Authorization", "Bearer "+c.cfg.RefreshToken)
	code, body, err := c.Do(req)
	if err != nil {
		return err
	}
	if code != 200 {
		return errors.New(string(body))
	}
	var resp struct {
		Authorization Authorization `json:"authorization"`
	}
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return err
	}
	if resp.Authorization.AccessToken == "" || resp.Authorization.RefreshToken == "" {
		slog.Debug("刷新Token失败: " + string(body))
		return errors.New("refresh token failed")
	}
	c.cfg.AccessToken = resp.Authorization.AccessToken
	c.cfg.RefreshToken = resp.Authorization.RefreshToken
	slog.Info("刷新Token成功", "accessToken", c.cfg.AccessToken, "refreshToken", c.cfg.RefreshToken)
	err = c.saveToken()
	if err != nil {
		slog.Error("保存Token失败", "error", err)
	}
	return nil
}

type GetWalletsRequest struct {
	Segment int    `json:"segment"`
	Limit   int    `json:"limit"`
	Offset  int    `json:"offset"`
	Order   string `json:"order"`
	OrderBy string `json:"orderBy"`
}

type WalletInfo struct {
	Segments           []int     `json:"segments"`           // 所属组合
	Address            string    `json:"address"`            // 钱包地址
	TotalEquity        float64   `json:"totalEquity"`        // 钱包总权益
	EarliestActivityAt time.Time `json:"earliestActivityAt"` // 最早活动时间
	PerpPnl            float64   `json:"perpPnl"`            // 合约盈亏
	PerpEquity         float64   `json:"perpEquity"`         // 持仓价值
	ExposureRatio      float64   `json:"exposureRatio"`      // 持仓比例
	PerpBias           float64   `json:"perpBias"`           // 持仓偏差
	OpenValue          float64   `json:"openValue"`          // 开仓价值
	SumUpnl            float64   `json:"sumUpnl"`            // 未实现盈亏
}

func cutErrMsg(body string) string {
	if len(body) < 30 {
		return body
	}
	if strings.Contains(body, "502 Bad Gateway") {
		return "502 Bad Gateway"
	}
	if strings.Contains(body, "504 Gateway Time-out") {
		return "504 Gateway Time-out"
	}
	return body[:15] + "..." + body[len(body)-15:]
}

func (c *Client) GetWallets(query *GetWalletsRequest) (int, []WalletInfo, error) {
	if query.Limit == 0 {
		query.Limit = 100
	}
	if query.Order == "" {
		query.Order = "desc"
	}
	if query.OrderBy == "" {
		query.OrderBy = "openValue"
	}

	fullURL := fmt.Sprintf("%s/segments/%d/wallets?segmentId=%d&limit=%d&offset=%d&order=%s&orderBy=%s",
		BaseUrl, query.Segment, query.Segment, query.Limit, query.Offset, query.Order, query.OrderBy)

	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return 0, nil, err
	}
	code, body, err := c.Do(req)
	if err != nil {
		return 0, nil, err
	}
	if code != 200 {
		return 0, nil, errors.New(cutErrMsg(string(body)))
	}

	var response struct {
		TotalCount int          `json:"totalCount"`
		Items      []WalletInfo `json:"items"`
	}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return 0, nil, fmt.Errorf("Unmarshal: %w", err)
	}

	return response.TotalCount, response.Items, nil
}

type GetPositionsRequest struct {
	Segment int `json:"segment"`
	Limit   int `json:"limit"`
	Offset  int `json:"offset"`
}

type PositionInfo struct {
	ID               string  `json:"id"`                  // 仓位ID
	Side             string  `json:"side"`                // 方向
	Address          string  `json:"address"`             // 地址
	Value            float64 `json:"value"`               // 仓位价值
	Size             float64 `json:"size"`                // 仓位大小
	Coin             string  `json:"coin"`                // 币种
	EntryPrice       float64 `json:"entryPrice"`          // 开仓价格
	UnrealizedPnl    float64 `json:"unrealizedPnl"`       // 未实现盈亏
	Funding          float64 `json:"funding"`             // 资金费用
	LiquidationPrice float64 `json:"liquidationPrice"`    // 清算价格
	MarkPrice        float64 `json:"markPrice"`           // 标记价格
	MidPrice         float64 `json:"midPrice"`            // 中间价格
	PremiumPrice     float64 `json:"premiumPrice"`        // 溢价
	CrossLeverage    int     `json:"crossLeverage"`       // 全仓杠杆
	IsolatedLeverage int     `json:"isolatedLeverage"`    // 逐仓杠杆
	CloseTime        *string `json:"closeTime"`           // 平仓时间
	OpenTime         string  `json:"openTime"`            // 开仓时间
	LiqProgress      float64 `json:"liquidationProgress"` // 清算进度
}

func (c *Client) GetPositions(query *GetPositionsRequest) (int, []PositionInfo, error) {
	if query.Limit == 0 {
		query.Limit = 100
	}

	fullURL := fmt.Sprintf("%s/segments/%d/positions/largest?segmentId=%d&limit=%d&offset=%d",
		BaseUrl, query.Segment, query.Segment, query.Limit, query.Offset)

	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return 0, nil, err
	}
	code, body, err := c.Do(req)
	if err != nil {
		return 0, nil, err
	}
	if code != 200 {
		return 0, nil, errors.New(cutErrMsg(string(body)))
	}

	var response struct {
		TotalCount int            `json:"totalCount"`
		Items      []PositionInfo `json:"items"`
	}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return 0, nil, fmt.Errorf("Unmarshal: %w", err)
	}
	return response.TotalCount, response.Items, nil
}
