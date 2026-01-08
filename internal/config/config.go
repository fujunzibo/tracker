package config

import (
	"hyper-tracker/pkg/cmm"
	"hyper-tracker/pkg/configx"
	"hyper-tracker/pkg/log"
	"os"
	"reflect"
	"time"

	"gopkg.in/yaml.v3"
)

// LoadConfig 加载配置文件
func LoadConfig(file string) (*Config, error) {
	fp, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	cfg := new(Config)
	v := reflect.ValueOf(cfg).Elem()
	configx.Preset(v)

	dec := yaml.NewDecoder(fp)
	if err := dec.Decode(cfg); err != nil {
		return nil, err
	}

	err = configx.Validate(v)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// Config 主配置结构
type Config struct {
	Server    ServerConfig      `json:"server" yaml:"server"`
	CMM       cmm.Config        `json:"cmm" yaml:"cmm"`
	Log       log.Config        `json:"log" yaml:"log"`
	DSN       string            `json:"dsn" yaml:"dsn"`
	Network   NetworkConfig     `json:"network" yaml:"network"`
	Collect   CollectConfig     `json:"collect" yaml:"collect"`
	Follow    FollowConfig      `json:"follow" yaml:"follow"`
	QueryQysy QueryQysyConfig  `json:"queryQysy" yaml:"query_qysy"`
	Binance   *BinanceConfig    `json:"binance" yaml:"binance"`
	Okx       *OkxConfig        `json:"okx" yaml:"okx"`
}

// ServerConfig HTTP 服务器配置
type ServerConfig struct {
	Listen string `json:"listen" yaml:"listen"`
	Secret string `json:"secret" yaml:"secret"`
}

func (c *ServerConfig) Preset() {
	c.Listen = "0.0.0.0:8080"
	c.Secret = "-hashhub-"
}

// ExecConfig 执行配置
type ExecConfig struct {
	Name    string        `json:"name" yaml:"name"`
	Timeout time.Duration `json:"timeout" yaml:"timeout"`
	Queries []string      `json:"queries" yaml:"queries"`
}

// NetworkConfig 网络配置
type NetworkConfig struct {
	LocalAddrs []string `json:"localAddrs" yaml:"localAddrs"`
	Proxies    []string `json:"proxies" yaml:"proxies"`
}

// CollectConfig 数据采集配置
type CollectConfig struct {
	Segments               []int        `json:"segments" yaml:"segments"`
	Interval               int          `json:"interval" yaml:"interval"`
	RateLimit              int          `json:"rateLimit" yaml:"rateLimit"`
	RetryCount             int          `json:"retryCount" yaml:"retryCount"`
	RetryDelay             int          `json:"retryDelay" yaml:"retryDelay"`
	Blacklist              []string     `json:"blacklist" yaml:"blacklist"`
	WorkerCount            int          `json:"workerCount" yaml:"workerCount"`
	QueueSize              int          `json:"queueSize" yaml:"queueSize"`
	BatchInterval          int          `json:"batchInterval" yaml:"batchInterval"`
	SaveInterval           int          `json:"saveInterval" yaml:"saveInterval"`
	PointInterval          int64        `json:"pointInterval" yaml:"pointInterval"`
	CmmExecs               []ExecConfig `json:"cmmExecs" yaml:"cmmExecs"`
	HyperExecs             []ExecConfig `json:"hyperExecs" yaml:"hyperExecs"`
	UpdatePositionSets     []int        `json:"updatePositionSets" yaml:"updatePositionSets"`
	UpdatePositionInterval int          `json:"updatePositionInterval" yaml:"updatePositionInterval"`
}

func (c *CollectConfig) Preset() {
	c.RetryCount = 5
	c.RetryDelay = 1000
	c.WorkerCount = 5
	c.QueueSize = 1000
	c.BatchInterval = 300
	c.SaveInterval = 2
	c.PointInterval = 86400
}

// BinanceConfig 币安交易所配置
type BinanceConfig struct {
	ApiKey    string `json:"apiKey" yaml:"apiKey"`
	ApiSecret string `json:"apiSecret" yaml:"apiSecret"`
	Testnet   bool   `json:"testnet" yaml:"testnet"`
	Debug     bool   `json:"debug" yaml:"debug"`
	Unified   bool   `json:"unified" yaml:"unified"`
}

func (c *BinanceConfig) Preset() {
	c.Testnet = false
	c.Debug = false
	c.Unified = true
}

// OkxConfig OKX 交易所配置
type OkxConfig struct {
	ApiKey        string `json:"apiKey" yaml:"apiKey"`
	ApiSecret     string `json:"apiSecret" yaml:"apiSecret"`
	ApiPassphrase string `json:"apiPassphrase" yaml:"apiPassphrase"`
	Testnet       bool   `json:"testnet" yaml:"testnet"`
	Debug         bool   `json:"debug" yaml:"debug"`
}

func (c *OkxConfig) Preset() {
	c.Testnet = false
	c.Debug = false
}

// FollowConfig 跟单配置
type FollowConfig struct {
	RpcUrl                    string   `json:"rpcUrl" yaml:"rpcUrl"`                             // QuickNode RPC URL（用于链上数据获取，备用）
	HyperEvmRpcUrl            string   `json:"hyperEvmRpcUrl" yaml:"hyperEvmRpcUrl"`             // HyperEVM RPC URL（Hyperliquid 官方 RPC，默认：https://rpc.hyperliquid.xyz/evm）
	ClearinghouseAddress      string   `json:"clearinghouseAddress" yaml:"clearinghouseAddress"` // Hyperliquid Clearinghouse 合约地址
	ExchangeAddress           string   `json:"exchangeAddress" yaml:"exchangeAddress"`          // Hyperliquid Exchange 合约地址（可选）
	AddressSets               []int    `json:"addressSets" yaml:"addressSets"`
	ExcludeCoins              []string `json:"excludeCoins" yaml:"excludeCoins"`
	FollowRatio               int      `json:"followRatio" yaml:"followRatio"`
	Leverage                  int64    `json:"leverage" yaml:"leverage"`
	MinFollowValue            float64  `json:"minFollowValue" yaml:"minFollowValue"`
	MaxFollowValue            float64  `json:"maxFollowValue" yaml:"maxFollowValue"`
	TimeDiff                  int64    `json:"timeDiff" yaml:"timeDiff"`
	MergeInterval             int64    `json:"mergeInterval" yaml:"mergeInterval"`
	PerWsUserCount            int      `json:"perWsUserCount" yaml:"perWsUserCount"`
	MsgIODir                  string   `json:"msgIODir" yaml:"msgIODir"`
	MarketDataUrl             string   `json:"marketDataUrl" yaml:"marketDataUrl"`
	Listen                    string   `json:"listen" yaml:"listen"`
	AutoStart                 bool     `json:"autoStart" yaml:"autoStart"`
	CopyPosition              bool     `json:"copyPosition" yaml:"copyPosition"`
}

func (c *FollowConfig) Preset() {
	c.PerWsUserCount = 10
	c.Leverage = 5
	c.TimeDiff = 1000
	c.MinFollowValue = 5
	c.MergeInterval = 1000
	c.Listen = "127.0.0.1:3006"
}

// QueryQysyConfig 用户权益损益查询配置
type QueryQysyConfig struct {
	MaxConcurrency int `json:"maxConcurrency" yaml:"max_concurrency"` // 最大并发数
	BatchSize      int `json:"batchSize" yaml:"batch_size"`           // 批量保存大小
	RetryCount     int `json:"retryCount" yaml:"retry_count"`         // 重试次数
	RetryDelay     int `json:"retryDelay" yaml:"retry_delay"`         // 重试延迟（秒）
	RateLimit      int `json:"rateLimit" yaml:"rate_limit"`           // 速率限制（每10秒请求数）
}

func (c *QueryQysyConfig) Preset() {
	c.MaxConcurrency = 400
	c.BatchSize = 400
	c.RetryCount = 3
	c.RetryDelay = 2
	c.RateLimit = 1000
}
