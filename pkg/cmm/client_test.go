package cmm

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

var testConfig = &Config{
	AccessToken:  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InhpYW95YW8iLCJzdWIiOjE3Mjk4MCwiand0aWQiOiI3ZjBhYzE4NGJmMTYxNDczZTMzMyIsImlhdCI6MTc1Nzg1MDY5NywiZXhwIjoxNzU3ODkzODk3fQ.G4nNdXpA-WfnEdYdmcuKr6Q5piaQMYUbTnwJjYkWuMI",
	RefreshToken: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InhpYW95YW8iLCJzdWIiOjE3Mjk4MCwiand0aWQiOiI3ZjBhYzE4NGJmMTYxNDczZTMzMyIsImlhdCI6MTc1Nzg1MDY5NywiZXhwIjoxNzU5MDYwMjk3fQ.ePyv1M50hJzuj702vCcqWVUlkB9GSGusf-AEnyaRm8s",
}

// TestNewClient 测试客户端初始化
func TestNewClient(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
		want *Client
	}{
		{
			name: "使用默认配置初始化",
			cfg: &Config{
				AccessToken:  "test-token",
				RefreshToken: "test-refresh-token",
			},
			want: &Client{
				cfg: &Config{
					AccessToken:  "test-token",
					RefreshToken: "test-refresh-token",
					TokenTTL:     12 * 60 * 60, // 默认TTL
					Timeout:      10,           // 默认超时
				},
			},
		},
		{
			name: "使用自定义配置初始化",
			cfg: &Config{
				AccessToken:  "test-token",
				RefreshToken: "test-refresh-token",
				TokenTTL:     3600,
				Timeout:      30,
			},
			want: &Client{
				cfg: &Config{
					AccessToken:  "test-token",
					RefreshToken: "test-refresh-token",
					TokenTTL:     3600,
					Timeout:      30,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewClient(tt.cfg, nil, nil)

			// 验证配置是否正确设置
			if got.cfg.AccessToken != tt.want.cfg.AccessToken {
				t.Errorf("AccessToken = %v, want %v", got.cfg.AccessToken, tt.want.cfg.AccessToken)
			}
			if got.cfg.TokenTTL != tt.want.cfg.TokenTTL {
				t.Errorf("TokenTTL = %v, want %v", got.cfg.TokenTTL, tt.want.cfg.TokenTTL)
			}
			if got.cfg.Timeout != tt.want.cfg.Timeout {
				t.Errorf("Timeout = %v, want %v", got.cfg.Timeout, tt.want.cfg.Timeout)
			}
		})
	}
}

// TestClient_Do 测试HTTP请求执行
func TestClient_Do(t *testing.T) {
	// 创建测试服务器
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 验证请求头
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("Authorization header = %v, want %v", r.Header.Get("Authorization"), "Bearer test-token")
		}
		if r.Header.Get("Origin") != "https://app.coinmarketman.com" {
			t.Errorf("Origin header = %v, want %v", r.Header.Get("Origin"), "https://app.coinmarketman.com")
		}
		if r.Header.Get("Accept") != "application/json" {
			t.Errorf("Accept header = %v, want %v", r.Header.Get("Accept"), "application/json")
		}

		// 返回测试响应
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success"}`))
	}))
	defer server.Close()

	// 创建测试客户端
	client := NewClient(&Config{
		AccessToken: "test-token",
		Timeout:     5,
	}, nil, nil)

	// 创建测试请求
	req, err := http.NewRequest("GET", server.URL+"/test", nil)
	if err != nil {
		t.Fatalf("创建请求失败: %v", err)
	}

	// 执行请求
	code, body, err := client.Do(req)
	if err != nil {
		t.Fatalf("执行请求失败: %v", err)
	}

	// 验证响应
	if code != http.StatusOK {
		t.Errorf("状态码 = %v, want %v", code, http.StatusOK)
	}

	var response map[string]string
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}

	if response["status"] != "success" {
		t.Errorf("响应状态 = %v, want %v", response["status"], "success")
	}
}

// TestClient_GetWallets 测试获取钱包列表
func TestClient_GetWallets(t *testing.T) {
	// 创建测试服务器
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 验证请求方法
		if r.Method != "GET" {
			t.Errorf("请求方法 = %v, want %v", r.Method, "GET")
		}

		// 验证请求参数
		query := r.URL.Query()
		if query.Get("limit") != "100" {
			t.Errorf("limit = %v, want %v", query.Get("limit"), "100")
		}
		if query.Get("order") != "desc" {
			t.Errorf("order = %v, want %v", query.Get("order"), "desc")
		}
		if query.Get("orderBy") != "openValue" {
			t.Errorf("orderBy = %v, want %v", query.Get("orderBy"), "openValue")
		}

		// 返回测试响应
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		response := `{
			"totalCount": 1,
			"items": [{
				"segments": [1],
				"address": "test-address",
				"totalEquity": 1000.0,
				"earliestActivityAt": "2024-01-01T00:00:00Z",
				"perpPnl": 100.0,
				"perpEquity": 900.0,
				"exposureRatio": 0.9,
				"perpBias": 0.1,
				"openValue": 800.0,
				"sumUpnl": 50.0
			}]
		}`
		w.Write([]byte(response))
	}))
	defer server.Close()

	// 保存原始BaseUrl并在测试结束后恢复
	originalBaseUrl := BaseUrl
	BaseUrl = server.URL
	defer func() { BaseUrl = originalBaseUrl }()

	// 创建测试客户端
	client := NewClient(&Config{
		AccessToken: "test-token",
		Timeout:     5,
	}, nil, nil)

	// 执行请求
	_, wallets, err := client.GetWallets(&GetWalletsRequest{
		Segment: 1,
	})
	if err != nil {
		t.Fatalf("获取钱包列表失败: %v", err)
	}

	// 验证响应
	if len(wallets) != 1 {
		t.Fatalf("钱包数量 = %v, want %v", len(wallets), 1)
	}

	wallet := wallets[0]
	if wallet.Address != "test-address" {
		t.Errorf("钱包地址 = %v, want %v", wallet.Address, "test-address")
	}
	if wallet.TotalEquity != 1000.0 {
		t.Errorf("总价值 = %v, want %v", wallet.TotalEquity, 1000.0)
	}
	if wallet.PerpPnl != 100.0 {
		t.Errorf("未实现盈亏 = %v, want %v", wallet.PerpPnl, 100.0)
	}
}

// TestClient_GetPositions 测试获取仓位列表
func TestClient_GetPositions(t *testing.T) {
	// 创建测试服务器
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 验证请求方法
		if r.Method != "GET" {
			t.Errorf("请求方法 = %v, want %v", r.Method, "GET")
		}

		// 验证请求参数
		query := r.URL.Query()
		if query.Get("limit") != "100" {
			t.Errorf("limit = %v, want %v", query.Get("limit"), "100")
		}

		// 返回测试响应
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		response := `{
			"totalCount": 1,
			"items": [{
				"id": "test-position-id",
				"side": "long",
				"address": "test-address",
				"value": 1000.0,
				"size": 10.0,
				"coin": "BTC",
				"entryPrice": 50000.0,
				"unrealizedPnl": 100.0,
				"funding": -5.0,
				"liquidationPrice": 45000.0,
				"markPrice": 51000.0,
				"midPrice": 50900.0,
				"premiumPrice": 100.0,
				"crossLeverage": 10,
				"isolatedLeverage": 0,
				"openTime": "2024-01-01T00:00:00Z",
				"liquidationProgress": 0.0
			}]
		}`
		w.Write([]byte(response))
	}))
	defer server.Close()

	// 保存原始BaseUrl并在测试结束后恢复
	originalBaseUrl := BaseUrl
	BaseUrl = server.URL
	defer func() { BaseUrl = originalBaseUrl }()

	// 创建测试客户端
	client := NewClient(&Config{
		AccessToken: "test-token",
		Timeout:     5,
	}, nil, nil)

	// 执行请求
	_, positions, err := client.GetPositions(&GetPositionsRequest{
		Segment: 1,
	})
	if err != nil {
		t.Fatalf("获取仓位列表失败: %v", err)
	}

	// 验证响应
	if len(positions) != 1 {
		t.Fatalf("仓位数量 = %v, want %v", len(positions), 1)
	}

	position := positions[0]
	if position.ID != "test-position-id" {
		t.Errorf("仓位ID = %v, want %v", position.ID, "test-position-id")
	}
	if position.Side != "long" {
		t.Errorf("仓位方向 = %v, want %v", position.Side, "long")
	}
	if position.Value != 1000.0 {
		t.Errorf("仓位价值 = %v, want %v", position.Value, 1000.0)
	}
	if position.Size != 10.0 {
		t.Errorf("仓位大小 = %v, want %v", position.Size, 10.0)
	}
	if position.EntryPrice != 50000.0 {
		t.Errorf("开仓价格 = %v, want %v", position.EntryPrice, 50000.0)
	}
}

func TestClient_GetWalletsOrigion(t *testing.T) {
	client := NewClient(testConfig, nil, nil)
	_, wallets, err := client.GetWallets(&GetWalletsRequest{
		Segment: 1,
	})
	if err != nil {
		t.Fatalf("获取钱包列表失败: %v", err)
	}
	if len(wallets) == 0 {
		t.Fatalf("钱包数量 = %v, want > 0", len(wallets))
	}
	t.Logf("钱包数量 = %v", len(wallets))
}

func TestClient_GetPositionsOrigion(t *testing.T) {
	client := NewClient(testConfig, nil, nil)
	_, positions, err := client.GetPositions(&GetPositionsRequest{
		Segment: 1,
	})
	if err != nil {
		t.Fatalf("获取仓位列表失败: %v", err)
	}
	if len(positions) == 0 {
		t.Fatalf("仓位数量 = %v, want > 0", len(positions))
	}
	t.Logf("仓位数量 = %v", len(positions))
}

func TestClient_RefreshToken(t *testing.T) {
	client := NewClient(testConfig, nil, nil)
	err := client.RefreshToken()
	if err != nil {
		t.Fatalf("刷新Token失败: %v", err)
	}
	t.Logf("刷新Token成功")
}
