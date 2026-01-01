package utils

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"hyper-tracker/pkg/errorx"
)

// LocalAddrPool 管理本地地址池
type LocalAddrPool struct {
	addrs   []net.IP      // 可用的本地IP地址列表
	proxies []*url.URL    // 代理列表
	current atomic.Uint64 // 当前使用的地址索引
	mu      sync.RWMutex  // 用于保护地址列表的互斥锁
}

// NewLocalAddrPool 创建一个新的本地地址池
func NewLocalAddrPool(addrs []string, proxies []string) (*LocalAddrPool, error) {
	if len(addrs) == 0 && len(proxies) == 0 {
		return nil, errorx.New("本地地址池不能为空")
	}

	pool := &LocalAddrPool{
		addrs:   make([]net.IP, 0, len(addrs)),
		proxies: make([]*url.URL, 0, len(proxies)),
	}

	for _, addr := range addrs {
		ip := net.ParseIP(addr)
		if ip == nil {
			return nil, errorx.Errorf("无效的IP地址: %s", addr)
		}
		pool.addrs = append(pool.addrs, ip)
	}
	for _, proxy := range proxies {
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			return nil, errorx.Errorf("无效的代理地址: %s", proxy)
		}
		pool.proxies = append(pool.proxies, proxyURL)
	}

	return pool, nil
}

// NextAddr 轮询获取下一个本地地址或代理
func (p *LocalAddrPool) NextAddr() (net.IP, *url.URL) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	l1 := uint64(len(p.addrs))
	l2 := uint64(len(p.proxies))
	count := l1 + l2
	if count == 0 {
		return nil, nil
	}

	current := p.current.Add(1) % count
	if current < l1 {
		return p.addrs[current], nil
	} else {
		return nil, p.proxies[current-l1]
	}
}

func (p *LocalAddrPool) Addrs() []net.IP {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.addrs
}

func (p *LocalAddrPool) Proxies() []*url.URL {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.proxies
}

// HttpClient 支持本地地址轮换的HTTP客户端
type HttpClient struct {
	addrPool  *LocalAddrPool
	defaultIP net.IP
	timeout   time.Duration
}

// HttpClientOption 定义HttpClient的配置选项
type HttpClientOption func(*HttpClient)

// WithLocalAddrPool 设置本地地址池
func WithLocalAddrPool(addrs []string) HttpClientOption {
	return func(c *HttpClient) {
		if len(addrs) == 0 {
			return
		}
		pool, err := NewLocalAddrPool(addrs, nil)
		if err != nil {
			slog.Error("NewLocalAddrPool: " + err.Error())
			return
		}
		c.addrPool = pool
	}
}

func WithProxies(proxies []string) HttpClientOption {
	return func(c *HttpClient) {
		if len(proxies) == 0 {
			return
		}
		pool, err := NewLocalAddrPool(nil, proxies)
		if err != nil {
			slog.Error("NewLocalAddrPool: " + err.Error())
			return
		}
		c.addrPool = pool
	}
}

func WithLocalAddrPoolAndProxies(addrs []string, proxies []string) HttpClientOption {
	return func(c *HttpClient) {
		if len(addrs) == 0 && len(proxies) == 0 {
			return
		}
		pool, err := NewLocalAddrPool(addrs, proxies)
		if err != nil {
			slog.Error("NewLocalAddrPool: " + err.Error())
			return
		}
		c.addrPool = pool
	}
}

// WithDefaultIP 设置默认本地IP
func WithDefaultIP(ip string) HttpClientOption {
	return func(c *HttpClient) {
		if parsedIP := net.ParseIP(ip); parsedIP != nil {
			c.defaultIP = parsedIP
		}
	}
}

func WithTimeout(timeout time.Duration) HttpClientOption {
	return func(c *HttpClient) {
		c.timeout = timeout
	}
}

// NewHttpClient 创建一个新的HTTP客户端
func NewHttpClient(opts ...HttpClientOption) *HttpClient {
	c := &HttpClient{}
	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *HttpClient) Client() *http.Client {
	if c.addrPool == nil {
		if c.defaultIP == nil {
			return http.DefaultClient
		}
		return c.newClientWithIP(c.defaultIP)
	}

	ip, proxy := c.addrPool.NextAddr()
	if ip != nil {
		return c.newClientWithIP(ip)
	} else if proxy != nil {
		return c.newClientWithProxy(proxy)
	}
	return http.DefaultClient
}

// Range 遍历本地地址池和代理列表，执行回调函数
func (c *HttpClient) Range(h func(*http.Client) bool) {
	addrs := c.addrPool.Addrs()
	for _, ip := range addrs {
		client := c.newClientWithIP(ip)
		if !h(client) {
			return
		}
	}
	proxies := c.addrPool.Proxies()
	for _, proxy := range proxies {
		client := c.newClientWithProxy(proxy)
		if !h(client) {
			return
		}
	}
}

// dialContext 创建一个支持本地地址选择的连接器
func (c *HttpClient) newClientWithIP(ip net.IP) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				dialer := &net.Dialer{
					LocalAddr: &net.TCPAddr{IP: ip},
				}
				return dialer.DialContext(ctx, network, addr)
			},
		},
		Timeout: c.timeout,
	}
}

func (c *HttpClient) newClientWithProxy(proxy *url.URL) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxy),
		},
		Timeout: c.timeout,
	}
}
