package collector

import (
	"context"
	"hyper-tracker/internal/config"
)

// Service 数据采集服务
type Service struct {
	ctx         context.Context
	cfg         *config.Config
	cmm         *CMM
	hyperliquid *Hyperliquid
}

// NewService 创建数据采集服务
func NewService(ctx context.Context, cfg *config.Config) *Service {
	cmm := NewCMM(ctx, cfg, &cfg.Network)
	hyperliquid := NewHyperliquid(ctx, &cfg.Collect, &cfg.Network, cfg.Follow.ExcludeCoins)
	return &Service{
		ctx:         ctx,
		cfg:         cfg,
		cmm:         cmm,
		hyperliquid: hyperliquid,
	}
}

// Start 启动采集服务
func (s *Service) Start() {
	go s.cmm.Start()
	go s.hyperliquid.Start()
}

// GetHyperliquid 获取 Hyperliquid 采集器（用于工具命令）
func (s *Service) GetHyperliquid() *Hyperliquid {
	return s.hyperliquid
}

