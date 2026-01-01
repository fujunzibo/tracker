package api

import (
	"hyper-tracker/internal/config"
	"hyper-tracker/internal/service/collector"
	"log/slog"
	"net/http"
)

// Service API 服务
type Service struct {
	cfg       *config.Config
	logger    *slog.Logger
	collector *collector.Service
	server    *http.Server
}

// NewService 创建 API 服务
func NewService(cfg *config.Config, collectorSvc *collector.Service) *Service {
	return &Service{
		cfg:       cfg,
		logger:    slog.With("service", "API"),
		collector: collectorSvc,
	}
}

// NewServiceWithFollow 创建带跟单服务的 API 服务
func NewServiceWithFollow(cfg *config.Config, collectorSvc *collector.Service) *Service {
	return &Service{
		cfg:       cfg,
		logger:    slog.With("service", "API"),
		collector: collectorSvc,
	}
}

// Start 启动 HTTP API 服务
func (s *Service) Start() {
	mux := http.NewServeMux()

	// 数据管理 API
	mux.HandleFunc("/api/test", s.withAuth("test", s.handleTest))
	mux.HandleFunc("/api/skip/list", s.withAuth("listSkipAddress", s.handleListSkipAddress))
	mux.HandleFunc("/api/skip/add", s.withAuth("addSkipAddress", s.handleAddSkipAddress))
	mux.HandleFunc("/api/skip/delete", s.withAuth("deleteSkipAddress", s.handleDeleteSkipAddress))
	mux.HandleFunc("/api/skip/remark", s.withAuth("updateRemark", s.handleUpdateRemark))
	mux.HandleFunc("/api/data/refresh", s.withAuth("refreshData", s.handleRefreshData))

	// 地址集合 API
	mux.HandleFunc("/api/address-set/list", s.withAuth("listAddressSet", s.handleListAddressSet))
	mux.HandleFunc("/api/address-set/get", s.withAuth("getAddressSet", s.handleGetAddressSet))
	mux.HandleFunc("/api/address-set/create", s.withAuth("createAddressSet", s.handleCreateAddressSet))
	mux.HandleFunc("/api/address-set/update", s.withAuth("updateAddressSet", s.handleUpdateAddressSet))
	mux.HandleFunc("/api/address-set/delete", s.withAuth("deleteAddressSet", s.handleDeleteAddressSet))
	mux.HandleFunc("/api/address-set/refresh", s.withAuth("refreshAddressSet", s.handleRefreshAddressSet))
	mux.HandleFunc("/api/address-set/preview", s.withAuth("previewAddressSet", s.handlePreviewAddressSet))
	mux.HandleFunc("/api/address-set/positions", s.withAuth("positionsAddressSet", s.handleGetAddressSetPositions))
	mux.HandleFunc("/api/address-set/summary", s.withAuth("summaryAddressSet", s.handleGetAddressSetSummary))

	// 静态文件服务
	fs := http.FileServer(http.Dir("static"))
	mux.Handle("/", fs)

	s.server = &http.Server{
		Addr:    s.cfg.Server.Listen,
		Handler: mux,
	}

	s.logger.Info("HTTP API 服务启动", "listen", s.cfg.Server.Listen)
	go func() {
		err := s.server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP API 服务错误", "error", err)
		}
	}()
}

// Stop 停止 API 服务
func (s *Service) Stop() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}

func (s *Service) handleTest(w http.ResponseWriter, r *http.Request) {
	WriteMsg(w, "success")
}
