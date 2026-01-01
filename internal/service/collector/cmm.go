package collector

import (
	"context"
	"fmt"
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"hyper-tracker/pkg/cmm"
	"hyper-tracker/pkg/utils"
	"log/slog"
	"slices"
	"time"
)

type CMM struct {
	ctx         context.Context
	cfg         *config.Config
	cmm         *cmm.Client
	rateLimiter *utils.RateLimiter
}

func NewCMM(ctx context.Context, cfg *config.Config, networkConfig *config.NetworkConfig) *CMM {
	return &CMM{
		ctx:         ctx,
		cfg:         cfg,
		cmm:         cmm.NewClient(&cfg.CMM, networkConfig.LocalAddrs, networkConfig.Proxies),
		rateLimiter: utils.NewRateLimiter(cfg.Collect.RateLimit),
	}
}

func (cm *CMM) getPageWallets(segment int, limit int, offset int) (int, []cmm.WalletInfo, error) {
	query := &cmm.GetWalletsRequest{
		Segment: segment,
		Offset:  offset,
		Limit:   limit,
	}
	totalCount, wallets, err := cm.cmm.GetWallets(query)
	if err != nil {
		return 0, nil, err
	}

	return totalCount, wallets, nil
}

func checkProgress(logger *slog.Logger, totalCount int, limit int, page int, size int) bool {
	currOffset := (page-1)*limit + size
	finish := totalCount <= currOffset
	if finish {
		logger.Debug(fmt.Sprintf("第%d页采集完成, 全部采集完成", page))
	} else {
		logger.Debug(fmt.Sprintf("第%d页采集完成, 剩余%d条, 进度%.2f%%", page, totalCount-currOffset, float64(currOffset)/float64(totalCount)*100))
	}
	return finish
}

func (cm *CMM) collectWallets(segment int, dataCh chan<- []cmm.WalletInfo) {
	logger := slog.With("segment", segment, "worker", "collectWallets")
	logger.Debug("开始采集")
	startTime := time.Now()
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("panic: %v", err))
		}
		logger.Debug("采集结束", "cost", time.Since(startTime))
	}()
	limit := 100
	var retryCount int
	for page := 1; ; page++ {
		if err := cm.rateLimiter.Wait(cm.ctx); err != nil {
			logger.Error("等待限流令牌失败", "error", err)
			break
		}

		offset := (page - 1) * limit
		totalCount, wallets, err := cm.getPageWallets(segment, limit, offset)
		if err != nil {
			logger.Error("采集失败", "error", err)
			retryCount++
			if retryCount > cm.cfg.Collect.RetryCount {
				break
			}
			page-- // 重试时，页码减1
			logger.Debug(fmt.Sprintf("%d次重试", retryCount))
			continue
		}
		retryCount = 0
		if totalCount == 0 || len(wallets) == 0 {
			break
		}
		dataCh <- wallets
		if checkProgress(logger, totalCount, limit, page, len(wallets)) {
			break
		}
	}
}

func (cm *CMM) saveWallets(ch <-chan []cmm.WalletInfo, now time.Time) error {
	logger := slog.With("worker", "saveWallets")

	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error(fmt.Sprintf("panic: %v", err))
			}
		}()
		for wallets := range ch {
			var addressInfos []dao.AddressInfo
			for _, wallet := range wallets {
				if wallet.Address == "" || slices.Contains(cm.cfg.Collect.Blacklist, wallet.Address) {
					continue
				}
				addressInfos = append(addressInfos, dao.AddressInfo{
					Address:     wallet.Address,
					TotalEquity: wallet.TotalEquity,
					PrepPnl:     wallet.PerpPnl,
					PrepOpen:    wallet.OpenValue,
					PrepEquity:  wallet.PerpEquity,
					SumUpnl:     wallet.SumUpnl,
					EarliestAt:  wallet.EarliestActivityAt,
					CreatedAt:   now,
					UpdatedAt:   now,
				})
			}
			if err := dao.BatchInsertAddressInfo(addressInfos); err != nil {
				logger.Error("BatchInsertAddressInfo: " + err.Error())
				continue
			}
		}
		logger.Debug("done")
	}()
	return nil
}

func (cm *CMM) exec() {
	startTime := time.Now()
	defer func() {
		slog.Info("采集结束，耗时: " + time.Since(startTime).String())
	}()
	walletsCh := make(chan []cmm.WalletInfo, 100)
	err := cm.saveWallets(walletsCh, startTime)
	if err != nil {
		slog.Error(fmt.Sprintf("saveWallets: %v", err))
		return
	}
	for _, segment := range cm.cfg.Collect.Segments {
		cm.collectWallets(segment, walletsCh)
	}
	close(walletsCh)
}

func (cm *CMM) Start() {
	c, resetTimer, saveNextTime := timer(cm.cfg.Collect.Interval, dao.CMMNextTimeVar)
	resetTimer()
	for {
		select {
		case <-c:
			cm.exec()
			saveNextTime()
			resetTimer()
			RefreshExec(cm.cfg.Collect.CmmExecs)
		case <-cm.ctx.Done():
			return
		}
	}
}
