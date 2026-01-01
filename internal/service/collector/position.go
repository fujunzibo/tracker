package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"hyper-tracker/pkg/hyper"
	"hyper-tracker/pkg/utils"
	"log/slog"
	"slices"
	"sync"
	"time"
)

type HyperliquidPositionWorker struct {
	ctx          context.Context
	cfg          *config.CollectConfig
	client       *hyper.Client
	logger       *slog.Logger
	rateLimiter  *utils.RateLimiter
	coins        []string
	excludeCoins []string
}

func NewHyperliquidPositionWorker(ctx context.Context, cfg *config.CollectConfig, client *hyper.Client, logger *slog.Logger, excludeCoins []string) *HyperliquidPositionWorker {
	return &HyperliquidPositionWorker{
		ctx:          ctx,
		cfg:          cfg,
		client:       client,
		logger:       logger,
		excludeCoins: excludeCoins,
		rateLimiter:  utils.NewRateLimiter(cfg.WorkerCount * 10),
	}
}

func (h *HyperliquidPositionWorker) getPositions(logger *slog.Logger, address string) ([]dao.Position, error) {
	var summary *hyper.AccountSummary
	var err error
	for i := 0; i < h.cfg.RetryCount; i++ {
		summary, err = h.client.GetUserAccountSummary(address)
		if err != nil {
			logger.Error("GetUserAccountSummary: "+err.Error(), "retry", i)
			if i != h.cfg.RetryCount-1 {
				time.Sleep(time.Duration(h.cfg.RetryDelay) * time.Second)
				continue
			}
		} else {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	var positions []dao.Position
	for _, position := range summary.AssetPositions {
		if len(h.coins) > 0 && !slices.Contains(h.coins, position.Position.Coin) {
			continue
		}
		if len(h.excludeCoins) > 0 && slices.Contains(h.excludeCoins, position.Position.Coin) {
			continue
		}
		entryPx, _ := position.Position.EntryPx.Float64()
		size, _ := position.Position.Szi.Float64()
		value, _ := position.Position.PositionValue.Float64()
		unrealPnl, _ := position.Position.UnrealizedPnl.Float64()
		liqPx, _ := position.Position.LiquidationPx.Float64()
		p := dao.Position{
			Coin:      position.Position.Coin,
			EntryPx:   entryPx,
			Leverage:  position.Position.Leverage.Value,
			Size:      size,
			Value:     value,
			UnrealPnl: unrealPnl,
			LiqPx:     liqPx,
			Time:      time.UnixMilli(summary.Time),
		}
		positions = append(positions, p)
	}
	return positions, nil
}

type PositionResult struct {
	Address   string
	Positions []dao.Position
}

func (h *HyperliquidPositionWorker) collectPositions(logger *slog.Logger, wait *sync.WaitGroup, addressCh <-chan string, resultCh chan<- PositionResult) {
	defer wait.Done()
	for address := range addressCh {
		if err := h.rateLimiter.Wait(h.ctx); err != nil {
			return
		}
		lg := logger.With("address", address)
		lg.Info("start")
		positions, err := h.getPositions(lg, address)
		if err != nil {
			lg.Error("getPositions: " + err.Error())
			continue
		}
		resultCh <- PositionResult{Address: address, Positions: positions}
	}
}

func (h *HyperliquidPositionWorker) updateSetPositions(setID int) error {
	set, err := dao.GetAddressSet(setID)
	if err != nil {
		return fmt.Errorf("GetAddressSet: %w", err)
	}
	logger := h.logger.With("worker", "updatePositions", "setID", setID)
	startTime := time.Now()
	logger.Info("start", "count", len(set.Addresses))
	defer func() {
		logger.Info("done", "cost", time.Since(startTime))
	}()
	wait := &sync.WaitGroup{}
	addressCh := make(chan string, len(set.Addresses))
	resultCh := make(chan PositionResult, len(set.Addresses))
	for i := 0; i < h.cfg.WorkerCount; i++ {
		wait.Add(1)
		go h.collectPositions(logger, wait, addressCh, resultCh)
	}
	for _, address := range set.Addresses {
		addressCh <- address
	}
	close(addressCh)
	positions := map[string][]dao.Position{}
	wait.Wait()
	close(resultCh)
	for result := range resultCh {
		positions[result.Address] = result.Positions
	}
	data, err := json.Marshal(positions)
	if err != nil {
		return fmt.Errorf("Marshal: %w", err)
	}
	return dao.SetVar(fmt.Sprintf("positions_%d", setID), string(data), "持仓数据")
}

func (h *HyperliquidPositionWorker) updatePositions() {
	coins, err := dao.GetCoins()
	if err != nil {
		h.logger.Error("GetCoins: " + err.Error())
		return
	}
	h.coins = coins
	for _, setID := range h.cfg.UpdatePositionSets {
		err := h.updateSetPositions(setID)
		if err != nil {
			h.logger.Error("updateSetPositions: " + err.Error())
		}
	}
}

func (h *HyperliquidPositionWorker) StartUpdatePositions() {
	tk := time.NewTicker(time.Duration(h.cfg.UpdatePositionInterval) * time.Second)
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-tk.C:
			h.updatePositions()
		}
	}
}

