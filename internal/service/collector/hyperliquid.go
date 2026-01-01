package collector

import (
	"context"
	"errors"
	"fmt"
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"hyper-tracker/pkg/hyper"
	"hyper-tracker/pkg/utils"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"
)

const (
	HyperLastTimeVar = "hyper_last_time"
	HyperLastAddrVar = "hyper_last_addr"
)

// DataResult 表示单个用户的Portfolio采集结果
type DataResult struct {
	User      string
	Portfolio []hyper.PortfolioItem
	Fills     []*dao.Fill
}

// PortfolioWorker 表示Portfolio采集的工作线程
type PortfolioWorker struct {
	client      *hyper.Client
	logger      *slog.Logger
	workerCount int
	queueSize   int
	retryCount  int
	retryDelay  time.Duration
	rateLimiter *utils.RateLimiter
}

// NewPortfolioWorker 创建新的Portfolio采集工作线程
func NewPortfolioWorker(client *hyper.Client, logger *slog.Logger, workerCount, queueSize, retryCount int, retryDelay time.Duration) *PortfolioWorker {
	return &PortfolioWorker{
		client:      client,
		logger:      logger,
		workerCount: workerCount,
		queueSize:   queueSize,
		retryCount:  retryCount,
		retryDelay:  retryDelay,
		rateLimiter: utils.NewRateLimiter(workerCount * 10),
	}
}

func (w *PortfolioWorker) getData(logger *slog.Logger, addr string, out chan<- DataResult, coins []string) {
	logger = logger.With("address", addr)
	var portfolio []hyper.PortfolioItem
	var fills []*dao.Fill
	var err error
	for i := 0; i < w.retryCount; i++ {
		portfolio, err = w.client.GetPortfolio(addr)
		if err != nil {
			logger.Error("get portfolio failed: "+err.Error(), "retry", i)
			time.Sleep(w.retryDelay)
			continue
		}
		break
	}
	if err == nil {
		fills, err = w.getFills(logger, addr, coins)
		if err != nil {
			logger.Error("get fills failed: " + err.Error())
		}
		out <- DataResult{User: addr, Portfolio: portfolio, Fills: fills}
	} else {
		logger.Error("get portfolio failed for all retries")
	}
}

// CollectData 并发采集多个用户的数据
func (w *PortfolioWorker) CollectData(ctx context.Context, addressCh <-chan string, coins []string) <-chan DataResult {
	resultCh := make(chan DataResult, w.queueSize)
	var wg sync.WaitGroup

	for i := 0; i < w.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer func() {
				wg.Done()
				if err := recover(); err != nil {
					w.logger.Error(fmt.Sprintf("CollectPortfolios panic: %v", err))
				}
			}()
			logger := w.logger.With("worker", workerID)
			logger.Info("Start")

			for {
				select {
				case addr, ok := <-addressCh:
					if !ok {
						logger.Info("address data channel closed, worker exit")
						return
					}
					if err := w.rateLimiter.Wait(ctx); err != nil {
						return
					}
					w.getData(logger, addr, resultCh, coins)

				case <-ctx.Done():
					logger.Info("context done, worker exit")
					return
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(resultCh)
		w.logger.Info("All portfolio collection threads completed")
	}()

	return resultCh
}

func formatFills(fills []hyper.Fill, address string, coins []string) ([]*dao.Fill, error) {
	filterCoin := len(coins) > 0
	result := make([]*dao.Fill, 0)
	fillMap := make(map[int64]*dao.Fill)
	for _, fill := range fills {
		if filterCoin && !slices.Contains(coins, fill.Coin) {
			continue
		}
		price, err := fill.Price.Float64()
		if err != nil {
			return nil, fmt.Errorf("parse price: %w", err)
		}
		size, err := fill.Size.Float64()
		if err != nil {
			return nil, fmt.Errorf("parse size: %w", err)
		}
		fee, err := fill.Fee.Float64()
		if err != nil {
			return nil, fmt.Errorf("parse fee: %w", err)
		}
		var pnl float64
		if fill.Direction == hyper.FillDirCloseLong || fill.Direction == hyper.FillDirCloseShort || fill.Direction == hyper.FillDirShort2Long || fill.Direction == hyper.FillDirLong2Short {
			var err error
			pnl, err = fill.ClosedPnl.Float64()
			if err != nil {
				return nil, fmt.Errorf("parse pnl: %w", err)
			}
		}
		fi, ok := fillMap[fill.OrderID]
		if ok {
			newSize := fi.Size + size
			fi.Price = (fi.Price*fi.Size + price*size) / newSize
			fi.Size = newSize
			fi.Pnl += pnl
			fi.Fee += fee
			continue
		}
		fi = &dao.Fill{
			TradeID: fill.TradeID,
			OrderID: fill.OrderID,
			Address: address,
			Coin:    fill.Coin,
			Price:   price,
			Size:    size,
			Dir:     fill.Direction,
			Pnl:     pnl,
			Fee:     fee,
			Time:    time.Unix(fill.Time/1000, 0).Local(),
		}
		result = append(result, fi)
	}
	return result, nil
}

func (w *PortfolioWorker) getFills(logger *slog.Logger, address string, coins []string) ([]*dao.Fill, error) {
	var fills []hyper.Fill
	var err error
	for i := 0; i < w.retryCount; i++ {
		fills, err = w.client.GetUserFills(address)
		if err == nil {
			break
		}
		logger.Error("get user fills: "+err.Error(), "retry", i)
		time.Sleep(w.retryDelay)
		continue
	}

	if err != nil {
		return nil, fmt.Errorf("get user fills: %w", err)
	}
	return formatFills(fills, address, coins)
}

// CollectFills 并发采集多个用户的成交记录
func (w *PortfolioWorker) CollectFills(ctx context.Context, addressCh <-chan string, coins []string) <-chan []*dao.Fill {
	resultCh := make(chan []*dao.Fill, w.queueSize)
	var wg sync.WaitGroup

	for i := 0; i < w.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer func() {
				wg.Done()
				if err := recover(); err != nil {
					w.logger.Error(fmt.Sprintf("CollectFills panic: %v", err))
				}
			}()
			logger := w.logger.With("worker", workerID)
			logger.Info("Start")

			for {
				select {
				case address, ok := <-addressCh:
					if !ok {
						logger.Info("address data channel closed, worker exit")
						return
					}
					if err := w.rateLimiter.Wait(ctx); err != nil {
						return
					}
					fills, err := w.getFills(logger, address, coins)
					if err != nil {
						logger.Error("get fills: " + err.Error())
						continue
					}
					resultCh <- fills

				case <-ctx.Done():
					logger.Info("context done, worker exit")
					return
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(resultCh)
		w.logger.Info("All fills collection threads completed")
	}()

	return resultCh
}

type Hyperliquid struct {
	ctx            context.Context
	logger         *slog.Logger
	cfg            *config.CollectConfig
	client         *hyper.Client
	worker         *PortfolioWorker
	positionWorker *HyperliquidPositionWorker
}

func NewHyperliquid(ctx context.Context, collectConfig *config.CollectConfig, networkConfig *config.NetworkConfig, excludeCoins []string) *Hyperliquid {
	client := hyper.WithLocalAddrsAndProxies(networkConfig.LocalAddrs, networkConfig.Proxies)
	logger := slog.With("service", "Hyperliquid")
	return &Hyperliquid{
		ctx:            ctx,
		logger:         logger,
		cfg:            collectConfig,
		client:         client,
		worker:         NewPortfolioWorker(client, logger, collectConfig.WorkerCount, collectConfig.QueueSize, collectConfig.RetryCount, time.Duration(collectConfig.RetryDelay)*time.Millisecond),
		positionWorker: NewHyperliquidPositionWorker(ctx, collectConfig, client, logger, excludeCoins),
	}
}

func cutPoints(points []hyper.Point, lastTime int64) []hyper.Point {
	for i, point := range points {
		if point.Time >= lastTime {
			return points[i:]
		}
	}
	return points
}

// TimeBucket 将时间戳按照指定的间隔进行分桶
func TimeBucket(ts int64, interval int64) time.Time {
	bucketStart := (ts / interval) * interval
	return time.UnixMilli(bucketStart).Local()
}

func formatPoints(points []hyper.Point, interval int64) []hyper.Point {
	times := make(map[int64]struct{})
	var result []hyper.Point
	for i := range points {
		point := points[i]
		point.Time = TimeBucket(point.Time, interval).UnixMilli()
		if _, ok := times[point.Time]; !ok {
			times[point.Time] = struct{}{}
			result = append(result, point)
		} else {
			result[len(result)-1].Value = point.Value
		}
	}
	return result
}

// MergePoints 合并两个时间序列的所有时间点
func MergePoints(logger *slog.Logger, equity, pnl []hyper.Point, address string) []dao.Portfolio {
	if len(equity) == 0 || len(pnl) == 0 {
		return nil
	}

	logger = logger.With("address", address)

	var result []dao.Portfolio

	timePoints := make(map[int64]struct{})
	for _, p := range equity {
		timePoints[p.Time] = struct{}{}
	}
	for _, p := range pnl {
		timePoints[p.Time] = struct{}{}
	}

	times := make([]int64, 0, len(timePoints))
	for t := range timePoints {
		times = append(times, t)
	}
	slices.Sort(times)

	equityMap := make(map[int64]float64)
	for _, p := range equity {
		val, err := p.Value.Float64()
		if err != nil {
			logger.Error("parse equity value: " + err.Error())
		}
		equityMap[p.Time] = val
	}

	pnlMap := make(map[int64]float64)
	for _, p := range pnl {
		val, err := p.Value.Float64()
		if err != nil {
			logger.Error("parse pnl value: " + err.Error())
		}
		pnlMap[p.Time] = val
	}

	var lastEquity, lastPnl float64
	if len(times) > 0 {
		firstTime := time.UnixMilli(times[0]).Local()
		pre, err := dao.GetPreNoZeroPortfolio(address, firstTime)
		if err == nil && pre != nil {
			lastEquity = pre.Equity
			lastPnl = pre.Pnl
		} else if err != nil {
			if !errors.Is(err, dao.ErrNoRows) {
				logger.Warn("获取前一个portfolio失败: " + err.Error())
			}
		}
	}

	for _, t := range times {
		e, ok := equityMap[t]
		if !ok || e == 0 {
			e = lastEquity
		} else {
			lastEquity = e
		}

		p, ok := pnlMap[t]
		if !ok || p == 0 {
			p = lastPnl
		} else {
			lastPnl = p
		}

		result = append(result, dao.Portfolio{
			Time:    time.UnixMilli(t).Local(),
			Address: address,
			Equity:  e,
			Pnl:     p,
		})
	}

	return result
}

func selectSeries(result *DataResult, lastTime int64, interval int64) ([]hyper.Point, []hyper.Point) {
	if len(result.Portfolio) == 0 {
		return nil, nil
	}
	items := map[string]hyper.PortfolioItem{}
	for _, item := range result.Portfolio {
		items[item.Type] = item
	}
	avPoints := items["allTime"].Series.AccountValueHistory
	equity := cutPoints(avPoints, lastTime)
	equity = formatPoints(equity, interval)

	pnlPoints := items["perpAllTime"].Series.PnlHistory
	pnl := cutPoints(pnlPoints, lastTime)
	pnl = formatPoints(pnl, interval)

	return equity, pnl
}

func (h *Hyperliquid) getPortfolioVolume(portfolio *DataResult) *dao.PortfolioVolume {
	var volume dao.PortfolioVolume
	var err error
	logger := h.logger.With("address", portfolio.User)
	for _, item := range portfolio.Portfolio {
		switch item.Type {
		case "perpWeek":
			volume.Vol7d, err = item.Series.Vlm.Float64()
			if err != nil {
				logger.Error("parse vol7d: " + err.Error())
			}
		case "perpMonth":
			volume.Vol30d, err = item.Series.Vlm.Float64()
			if err != nil {
				logger.Error("parse vol30d: " + err.Error())
			}
		case "allTime":
			volume.VolAll, err = item.Series.Vlm.Float64()
			if err != nil {
				logger.Error("parse volAll: " + err.Error())
			}
		}
	}
	volume.Address = portfolio.User
	volume.UpdateTime = time.Now()
	return &volume
}

var ErrEmptyPortfolio = errors.New("empty portfolio")

func (h *Hyperliquid) savePortfolio(result *DataResult, lastTime int64) error {
	if len(result.Portfolio) == 0 {
		return ErrEmptyPortfolio
	}

	equity, pnl := selectSeries(result, lastTime, h.cfg.PointInterval*1000)
	if len(equity) == 0 {
		return ErrEmptyPortfolio
	}

	portfolios := MergePoints(h.logger, equity, pnl, result.User)

	return dao.BatchSavePortfolio(portfolios)
}

func (h *Hyperliquid) updateDataWorker(ch <-chan DataResult) {
	lastTimeVal, err := dao.GetVarWithDefault(HyperLastTimeVar, "0")
	var lastTime int64
	if err == nil {
		lastTime, err = strconv.ParseInt(lastTimeVal, 10, 64)
		if err != nil {
			h.logger.Error("parse hyper last time: " + err.Error())
		} else {
			lastTime -= 86400000 // 提前一天，防止漏数据
		}
	}
	if lastTime == 0 {
		lastTime = time.Now().Add(-24 * time.Hour * 365).UnixMilli()
	}
	dur := time.Duration(h.cfg.SaveInterval) * time.Second
	timeout := time.NewTimer(dur)
	getRows := func() ([]DataResult, bool) {
		rows := make([]DataResult, 0)
		timeout.Reset(dur)
		for {
			select {
			case <-timeout.C:
				return rows, false
			case portfolio, ok := <-ch:
				if !ok {
					return rows, true
				}
				rows = append(rows, portfolio)
			}
		}
	}

	defer func() {
		if err := recover(); err != nil {
			h.logger.Error(fmt.Sprintf("updatePortfoliosWorker panic: %v", err))
		}
	}()
	startTime := time.Now().Unix()
	var count int
	for {
		rows, done := getRows()
		var updates int
		var volumes []*dao.PortfolioVolume
		for _, r := range rows {
			logger := h.logger.With("address", r.User)
			volumes = append(volumes, h.getPortfolioVolume(&r))
			err := h.savePortfolio(&r, lastTime)
			if err != nil {
				if errors.Is(err, ErrEmptyPortfolio) {
					logger.Debug("empty portfolio")
				} else {
					logger.Error("savePortfolio: " + err.Error())
				}
			}
			if len(r.Fills) > 0 {
				err = dao.BatchSaveFill(r.Fills)
				if err != nil {
					logger.Error("BatchSaveFill: " + err.Error())
				}
			}
			updates++
		}
		if len(volumes) > 0 {
			if err := dao.BatchInsertPortfolioVolume(volumes); err != nil {
				h.logger.Error("BatchInsertPortfolioVolume: " + err.Error())
			}
		}
		if updates > 0 {
			count += updates
			speed := float64(count) / float64(time.Now().Unix()-startTime)
			h.logger.Info("batch update address info", "len", updates, "count", count, "speed", fmt.Sprintf("%.2f", speed))
		}
		if done {
			return
		}
	}
}

func (h *Hyperliquid) exec() {
	logger := slog.With("service", "Hyperliquid")
	startTime := time.Now()
	lastAddr, err := dao.GetVarWithDefault(HyperLastAddrVar, "")
	if err != nil {
		logger.Error("GetVarWithDefault: " + err.Error())
		return
	}
	coins, err := dao.GetCoins()
	if err != nil {
		logger.Error("GetCoins: " + err.Error())
		return
	}
	in, err := dao.GetAddressChannel(logger, lastAddr, 0, h.cfg.QueueSize)
	if err != nil {
		logger.Error("GetAddress: " + err.Error())
		return
	}
	out := h.worker.CollectData(h.ctx, in, coins)
	h.updateDataWorker(out)

	err = dao.SetVar(HyperLastAddrVar, "", "")
	if err != nil {
		logger.Error("SetVar HyperLastAddrVar: " + err.Error())
	}
	err = dao.SetVar(HyperLastTimeVar, fmt.Sprintf("%d", time.Now().UnixMilli()), "")
	if err != nil {
		logger.Error("SetVar HyperLastTimeVar: " + err.Error())
	}

	logger.Info("done", "cost", time.Since(startTime))
}

func (h *Hyperliquid) Start() {
	go h.positionWorker.StartUpdatePositions()

	c, resetTimer, saveNextTime := timer(h.cfg.BatchInterval, dao.HyperNextTimeVar)
	resetTimer()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-c:
			h.exec()
			saveNextTime()
			resetTimer()
			RefreshExec(h.cfg.HyperExecs)
		}
	}
}

func (h *Hyperliquid) updateFillsWorker(logger *slog.Logger, ch <-chan []*dao.Fill) {
	dur := time.Duration(h.cfg.SaveInterval) * time.Second
	timeout := time.NewTimer(dur)
	var count int
	getFills := func() ([]*dao.Fill, bool) {
		fills := make([]*dao.Fill, 0)
		timeout.Reset(dur)
		for {
			select {
			case <-timeout.C:
				return fills, false
			case fis, ok := <-ch:
				if !ok {
					return fills, true
				}
				count++
				fills = append(fills, fis...)
			}
		}
	}
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("updateFills panic: %v", err))
		}
	}()
	startTime := time.Now().Unix()
	for {
		fills, done := getFills()
		size := len(fills)
		if size == 0 {
			continue
		}
		speed := float64(count) / float64(time.Now().Unix()-startTime)
		logger.Info("batch save fill", "len", size, "count", count, "speed", fmt.Sprintf("%.2f", speed))
		err := dao.BatchSaveFill(fills)
		if err != nil {
			logger.Error("BatchSaveFill: " + err.Error())
		}
		if done {
			return
		}
	}
}

func (h *Hyperliquid) UpdateAddressSetFills(setID int) {
	logger := slog.With("service", "Hyperliquid")
	set, err := dao.GetAddressSet(setID)
	if err != nil {
		logger.Error("GetAddressSet: " + err.Error())
		return
	}
	coins, err := dao.GetCoins()
	if err != nil {
		logger.Error("GetCoins: " + err.Error())
		return
	}
	startTime := time.Now()
	ch := make(chan string, len(set.Addresses))
	out := h.worker.CollectFills(h.ctx, ch, coins)
	for _, address := range set.Addresses {
		ch <- address
	}
	close(ch)
	set = nil
	h.updateFillsWorker(logger, out)
	logger.Info("done", "cost", time.Since(startTime))
}

func (h *Hyperliquid) FixZeroPortfolio(start time.Time) {
	logger := h.logger.With("worker", "FixZeroPortfolio")
	ch, err := dao.GetAllZeroPortfolio(start)
	if err != nil {
		logger.Error("GetAllZeroPortfolio: " + err.Error())
		return
	}
	startTime := time.Now()
	var count int
	var total int
	var updates []dao.Portfolio
	execUpdate := func() {
		err := dao.BatchSavePortfolio(updates)
		if err != nil {
			logger.Error("BatchSavePortfolio: " + err.Error())
			return
		}
		count += len(updates)
		updates = make([]dao.Portfolio, 0)
		speed := float64(count) / time.Since(startTime).Seconds()
		logger.Info("batch save portfolio", "count", count, "speed", fmt.Sprintf("%.2f", speed))
	}
	for portfolio := range ch {
		total++
		if total%1000 == 0 {
			logger.Info("total", "total", total, "cost", time.Since(startTime))
		}
		pre, err := dao.GetPreNoZeroPortfolio(portfolio.Address, portfolio.Time)
		if err != nil {
			if !errors.Is(err, dao.ErrNoRows) {
				logger.Error("GetPreNoZeroPortfolio: " + err.Error())
			}
			continue
		}
		if pre.Equity == 0 || pre.Pnl == 0 {
			logger.Error("pre portfolio is zero", "address", portfolio.Address, "time", portfolio.Time)
		}
		if portfolio.Equity == 0 {
			portfolio.Equity = pre.Equity
		}
		if portfolio.Pnl == 0 {
			portfolio.Pnl = pre.Pnl
		}
		updates = append(updates, *portfolio)
		if len(updates) >= 100 {
			execUpdate()
		}
	}
	if len(updates) > 0 {
		execUpdate()
	}
	logger.Info("done", "cost", time.Since(startTime))
}
