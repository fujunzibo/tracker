package dao

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

type Portfolio struct {
	Time    time.Time `json:"time"`
	Address string    `json:"address"`
	Equity  float64   `json:"equity"`
	Pnl     float64   `json:"pnl"`
}

func BatchSavePortfolio(portfolios []Portfolio) error {
	query := fmt.Sprintf("INSERT INTO %s (time, address, equity, pnl) VALUES ($1, $2, $3, $4) ON CONFLICT (address, time) DO UPDATE SET equity = EXCLUDED.equity, pnl = EXCLUDED.pnl", TablePortfolio)
	batch := &pgx.Batch{}
	for _, portfolio := range portfolios {
		batch.Queue(query, portfolio.Time, portfolio.Address, portfolio.Equity, portfolio.Pnl)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	batchResults := db.SendBatch(ctx, batch)
	defer batchResults.Close()
	_, err := batchResults.Exec()
	if err != nil {
		return fmt.Errorf("Exec: %w", err)
	}
	return nil
}

func GetAllZeroPortfolio(start time.Time) (<-chan *Portfolio, error) {
	logger := slog.With("worker", "GetAllZeroPortfolio")
	logger.Info("start")
	query := fmt.Sprintf("SELECT time, address, equity, pnl FROM %s WHERE time >= $1 AND (equity = 0 OR pnl = 0) ORDER BY time ASC", TablePortfolio)
	rows, err := QueryWithTimeout(0, query, start)
	if err != nil {
		return nil, fmt.Errorf("Query %s: %w", query, err)
	}
	ch := make(chan *Portfolio, 100)
	go func() {
		defer rows.Close()
		defer close(ch)
		logger.Info("read rows")
		for rows.Next() {
			var portfolio Portfolio
			err := rows.Scan(&portfolio.Time, &portfolio.Address, &portfolio.Equity, &portfolio.Pnl)
			if err != nil {
				logger.Error(fmt.Sprintf("scan: %v", err))
				break
			}
			ch <- &portfolio
		}
		logger.Info("done")
	}()
	return ch, nil
}

func GetPreNoZeroPortfolio(address string, ti time.Time) (*Portfolio, error) {
	query := fmt.Sprintf("SELECT time, address, equity, pnl FROM %s WHERE address = $1 AND time < $2 AND (equity != 0 AND pnl != 0) ORDER BY time DESC LIMIT 1", TablePortfolio)
	row := QueryRowWithTimeout(time.Second*10, query, address, ti)
	var portfolio Portfolio
	err := row.Scan(&portfolio.Time, &portfolio.Address, &portfolio.Equity, &portfolio.Pnl)
	if err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	return &portfolio, nil
}

type Fill struct {
	TradeID int64     `json:"tradeId"`
	OrderID int64     `json:"orderId"`
	Address string    `json:"address"`
	Coin    string    `json:"coin"`
	Price   float64   `json:"price"`
	Size    float64   `json:"size"`
	Dir     string    `json:"dir"`
	Pnl     float64   `json:"pnl"`
	Fee     float64   `json:"fee"`
	Time    time.Time `json:"time"`
}

func BatchSaveFill(fills []*Fill) error {
	query := fmt.Sprintf("INSERT INTO %s (tid, oid, address, coin, price, size, dir, pnl, fee, time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (tid, time) DO UPDATE SET price = EXCLUDED.price, size = EXCLUDED.size, dir = EXCLUDED.dir, pnl = EXCLUDED.pnl, fee = EXCLUDED.fee", TableFill)
	batch := &pgx.Batch{}
	for _, fill := range fills {
		batch.Queue(query, fill.TradeID, fill.OrderID, fill.Address, fill.Coin, fill.Price, fill.Size, fill.Dir, fill.Pnl, fill.Fee, fill.Time)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	batchResults := db.SendBatch(ctx, batch)
	defer batchResults.Close()
	_, err := batchResults.Exec()
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

// PortfolioVolume 交易量记录
type PortfolioVolume struct {
	Address    string    `json:"address"`
	Vol7d      float64   `json:"vol7d"`
	Vol30d     float64   `json:"vol30d"`
	VolAll     float64   `json:"volAll"`
	UpdateTime time.Time `json:"updateTime"`
}

func BatchInsertPortfolioVolume(volumes []*PortfolioVolume) error {
	batch := &pgx.Batch{}
	for _, volume := range volumes {
		batch.Queue(fmt.Sprintf(`
			INSERT INTO %s (address, vol_7d, vol_30d, vol_all, update_time)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT (address)
			DO UPDATE SET vol_7d = EXCLUDED.vol_7d, vol_30d = EXCLUDED.vol_30d, vol_all = EXCLUDED.vol_all, update_time = EXCLUDED.update_time
		`, TablePortfolioVolume), volume.Address, volume.Vol7d, volume.Vol30d, volume.VolAll, volume.UpdateTime)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	batchResults := db.SendBatch(ctx, batch)
	defer batchResults.Close()
	_, err := batchResults.Exec()
	if err != nil {
		return fmt.Errorf("Exec: %w", err)
	}
	return nil
}

type Position struct {
	Coin      string    `json:"coin"`
	EntryPx   float64   `json:"entryPx"`
	Leverage  int       `json:"leverage"`
	Size      float64   `json:"size"`
	Value     float64   `json:"value"`
	UnrealPnl float64   `json:"unrealPnl"`
	LiqPx     float64   `json:"liqPx"`
	Time      time.Time `json:"time"`
}
