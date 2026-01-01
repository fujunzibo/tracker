package dao

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type Summary struct {
	Time   time.Time `json:"time"`
	Equity float64   `json:"equity"`
	Pnl    float64   `json:"pnl"`
}

func RefreshAllSummary(worker int) error {
	query := "SELECT time FROM portfolio_times ORDER BY time"
	rows, err := QueryWithTimeout(time.Minute, query)
	if err != nil {
		return fmt.Errorf("Query %s: %w", query, err)
	}
	defer rows.Close()
	wg := sync.WaitGroup{}
	wg.Add(worker)
	ch := make(chan time.Time, worker)
	for i := 0; i < worker; i++ {
		go func() {
			defer wg.Done()
			for ti := range ch {
				start := time.Now()
				err := ExecWithTimeout(0, "SELECT refresh_summary($1::timestamptz)", ti)
				if err != nil {
					slog.Error(fmt.Sprintf("refresh_summary: %s, time: %s", err.Error(), ti.Format(time.RFC3339)))
				} else {
					slog.Info("refresh_summary success", "time", ti.Format(time.RFC3339), "cost", time.Since(start))
				}
			}
		}()
	}
	for rows.Next() {
		var time time.Time
		err := rows.Scan(&time)
		if err != nil {
			return fmt.Errorf("Scan: %w", err)
		}
		ch <- time
	}
	close(ch)
	wg.Wait()
	return nil
}
