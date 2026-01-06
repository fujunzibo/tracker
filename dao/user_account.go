package dao

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// UserAccount 用户账户数据
type UserAccount struct {
	Address          string    `db:"address"`            // 钱包地址
	TotalEquity      float64   `db:"total_equity"`       // 用户总权益
	AvailableBalance float64   `db:"available_balance"`   // 可用余额
	UsedMargin       float64   `db:"used_margin"`        // 已用保证金
	TotalPnl         float64   `db:"total_pnl"`          // 累计盈亏
	TotalFee         float64   `db:"total_fee"`          // 累计手续费
	TradeCount7d     int       `db:"trade_count_7d"`     // 7日内累计交易次数
	UpdatedAt        time.Time `db:"updated_at"`         // 更新时间
	Source           string    `db:"-"`                  // 数据来源：API 或 QuickNode（不保存到数据库，仅用于日志）
}

// CreateUserAccountTable 创建用户账户表
func CreateUserAccountTable() error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			address VARCHAR(42) NOT NULL PRIMARY KEY,
			total_equity NUMERIC(32, 12) NOT NULL DEFAULT 0,
			available_balance NUMERIC(32, 12) NOT NULL DEFAULT 0,
			used_margin NUMERIC(32, 12) NOT NULL DEFAULT 0,
			total_pnl NUMERIC(32, 12) NOT NULL DEFAULT 0,
			total_fee NUMERIC(32, 12) NOT NULL DEFAULT 0,
			trade_count_7d INT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE INDEX IF NOT EXISTS idx_user_account_updated_at ON %s (updated_at);
	`, TableUserAccount, TableUserAccount)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	_, err := db.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("创建表失败: %w", err)
	}
	return nil
}

// SaveUserAccount 保存或更新用户账户数据
func SaveUserAccount(data *UserAccount) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (address, total_equity, available_balance, used_margin, total_pnl, total_fee, trade_count_7d, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (address) DO UPDATE SET
			total_equity = EXCLUDED.total_equity,
			available_balance = EXCLUDED.available_balance,
			used_margin = EXCLUDED.used_margin,
			total_pnl = EXCLUDED.total_pnl,
			total_fee = EXCLUDED.total_fee,
			trade_count_7d = EXCLUDED.trade_count_7d,
			updated_at = EXCLUDED.updated_at
	`, TableUserAccount)
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	_, err := db.Exec(ctx, query,
		data.Address,
		data.TotalEquity,
		data.AvailableBalance,
		data.UsedMargin,
		data.TotalPnl,
		data.TotalFee,
		data.TradeCount7d,
		data.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("保存数据失败: %w", err)
	}
	return nil
}

// BatchSaveUserAccount 批量保存用户账户数据
func BatchSaveUserAccount(dataList []*UserAccount) error {
	if len(dataList) == 0 {
		return nil
	}
	
	query := fmt.Sprintf(`
		INSERT INTO %s (address, total_equity, available_balance, used_margin, total_pnl, total_fee, trade_count_7d, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (address) DO UPDATE SET
			total_equity = EXCLUDED.total_equity,
			available_balance = EXCLUDED.available_balance,
			used_margin = EXCLUDED.used_margin,
			total_pnl = EXCLUDED.total_pnl,
			total_fee = EXCLUDED.total_fee,
			trade_count_7d = EXCLUDED.trade_count_7d,
			updated_at = EXCLUDED.updated_at
	`, TableUserAccount)
	
	batch := &pgx.Batch{}
	for _, data := range dataList {
		batch.Queue(query,
			data.Address,
			data.TotalEquity,
			data.AvailableBalance,
			data.UsedMargin,
			data.TotalPnl,
			data.TotalFee,
			data.TradeCount7d,
			data.UpdatedAt,
		)
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	batchResults := db.SendBatch(ctx, batch)
	defer batchResults.Close()
	
	for i := 0; i < len(dataList); i++ {
		_, err := batchResults.Exec()
		if err != nil {
			data := dataList[i]
			return fmt.Errorf("批量保存失败 [%d] address=%s: %w", 
				i, data.Address, err)
		}
	}
	return nil
}
