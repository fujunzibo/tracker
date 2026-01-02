package dao

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// UserQysy 用户权益损益数据
type UserQysy struct {
	Address          string    `db:"address"`           // 钱包地址
	UserEquity       float64   `db:"user_equity"`       // 用户权益
	TotalLoss        float64   `db:"total_loss"`         // 用户损益(累计亏损)
	ProfitLossRatio  float64   `db:"profit_loss_ratio"` // 整体盈亏比(所有盈利订单累计/所有亏损订单累计)
	FeeRatio         float64   `db:"fee_ratio"`         // 手续费占比(手续费累计/总亏损)
	AvgTradeCount7d  float64   `db:"avg_trade_count_7d"` // 7日内平均交易次数
	UpdatedAt        time.Time `db:"updated_at"`        // 更新时间
}

// CreateUserQysyTable 创建用户权益损益表
func CreateUserQysyTable() error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			address VARCHAR(42) NOT NULL PRIMARY KEY,
			user_equity NUMERIC(32, 12) NOT NULL DEFAULT 0,
			total_loss NUMERIC(32, 12) NOT NULL DEFAULT 0,
			profit_loss_ratio NUMERIC(18, 6) NOT NULL DEFAULT 0,
			fee_ratio NUMERIC(18, 6) NOT NULL DEFAULT 0,
			avg_trade_count_7d NUMERIC(18, 6) NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE INDEX IF NOT EXISTS idx_user_qysy_updated_at ON %s (updated_at);
	`, TableUserQysy, TableUserQysy)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	_, err := db.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("创建表失败: %w", err)
	}
	return nil
}

// SaveUserQysy 保存或更新用户权益损益数据
func SaveUserQysy(data *UserQysy) error {
	// 用户权益为0的数据不写入数据库
	if data.UserEquity == 0 {
		return nil
	}
	
	query := fmt.Sprintf(`
		INSERT INTO %s (address, user_equity, total_loss, profit_loss_ratio, fee_ratio, avg_trade_count_7d, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (address) DO UPDATE SET
			user_equity = EXCLUDED.user_equity,
			total_loss = EXCLUDED.total_loss,
			profit_loss_ratio = EXCLUDED.profit_loss_ratio,
			fee_ratio = EXCLUDED.fee_ratio,
			avg_trade_count_7d = EXCLUDED.avg_trade_count_7d,
			updated_at = EXCLUDED.updated_at
	`, TableUserQysy)
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	_, err := db.Exec(ctx, query,
		data.Address,
		data.UserEquity,
		data.TotalLoss,
		data.ProfitLossRatio,
		data.FeeRatio,
		data.AvgTradeCount7d,
		data.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("保存数据失败: %w", err)
	}
	return nil
}

// BatchSaveUserQysy 批量保存用户权益损益数据
func BatchSaveUserQysy(dataList []*UserQysy) error {
	if len(dataList) == 0 {
		return nil
	}
	
	// 过滤掉用户权益为0的数据
	var validDataList []*UserQysy
	for _, data := range dataList {
		if data.UserEquity != 0 {
			validDataList = append(validDataList, data)
		}
	}
	
	if len(validDataList) == 0 {
		return nil
	}
	
	query := fmt.Sprintf(`
		INSERT INTO %s (address, user_equity, total_loss, profit_loss_ratio, fee_ratio, avg_trade_count_7d, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (address) DO UPDATE SET
			user_equity = EXCLUDED.user_equity,
			total_loss = EXCLUDED.total_loss,
			profit_loss_ratio = EXCLUDED.profit_loss_ratio,
			fee_ratio = EXCLUDED.fee_ratio,
			avg_trade_count_7d = EXCLUDED.avg_trade_count_7d,
			updated_at = EXCLUDED.updated_at
	`, TableUserQysy)
	
	batch := &pgx.Batch{}
	for _, data := range validDataList {
		batch.Queue(query,
			data.Address,
			data.UserEquity,
			data.TotalLoss,
			data.ProfitLossRatio,
			data.FeeRatio,
			data.AvgTradeCount7d,
			data.UpdatedAt,
		)
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	batchResults := db.SendBatch(ctx, batch)
	defer batchResults.Close()
	
	for i := 0; i < len(validDataList); i++ {
		_, err := batchResults.Exec()
		if err != nil {
			// 输出详细错误信息，包括具体的数据值
			data := validDataList[i]
			return fmt.Errorf("批量保存失败 [%d] address=%s user_equity=%f total_loss=%f profit_loss_ratio=%f fee_ratio=%f avg_trade_count_7d=%f: %w", 
				i, data.Address, data.UserEquity, data.TotalLoss, data.ProfitLossRatio, data.FeeRatio, data.AvgTradeCount7d, err)
		}
	}
	return nil
}

// GetAllAddresses 获取所有地址
func GetAllAddresses() ([]string, error) {
	query := fmt.Sprintf(`SELECT DISTINCT address FROM %s ORDER BY address`, TableAddress)
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("查询地址失败: %w", err)
	}
	defer rows.Close()
	
	var addresses []string
	for rows.Next() {
		var address string
		if err := rows.Scan(&address); err != nil {
			return nil, fmt.Errorf("扫描地址失败: %w", err)
		}
		addresses = append(addresses, address)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历地址失败: %w", err)
	}
	
	return addresses, nil
}
