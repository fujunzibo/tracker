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
	// 只有当用户权益为0且用户损益(累计亏损)也为0时，才不写入数据库
	if data.UserEquity == 0 && data.TotalLoss == 0 {
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
	
	// 过滤掉用户权益为0且用户损益(累计亏损)也为0的数据
	var validDataList []*UserQysy
	for _, data := range dataList {
		if !(data.UserEquity == 0 && data.TotalLoss == 0) {
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

// AddressWithEquity 地址和权益信息
type AddressWithEquity struct {
	Address     string
	TotalEquity float64
	PrepPnl     float64 // 合约盈亏
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

// GetAllAddressesWithEquity 获取所有地址及其权益
func GetAllAddressesWithEquity() ([]AddressWithEquity, error) {
	query := fmt.Sprintf(`SELECT address, total_equity, prep_pnl FROM %s ORDER BY address`, TableAddress)
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("查询地址失败: %w", err)
	}
	defer rows.Close()
	
	var addresses []AddressWithEquity
	for rows.Next() {
		var addr AddressWithEquity
		if err := rows.Scan(&addr.Address, &addr.TotalEquity, &addr.PrepPnl); err != nil {
			return nil, fmt.Errorf("扫描地址失败: %w", err)
		}
		addresses = append(addresses, addr)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历地址失败: %w", err)
	}
	
	return addresses, nil
}

// CalculateUserQysyFromDB 从数据库 hyper_fills 表计算用户权益损益数据（备用方案）
func CalculateUserQysyFromDB(address string, totalEquity float64, prepPnl float64) (*UserQysy, error) {
	data := &UserQysy{
		Address:    address,
		UserEquity: totalEquity,
		UpdatedAt:  time.Now(),
	}

	// 计算7天前的时间
	sevenDaysAgo := time.Now().AddDate(0, 0, -7)

	// NUMERIC(32, 12) 最大值为 9999999999999999999999999999.999999999999
	const maxEquity = 9999999999999999999999999999.999999999999
	// NUMERIC(18, 6) 最大值为 999999999999.999999
	const maxRatio = 999999999999.0

	// 查询所有交易记录
	query := fmt.Sprintf(`
		SELECT 
			COALESCE(SUM(CASE WHEN pnl < 0 THEN ABS(pnl) ELSE 0 END), 0) as total_loss,
			COALESCE(SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END), 0) as total_profit,
			COALESCE(SUM(ABS(fee)), 0) as total_fee,
			COUNT(*) FILTER (WHERE time >= $2) as trade_count_7d
		FROM %s 
		WHERE address = $1
	`, TableFill)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var totalLoss, totalProfit, totalFee float64
	var tradeCount7d int

	err := db.QueryRow(ctx, query, address, sevenDaysAgo).Scan(&totalLoss, &totalProfit, &totalFee, &tradeCount7d)
	if err != nil {
		// 如果没有交易记录，根据 prep_pnl 计算 total_loss
		if err == pgx.ErrNoRows {
			var calculatedLoss float64
			if prepPnl < 0 {
				// 如果 prep_pnl < 0（亏损），则 total_loss = -(prep_pnl)
				calculatedLoss = -prepPnl
			} else {
				// 如果 prep_pnl > 0（盈利），则 total_loss = 0
				calculatedLoss = 0
			}
			if calculatedLoss > maxEquity {
				calculatedLoss = maxEquity
			}
			data.TotalLoss = calculatedLoss
			return data, nil
		}
		return nil, fmt.Errorf("查询交易记录失败: %w", err)
	}

	// 限制值不超过最大值
	if totalLoss > maxEquity {
		totalLoss = maxEquity
	}
	if totalProfit > maxEquity {
		totalProfit = maxEquity
	}
	if totalFee > maxEquity {
		totalFee = maxEquity
	}

	// 1. 用户损益(累计亏损)
	// 如果从 hyper_fills 计算出的 total_loss 为 0，则根据 prep_pnl 计算
	if totalLoss == 0 {
		var calculatedLoss float64
		if prepPnl < 0 {
			// 如果 prep_pnl < 0（亏损），则 total_loss = -(prep_pnl)
			calculatedLoss = -prepPnl
		} else {
			// 如果 prep_pnl > 0（盈利），则 total_loss = 0
			calculatedLoss = 0
		}
		if calculatedLoss > maxEquity {
			calculatedLoss = maxEquity
		}
		data.TotalLoss = calculatedLoss
	} else {
		data.TotalLoss = totalLoss
	}

	// 2. 整体盈亏比(所有盈利订单累计/所有亏损订单累计)
	if totalLoss > 0 {
		ratio := totalProfit / totalLoss
		if ratio > maxRatio {
			data.ProfitLossRatio = maxRatio
		} else if ratio < 0 {
			data.ProfitLossRatio = 0
		} else {
			data.ProfitLossRatio = ratio
		}
	} else if totalProfit > 0 {
		// 如果只有盈利没有亏损，设为最大允许值
		data.ProfitLossRatio = maxRatio
	} else {
		data.ProfitLossRatio = 0
	}

	// 3. 手续费占比(手续费累计/总亏损)
	if totalLoss > 0 {
		ratio := totalFee / totalLoss
		if ratio > maxRatio {
			data.FeeRatio = maxRatio
		} else {
			data.FeeRatio = ratio
		}
	} else {
		data.FeeRatio = 0
	}

	// 4. 7日内平均交易次数
	const maxCount = 999999999999.999999
	avgCount := float64(tradeCount7d) / 7.0
	if avgCount > maxCount {
		data.AvgTradeCount7d = maxCount
	} else {
		data.AvgTradeCount7d = avgCount
	}

	return data, nil
}
