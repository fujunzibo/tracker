package dao

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// CalculateUserAccountFromDB 从数据库 hyper_fills 表计算用户账户数据（备用方案）
func CalculateUserAccountFromDB(address string, totalEquity float64, prepPnl float64) (*UserAccount, error) {
	data := &UserAccount{
		Address:     address,
		TotalEquity: totalEquity,
		UpdatedAt:   time.Now(),
	}

	// 计算7天前的时间
	sevenDaysAgo := time.Now().AddDate(0, 0, -7)

	const maxValue = 9999999999999999999999999999.999999999999

	// 查询所有交易记录，计算 total_pnl（累计盈亏）
	query := fmt.Sprintf(`
		SELECT 
			COALESCE(SUM(pnl), 0) as total_pnl,
			COALESCE(SUM(ABS(fee)), 0) as total_fee,
			COUNT(*) FILTER (WHERE time >= $2) as trade_count_7d
		FROM %s 
		WHERE address = $1
	`, TableFill)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var totalPnl, totalFee float64
	var tradeCount7d int

	err := db.QueryRow(ctx, query, address, sevenDaysAgo).Scan(&totalPnl, &totalFee, &tradeCount7d)
	if err != nil {
		// 如果没有交易记录，使用 prep_pnl 作为 total_pnl
		if err == pgx.ErrNoRows {
			if prepPnl != 0 {
				data.TotalPnl = prepPnl
			}
			return data, nil
		}
		return nil, fmt.Errorf("查询交易记录失败: %w", err)
	}

	// 限制值不超过最大值
	if totalPnl > maxValue {
		data.TotalPnl = maxValue
	} else if totalPnl < -maxValue {
		data.TotalPnl = -maxValue
	} else {
		data.TotalPnl = totalPnl
	}

	if totalFee > maxValue {
		data.TotalFee = maxValue
	} else {
		data.TotalFee = totalFee
	}

	data.TradeCount7d = tradeCount7d

	return data, nil
}
