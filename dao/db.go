package dao

import (
	"context"
	"fmt"
	"hyper-tracker/pkg/lifecycle"
	"hyper-tracker/pkg/utils"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	TableVars                = "vars"
	TableAddress             = "address"
	TablePortfolio           = "portfolio"
	TablePortfolioVolume     = "portfolio_volume"
	TableSummary             = "summary"
	TableSkipAddress         = "skip_address"
	TablePosition            = "position"
	TableFill                = "hyper_fills"
	TableFollowUserCoin      = "follow_user_coin"
	TableFollowTrade         = "follow_trade"
	TableFollowIncome        = "follow_income"
	TableFollowUserDailyStat = "follow_user_daily_stat"
	TableUserQysy            = "tb_user_qysy"

	CMMNextTimeVar   = "cmm_next_time"
	HyperNextTimeVar = "hyper_next_time"
	CoinsVar         = "coins"
)

var db *pgxpool.Pool

var ErrNoRows = pgx.ErrNoRows

// Tx 数据库事务包装
type Tx struct {
	pgx.Tx
}

// WithTransaction 执行数据库事务
func WithTransaction(fn func(*Tx) error) error {
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}

	// 包装事务对象
	wrappedTx := &Tx{Tx: tx}

	// 执行业务逻辑
	if err := fn(wrappedTx); err != nil {
		// 回滚事务
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("执行失败: %v, 回滚失败: %w", err, rbErr)
		}
		return err
	}

	// 提交事务
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	return nil
}

// InitDB 初始化数据库
func InitDB(ctx context.Context, dsn string) error {
	pool, err := pgxpool.New(context.Background(), dsn)
	utils.FatalOnError(err, "初始化数据库连接池失败")
	err = pool.Ping(ctx)
	utils.FatalOnError(err, "数据库连接失败")
	lifecycle.RegisterShutdownNoErrorHandler(pool.Close)
	db = pool
	return nil
}

func QueryWithTimeout(timeout time.Duration, query string, args ...any) (pgx.Rows, error) {
	if timeout == 0 {
		return db.Query(context.Background(), query, args...)
	}
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	return db.Query(ctx, query, args...)
}

func QueryRowWithTimeout(timeout time.Duration, query string, args ...any) pgx.Row {
	if timeout == 0 {
		return db.QueryRow(context.Background(), query, args...)
	}
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	return db.QueryRow(ctx, query, args...)
}

func ExecWithTimeout(timeout time.Duration, query string, args ...any) error {
	var ctx context.Context
	if timeout == 0 {
		ctx = context.Background()
	} else {
		ctx, _ = context.WithTimeout(context.Background(), timeout)
	}
	_, err := db.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("Exec %s: %w", query, err)
	}
	return nil
}
