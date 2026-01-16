package dao

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// SubQueryData SubQuery 数据
type SubQueryData struct {
	Address       string    `db:"address"`         // 钱包地址（主键）
	FromAddress   string    `db:"from_address"`    // 发送者地址
	ToAddress     string    `db:"to_address"`      // 接收者地址
	BlockNumber   int64     `db:"block_number"`    // 区块高度
	BlockTime     time.Time `db:"block_time"`      // 区块时间戳
	TransactionHash string  `db:"tx_hash"`         // 交易哈希
	LogIndex      int       `db:"log_index"`       // 日志索引
	EventSignature string   `db:"event_sig"`       // 事件签名
	EventData     string    `db:"event_data"`      // 事件数据（JSON格式）
	Topics        string    `db:"topics"`          // Topics（JSON格式）
	CreatedAt     time.Time `db:"created_at"`      // 创建时间
	UpdatedAt     time.Time `db:"updated_at"`      // 更新时间
}

const TableSubQuery = "tb_subquery"

// CreateSubQueryTable 创建 SubQuery 表
func CreateSubQueryTable() error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			address VARCHAR(42) NOT NULL,
			from_address VARCHAR(42) NOT NULL,
			to_address VARCHAR(42) NOT NULL,
			block_number BIGINT NOT NULL,
			block_time TIMESTAMPTZ NOT NULL,
			tx_hash VARCHAR(66) NOT NULL,
			log_index INT NOT NULL,
			event_sig VARCHAR(66),
			event_data TEXT,
			topics TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (address, tx_hash, log_index)
		);
		
		CREATE INDEX IF NOT EXISTS idx_subquery_from_address ON %s (from_address);
		CREATE INDEX IF NOT EXISTS idx_subquery_to_address ON %s (to_address);
		CREATE INDEX IF NOT EXISTS idx_subquery_block_number ON %s (block_number);
		CREATE INDEX IF NOT EXISTS idx_subquery_tx_hash ON %s (tx_hash);
		CREATE INDEX IF NOT EXISTS idx_subquery_block_time ON %s (block_time);
		CREATE INDEX IF NOT EXISTS idx_subquery_created_at ON %s (created_at);
	`, TableSubQuery, TableSubQuery, TableSubQuery, TableSubQuery, TableSubQuery, TableSubQuery, TableSubQuery)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	_, err := db.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("创建表失败: %w", err)
	}
	return nil
}

// BatchSaveSubQueryData 批量保存 SubQuery 数据
func BatchSaveSubQueryData(dataList []*SubQueryData) error {
	if len(dataList) == 0 {
		return nil
	}
	
	query := fmt.Sprintf(`
		INSERT INTO %s (address, from_address, to_address, block_number, block_time, tx_hash, log_index, event_sig, event_data, topics, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (address, tx_hash, log_index) DO UPDATE SET
			from_address = EXCLUDED.from_address,
			to_address = EXCLUDED.to_address,
			block_number = EXCLUDED.block_number,
			block_time = EXCLUDED.block_time,
			event_sig = EXCLUDED.event_sig,
			event_data = EXCLUDED.event_data,
			topics = EXCLUDED.topics,
			updated_at = EXCLUDED.updated_at
	`, TableSubQuery)
	
	batch := &pgx.Batch{}
	for _, data := range dataList {
		batch.Queue(query,
			data.Address,
			data.FromAddress,
			data.ToAddress,
			data.BlockNumber,
			data.BlockTime,
			data.TransactionHash,
			data.LogIndex,
			data.EventSignature,
			data.EventData,
			data.Topics,
			data.CreatedAt,
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
			return fmt.Errorf("批量保存失败 [%d] address=%s tx_hash=%s log_index=%d: %w",
				i, data.Address, data.TransactionHash, data.LogIndex, err)
		}
	}
	return nil
}

// GetAllAddressesFromSubQuery 从 tb_subquery 表获取所有唯一的地址
func GetAllAddressesFromSubQuery() ([]string, error) {
	query := fmt.Sprintf(`SELECT DISTINCT address FROM %s ORDER BY address`, TableSubQuery)
	
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
