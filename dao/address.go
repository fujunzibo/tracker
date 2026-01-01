package dao

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type AddressInfo struct {
	Address     string    `db:"address"`
	TotalEquity float64   `db:"total_equity"`
	PrepPnl     float64   `db:"prep_pnl"`
	PrepOpen    float64   `db:"prep_open"`
	PrepEquity  float64   `db:"prep_equity"`
	SumUpnl     float64   `db:"sum_upnl"`
	EarliestAt  time.Time `db:"earliest_at"`
	CreatedAt   time.Time `db:"created_at"`
	UpdatedAt   time.Time `db:"updated_at"`
}

func BatchInsertAddressInfo(addressInfos []AddressInfo) error {
	query := fmt.Sprintf("INSERT INTO %s (address, total_equity, prep_pnl, prep_open, prep_equity, sum_upnl, earliest_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (address) DO NOTHING", TableAddress)
	batch := &pgx.Batch{}
	for _, addressInfo := range addressInfos {
		batch.Queue(query, addressInfo.Address, addressInfo.TotalEquity, addressInfo.PrepPnl, addressInfo.PrepOpen, addressInfo.PrepEquity, addressInfo.SumUpnl, addressInfo.EarliestAt, addressInfo.CreatedAt, addressInfo.UpdatedAt)
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

// GetAddressChannel 获取地址信息通道
// before 如果为零时间，则按lasttime升序排序
// limit 如果为0，则不限制数量
func GetAddressChannel(logger *slog.Logger, after string, limit int, queueSize int) (<-chan string, error) {
	query := fmt.Sprintf(`SELECT a.address FROM "%s" a LEFT JOIN "%s" s ON a.address = s.address WHERE s.address IS NULL`, TableAddress, TableSkipAddress)
	args := []any{}
	if after != "" {
		args = append(args, after)
		query += fmt.Sprintf(" AND a.address > $%d", len(args))
	}
	query += " ORDER BY a.address ASC"
	if limit > 0 {
		args = append(args, limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	ch := make(chan string, queueSize)
	rows, err := db.Query(context.Background(), query, args...)
	if err != nil {
		return nil, fmt.Errorf("Query %s: %w", query, err)
	}
	go func() {
		var count int
		defer func() {
			rows.Close()
			close(ch)
			logger.Info("获取地址完成", "数量", count)
		}()
		for rows.Next() {
			var address string
			err := rows.Scan(&address)
			if err != nil {
				logger.Error("扫描错误: " + err.Error())
				return
			}
			ch <- address
			count++
		}
	}()
	return ch, nil
}

type SkipAddress struct {
	Address string `json:"address"`
	Type    int    `json:"type"`
	Remark  string `json:"remark"`
}

type AddSkipAddress struct {
	Address string
	Type    int
	Remark  string
}

func BatchSaveSkipAddresses(addresses []AddSkipAddress) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	query := fmt.Sprintf(`INSERT INTO %s (address, "type", remark) VALUES ($1, $2, $3) ON CONFLICT (address) DO UPDATE SET remark = EXCLUDED.remark`, TableSkipAddress)
	batch := &pgx.Batch{}
	for _, address := range addresses {
		batch.Queue(query, address.Address, address.Type, address.Remark)
	}
	results := db.SendBatch(ctx, batch)
	defer results.Close()
	_, err := results.Exec()
	if err != nil {
		return fmt.Errorf("Exec: %w", err)
	}
	return nil
}

func BatchDeleteSkipAddresses(addresses []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	query := fmt.Sprintf(`DELETE FROM %s WHERE address = $1`, TableSkipAddress)
	batch := &pgx.Batch{}
	for _, address := range addresses {
		batch.Queue(query, address)
	}
	results := db.SendBatch(ctx, batch)
	defer results.Close()
	_, err := results.Exec()
	if err != nil {
		return fmt.Errorf("Exec: %w", err)
	}
	return nil
}

func GetSkipAddresses(page, pageSize int, search, remarkSearch string, filterType *int) (*PageResult[SkipAddress], error) {
	// 构建查询条件
	conditions := []string{}
	args := []interface{}{}

	if search != "" {
		conditions = append(conditions, "address LIKE $"+fmt.Sprint(len(args)+1))
		args = append(args, search+"%")
	}

	if remarkSearch != "" {
		conditions = append(conditions, "remark LIKE $"+fmt.Sprint(len(args)+1))
		args = append(args, "%"+remarkSearch+"%")
	}

	if filterType != nil {
		conditions = append(conditions, "type = $"+fmt.Sprint(len(args)+1))
		args = append(args, *filterType)
	}

	// 构建WHERE子句
	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// 获取总数
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s %s`, TableSkipAddress, whereClause)
	var total int64
	err := QueryRowWithTimeout(time.Second*10, countQuery, args...).Scan(&total)
	if err != nil {
		return nil, fmt.Errorf("Count query %s: %w", countQuery, err)
	}

	// 获取分页数据
	query := fmt.Sprintf(`SELECT address, type, remark FROM %s %s ORDER BY address LIMIT $%d OFFSET $%d`,
		TableSkipAddress, whereClause, len(args)+1, len(args)+2)
	offset := (page - 1) * pageSize
	queryArgs := append(args, pageSize, offset)

	rows, err := QueryWithTimeout(time.Second*10, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("Query %s: %w", query, err)
	}
	defer rows.Close()

	var addresses []SkipAddress
	for rows.Next() {
		var address SkipAddress
		err := rows.Scan(&address.Address, &address.Type, &address.Remark)
		if err != nil {
			return nil, fmt.Errorf("Scan: %w", err)
		}
		addresses = append(addresses, address)
	}

	return &PageResult[SkipAddress]{
		Total: total,
		Items: addresses,
	}, nil
}

func UpdateSkipAddressRemark(address string, remark string) error {
	query := fmt.Sprintf(`UPDATE %s SET remark = $1 WHERE address = $2`, TableSkipAddress)
	return ExecWithTimeout(time.Second*10, query, remark, address)
}

func RefreshSkipAddress() error {
	return ExecWithTimeout(time.Minute*10, "SELECT refresh_skip_address()")
}

// AddressSet 地址集合
type AddressSet struct {
	ID                int       `json:"id"`
	Name              string    `json:"name"`
	Description       string    `json:"description"`
	SQLQuery          string    `json:"sqlQuery"`
	ExcludedAddresses []string  `json:"excludedAddresses"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

// AddressSetResult 地址集合结果
type AddressSetResult struct {
	AddressSet
	Addresses []string `json:"addresses"`
}

// CreateAddressSet 创建地址集合
func CreateAddressSet(set *AddressSet) error {
	query := `INSERT INTO address_set (name, description, sql_query, excluded_addresses) 
	         VALUES ($1, $2, $3, $4) RETURNING id, created_at, updated_at`
	return QueryRowWithTimeout(time.Second*10, query,
		set.Name, set.Description, set.SQLQuery, set.ExcludedAddresses).
		Scan(&set.ID, &set.CreatedAt, &set.UpdatedAt)
}

// UpdateAddressSet 更新地址集合
func UpdateAddressSet(set *AddressSet) error {
	query := `UPDATE address_set 
	         SET name = $1, description = $2, sql_query = $3, excluded_addresses = $4, updated_at = CURRENT_TIMESTAMP 
	         WHERE id = $5 RETURNING updated_at`
	return QueryRowWithTimeout(time.Second*10, query,
		set.Name, set.Description, set.SQLQuery, set.ExcludedAddresses, set.ID).
		Scan(&set.UpdatedAt)
}

// DeleteAddressSet 删除地址集合
func DeleteAddressSet(id int) error {
	query := `DELETE FROM address_set WHERE id = $1`
	return ExecWithTimeout(time.Second*10, query, id)
}

// GetAddressSet 获取地址集合
func GetAddressSet(id int) (*AddressSetResult, error) {
	// 获取基本信息
	var result AddressSetResult
	query := `SELECT id, name, description, sql_query, excluded_addresses, created_at, updated_at 
	         FROM address_set WHERE id = $1`
	err := QueryRowWithTimeout(time.Second*10, query, id).
		Scan(&result.ID, &result.Name, &result.Description, &result.SQLQuery,
			&result.ExcludedAddresses, &result.CreatedAt, &result.UpdatedAt)
	if err != nil {
		return nil, err
	}

	// 获取缓存的地址列表
	query = `SELECT address FROM address_set_cache WHERE set_id = $1 ORDER BY address`
	rows, err := QueryWithTimeout(time.Minute, query, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var addresses []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, err
		}
		addresses = append(addresses, addr)
	}
	result.Addresses = addresses

	return &result, nil
}

// ListAddressSets 列出所有地址集合
func ListAddressSets() ([]AddressSet, error) {
	query := `SELECT id, name, description, sql_query, excluded_addresses, created_at, updated_at 
	         FROM address_set ORDER BY id`
	rows, err := QueryWithTimeout(time.Minute, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sets []AddressSet
	for rows.Next() {
		var set AddressSet
		if err := rows.Scan(&set.ID, &set.Name, &set.Description, &set.SQLQuery,
			&set.ExcludedAddresses, &set.CreatedAt, &set.UpdatedAt); err != nil {
			return nil, err
		}
		sets = append(sets, set)
	}
	return sets, nil
}

// RefreshAddressSetCache 刷新地址集合缓存
func RefreshAddressSetCache(id int) error {
	return ExecWithTimeout(time.Minute*10, "SELECT refresh_address_set_cache($1)", id)
}

// ValidateSQL 验证SQL语句是否有效且只返回address字段
func ValidateSQL(sql string) error {
	// 添加LIMIT 0来快速验证SQL语法和结构
	query := fmt.Sprintf("SELECT * FROM (%s) q LIMIT 0", sql)
	rows, err := QueryWithTimeout(time.Second*10, query)
	if err != nil {
		return fmt.Errorf("SQL语法错误: %w", err)
	}
	defer rows.Close()

	// 检查列名
	cols := rows.FieldDescriptions()
	if len(cols) != 1 || string(cols[0].Name) != "address" {
		return fmt.Errorf("SQL必须且只能返回一个名为address的字段")
	}

	return nil
}

func GetAddressRole(address string) (string, error) {
	query := fmt.Sprintf("SELECT role FROM %s WHERE address = $1", TableAddress)
	row := QueryRowWithTimeout(time.Second*10, query, address)
	var role string
	err := row.Scan(&role)
	if err != nil {
		if errors.Is(err, ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return role, nil
}
