package dao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

func GetVar(name string) (string, error) {
	query := fmt.Sprintf("SELECT value FROM %s WHERE name = $1", TableVars)
	row := QueryRowWithTimeout(time.Second*10, query, name)
	var value string
	err := row.Scan(&value)
	if err != nil {
		return "", err
	}
	return value, nil
}

func GetVarWithDefault(name string, defaultValue string) (string, error) {
	value, err := GetVar(name)
	if err != nil {
		if errors.Is(err, ErrNoRows) {
			return defaultValue, nil
		}
		return defaultValue, err
	}
	return value, nil
}

func SetVar(name string, value string, remark string) error {
	query := fmt.Sprintf("INSERT INTO %s (name, value, remark) VALUES ($1, $2, $3) ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()", TableVars)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	_, err := db.Exec(ctx, query, name, value, remark)
	if err != nil {
		return err
	}
	return nil
}

// GetCoins 获取标的币种
func GetCoins() ([]string, error) {
	query := fmt.Sprintf("SELECT value FROM %s WHERE name = $1", TableVars)
	row := QueryRowWithTimeout(time.Second*10, query, CoinsVar)
	var coins string
	err := row.Scan(&coins)
	if err != nil {
		return nil, err
	}
	return strings.Split(coins, ","), nil
}

func GetSetUserPositions(setID int) (map[string][]Position, error) {
	key := fmt.Sprintf("positions_%d", setID)
	data, err := GetVar(key)
	if err != nil {
		return nil, err
	}
	var positions map[string][]Position
	err = json.Unmarshal([]byte(data), &positions)
	if err != nil {
		return nil, err
	}
	return positions, nil
}
