package api

import (
	"encoding/json"
	"errors"
	"hyper-tracker/dao"
	"hyper-tracker/internal/service/collector"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type H = map[string]any

// WriteJSON 将数据以 JSON 格式写入响应
func WriteJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(data)
	if err != nil {
		slog.Error("writeJSON: " + err.Error())
	}
}

// WriteMsg 将文本消息写入响应
func WriteMsg(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "text/plain")
	_, err := w.Write([]byte(msg))
	if err != nil {
		slog.Error("writeMsg: " + err.Error())
	}
}

// withAuth 认证和CORS中间件
func (s *Service) withAuth(name string, handle http.HandlerFunc) http.HandlerFunc {
	logger := slog.With("api", name)
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Query().Get("t")
		if token == "" {
			token = r.Header.Get("X-Secret")
		}
		if s.cfg.Server.Secret != "" && token != s.cfg.Server.Secret {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		header := w.Header()

		// CORS
		if r.Method == "OPTIONS" {
			header.Set("Access-Control-Allow-Origin", "*")
			header.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			header.Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Secret, x-Grafana-Action, x-Grafana-Device-Id")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		header.Set("Access-Control-Allow-Origin", "*")
		header.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		header.Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Secret, x-Grafana-Action, x-Grafana-Device-Id")

		start := time.Now()
		logger.Debug("start", "method", r.Method, "url", r.URL.String())

		defer func() {
			logger.Debug("done", "method", r.Method, "url", r.URL.String(), "cost", time.Since(start))
			err := recover()
			if err != nil {
				logger.Error("panic", "error", err)
			}
		}()

		handle(w, r)
	}
}

func (s *Service) handleAddSkipAddress(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	address := q.Get("address")
	if address == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}
	remark := q.Get("remark")
	addrs := strings.Split(address, ",")
	var values []dao.AddSkipAddress
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		values = append(values, dao.AddSkipAddress{
			Address: addr,
			Type:    0,
			Remark:  remark,
		})
	}
	err := dao.BatchSaveSkipAddresses(values)
	if err != nil {
		WriteMsg(w, "保存排除地址失败: "+err.Error())
		return
	}
	WriteMsg(w, "success")
}

func (s *Service) handleDeleteSkipAddress(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	address := q.Get("address")
	if address == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}
	addrs := strings.Split(address, ",")
	var values []string
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		values = append(values, addr)
	}
	err := dao.BatchDeleteSkipAddresses(values)
	if err != nil {
		WriteMsg(w, "删除排除地址失败: "+err.Error())
		return
	}
	WriteMsg(w, "success")
}

func (s *Service) handleListSkipAddress(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page := 1
	pageSize := 10

	if pageStr := q.Get("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}
	if sizeStr := q.Get("pageSize"); sizeStr != "" {
		if size, err := strconv.Atoi(sizeStr); err == nil && size > 0 && size <= 100 {
			pageSize = size
		}
	}

	// 获取搜索参数
	search := q.Get("search")
	remarkSearch := q.Get("remarkSearch")

	// 获取类型筛选参数
	var filterType *int
	if typeStr := q.Get("type"); typeStr != "" {
		if t, err := strconv.Atoi(typeStr); err == nil {
			filterType = &t
		}
	}

	result, err := dao.GetSkipAddresses(page, pageSize, search, remarkSearch, filterType)
	if err != nil {
		WriteMsg(w, "获取排除地址列表失败: "+err.Error())
		return
	}
	WriteJSON(w, result)
}

func (s *Service) handleUpdateRemark(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	address := q.Get("address")
	if address == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}
	remark := q.Get("remark")

	err := dao.UpdateSkipAddressRemark(address, remark)
	if err != nil {
		WriteMsg(w, "更新备注失败: "+err.Error())
		return
	}
	WriteMsg(w, "success")
}

func (s *Service) handleListAddressSet(w http.ResponseWriter, r *http.Request) {
	sets, err := dao.ListAddressSets()
	if err != nil {
		WriteMsg(w, "获取地址集合列表失败: "+err.Error())
		return
	}
	WriteJSON(w, sets)
}

func (s *Service) handleGetAddressSet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	idStr := q.Get("id")
	if idStr == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	set, err := dao.GetAddressSet(id)
	if err != nil {
		if errors.Is(err, dao.ErrNoRows) {
			http.Error(w, "地址集合不存在", http.StatusNotFound)
			return
		}
		WriteMsg(w, "获取地址集合失败: "+err.Error())
		return
	}
	WriteJSON(w, set)
}

func (s *Service) handleCreateAddressSet(w http.ResponseWriter, r *http.Request) {
	var set dao.AddressSet
	if err := json.NewDecoder(r.Body).Decode(&set); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if set.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	if set.SQLQuery == "" {
		http.Error(w, "sqlQuery is required", http.StatusBadRequest)
		return
	}

	if err := createAddressSet(&set); err != nil {
		WriteMsg(w, "创建地址集合失败: "+err.Error())
		return
	}
	WriteJSON(w, set)
}

func (s *Service) handleUpdateAddressSet(w http.ResponseWriter, r *http.Request) {
	var set dao.AddressSet
	if err := json.NewDecoder(r.Body).Decode(&set); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if set.ID == 0 {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	if set.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	if set.SQLQuery == "" {
		http.Error(w, "sqlQuery is required", http.StatusBadRequest)
		return
	}

	if err := updateAddressSet(&set); err != nil {
		WriteMsg(w, "更新地址集合失败: "+err.Error())
		return
	}
	WriteJSON(w, set)
}

func (s *Service) handleDeleteAddressSet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	idStr := q.Get("id")
	if idStr == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	if err := dao.DeleteAddressSet(id); err != nil {
		WriteMsg(w, "删除地址集合失败: "+err.Error())
		return
	}
	WriteMsg(w, "success")
}

func (s *Service) handleRefreshAddressSet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	idStr := q.Get("id")
	if idStr == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	if err := dao.RefreshAddressSetCache(id); err != nil {
		WriteMsg(w, "刷新地址集合缓存失败: "+err.Error())
		return
	}
	WriteMsg(w, "success")
}

func (s *Service) handlePreviewAddressSet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SQLQuery string `json:"sqlQuery"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.SQLQuery == "" {
		http.Error(w, "sqlQuery is required", http.StatusBadRequest)
		return
	}

	// 验证SQL语句
	if err := dao.ValidateSQL(req.SQLQuery); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 执行SQL查询
	rows, err := dao.QueryWithTimeout(0, req.SQLQuery)
	if err != nil {
		WriteMsg(w, "SQL执行失败: "+err.Error())
		return
	}
	defer rows.Close()

	var addresses []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			WriteMsg(w, "数据扫描失败: "+err.Error())
			return
		}
		addresses = append(addresses, addr)
	}

	WriteJSON(w, addresses)
}

func (s *Service) handleRefreshData(w http.ResponseWriter, r *http.Request) {
	collector.RefreshExec(s.cfg.Collect.HyperExecs)
	WriteMsg(w, "success")
}

type positionSum struct {
	Coin  string  `json:"coin"`
	Size  float64 `json:"size"`
	Value float64 `json:"value"`
	AvgPx float64 `json:"avgPx"`
	UnPnl float64 `json:"unpnl"`
}

func (s *Service) handleGetAddressSetPositions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	idStr := q.Get("id")
	if idStr == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	positions, err := dao.GetSetUserPositions(id)
	if err != nil {
		WriteMsg(w, "获取地址集合持仓数据失败: "+err.Error())
		return
	}

	pm := map[string]positionSum{}
	var coins []string
	for _, ps := range positions {
		for _, p := range ps {
			sum, ok := pm[p.Coin]
			if !ok {
				sum = positionSum{Coin: p.Coin}
				coins = append(coins, p.Coin)
			}
			sum.Size += p.Size
			sum.Value += p.Value
			sum.UnPnl += p.UnrealPnl
			pm[p.Coin] = sum
		}
	}
	sort.Strings(coins)
	var result []positionSum
	for _, coin := range coins {
		sum := pm[coin]
		sum.AvgPx = sum.Value / math.Abs(sum.Size)
		result = append(result, sum)
	}
	WriteJSON(w, result)
}

func (s *Service) handleGetAddressSetSummary(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	idStr := q.Get("id")
	if idStr == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	positions, err := dao.GetSetUserPositions(id)
	if err != nil {
		WriteMsg(w, "获取地址集合持仓数据失败: "+err.Error())
		return
	}
	var notional float64
	var unrealPnl float64
	for _, ps := range positions {
		for _, p := range ps {
			notional += p.Value
			unrealPnl += p.UnrealPnl
		}
	}
	result := H{
		"notional":  notional,
		"unrealPnl": unrealPnl,
	}
	WriteJSON(w, result)
}

// createAddressSet 创建地址集合
func createAddressSet(set *dao.AddressSet) error {
	if err := dao.ValidateSQL(set.SQLQuery); err != nil {
		return err
	}
	if err := dao.CreateAddressSet(set); err != nil {
		return err
	}
	return dao.RefreshAddressSetCache(set.ID)
}

// updateAddressSet 更新地址集合
func updateAddressSet(set *dao.AddressSet) error {
	if err := dao.ValidateSQL(set.SQLQuery); err != nil {
		return err
	}
	if err := dao.UpdateAddressSet(set); err != nil {
		return err
	}
	return dao.RefreshAddressSetCache(set.ID)
}
