package collector

import (
	"hyper-tracker/dao"
	"hyper-tracker/internal/config"
	"log/slog"
	"runtime"
	"time"
)

// timer 返回一个定时器，用于定时执行任务
func timer(intervalSec int, nextTimeVar string) (C <-chan time.Time, resetTimer func(), saveNextTime func()) {
	logger := slog.With("timer", nextTimeVar)
	intervalDur := time.Duration(intervalSec) * time.Second
	tr := time.NewTimer(intervalDur)
	nextTi, err := dao.GetVarWithDefault(nextTimeVar, "")
	if err != nil {
		logger.Error("get var: " + err.Error())
	}
	nextTime := time.Now()
	if nextTi != "" {
		ti, err := time.Parse(time.RFC3339, nextTi)
		if err != nil {
			logger.Error("parse next time: " + err.Error())
		} else {
			nextTime = ti
		}
	}
	resetTimer = func() {
		dur := time.Until(nextTime)
		if dur > 0 {
			tr.Reset(dur)
		} else {
			tr.Reset(time.Second)
		}
		nextTime = nextTime.Add(intervalDur)
	}
	saveNextTime = func() {
		err := dao.SetVar(nextTimeVar, nextTime.Format(time.RFC3339), "")
		if err != nil {
			logger.Error("save next time: " + err.Error())
		}
	}
	return tr.C, resetTimer, saveNextTime
}

// RefreshExec 刷新视图数据
func RefreshExec(execs []config.ExecConfig) {
	refreshExecMap := map[string]func(cfg *config.ExecConfig){
		"RefreshAllSummary": func(cfg *config.ExecConfig) {
			dao.RefreshAllSummary(runtime.NumCPU())
		},
	}

	for _, exec := range execs {
		logger := slog.With("refresh", exec.Name)
		if fn, ok := refreshExecMap[exec.Name]; ok && len(exec.Queries) == 0 {
			fn(&exec)
			continue
		}
		for _, query := range exec.Queries {
			start := time.Now()
			logger.Debug("exec", "query", query)
			err := dao.ExecWithTimeout(exec.Timeout, query)
			if err != nil {
				logger.Error(err.Error())
			}
			logger.Debug("done", "query", query, "cost", time.Since(start))
		}
	}
}

