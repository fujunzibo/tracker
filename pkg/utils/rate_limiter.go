package utils

import (
	"context"
	"sync"
	"time"
)

const unitTime = time.Second * 10

// RateLimiter 实现漏桶算法的限流器
type RateLimiter struct {
	rate      float64       // 每unitTime允许的请求数
	bucket    chan struct{} // 令牌桶
	lastToken time.Time     // 上次发放令牌的时间
	mu        sync.Mutex    // 互斥锁保护 lastToken
}

// NewRateLimiter 创建一个新的限流器
func NewRateLimiter(rate int) *RateLimiter {
	if rate <= 0 {
		rate = 10 // 每unitTime允许的请求数，这里相当于每秒1次
	}
	return &RateLimiter{
		rate:      float64(rate),
		bucket:    make(chan struct{}, rate),
		lastToken: time.Now(),
	}
}

// getNextTokenTime 计算下一个令牌的可用时间
func (r *RateLimiter) getNextTokenTime() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	// 计算期望的时间间隔
	interval := time.Duration(float64(unitTime) / r.rate)

	// 如果距离上次发放令牌的时间超过了间隔，直接从当前时间开始计算
	if now.Sub(r.lastToken) > interval {
		r.lastToken = now
		return now
	}

	// 否则，在上次发放时间的基础上增加间隔
	nextTime := r.lastToken.Add(interval)
	r.lastToken = nextTime
	return nextTime
}

// Wait 等待获取一个令牌
func (r *RateLimiter) Wait(ctx context.Context) error {
	nextTime := r.getNextTokenTime()
	delay := time.Until(nextTime)

	if delay > 0 {
		select {
		case <-time.After(delay):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
