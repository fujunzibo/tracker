package utils

import (
	"fmt"
	"hyper-tracker/pkg/errorx"
	"log/slog"
	"net"
	"os"
	"sync"
)

func FatalOnError(err error, msg string) {
	if err != nil {
		slog.Error(msg, slog.Any("error", err))
		os.Exit(1)
	}
}

type Zero = struct{}

var ClosedChan = func() chan Zero {
	ch := make(chan Zero)
	close(ch)
	return ch
}()

var LocalIP = GetLocalIP()

func GetLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		return ""
	}
	_ = conn.Close()

	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

// Recovery panic
func Recovery(logger *slog.Logger) {
	re := recover()
	if re != nil {
		stacks := errorx.FormatFrames(errorx.GetFrames(1), 2)
		if logger == nil {
			logger = slog.Default()
		}
		logger.Error(fmt.Sprintf("Panic: %v\n%s", re, stacks))
	}
}

type Mutex[T any] struct {
	sync.RWMutex
	data T
}

func NewMutex[T any](data T) *Mutex[T] {
	return &Mutex[T]{
		data: data,
	}
}

func (m *Mutex[T]) Get() T {
	m.RLock()
	defer m.RUnlock()
	return m.data
}

func (m *Mutex[T]) Set(data T) {
	m.Lock()
	defer m.Unlock()
	m.data = data
}

func (m *Mutex[T]) Swap(data T) T {
	m.Lock()
	defer m.Unlock()
	old := m.data
	m.data = data
	return old
}

func (m *Mutex[T]) Deal(fn func(data T) T) T {
	m.Lock()
	defer m.Unlock()
	data := fn(m.data)
	m.data = data
	return data
}

func (m *Mutex[T]) Do(fn func(data T)) {
	m.Lock()
	defer m.Unlock()
	fn(m.data)
}
