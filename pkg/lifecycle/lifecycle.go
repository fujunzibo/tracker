/**
 * 应用程序生命周期处理器
 */
package lifecycle

import (
	"errors"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Handler 启动阶段处理器
type Handler = func() error

// NoErrorHandler 没有错误的处理器
type NoErrorHandler func()

func (h NoErrorHandler) Handle() error {
	h()
	return nil
}

var (
	startupHandlers  = make([]Handler, 0)
	readyHandlers    = make([]Handler, 0)
	shutdownHandlers = make([]Handler, 0)
)

// RegisterStartupHandler 注册启动阶段处理器
func RegisterStartupHandler(handler Handler) {
	startupHandlers = append(startupHandlers, handler)
}

// RegisterStartupNoErrorHandler 注册启动阶段处理器，忽略错误
func RegisterStartupNoErrorHandler(handler NoErrorHandler) {
	startupHandlers = append(startupHandlers, handler.Handle)
}

// RegisterReadyHandler 注册就绪阶段处理器
func RegisterReadyHandler(handler Handler) {
	readyHandlers = append(readyHandlers, handler)
}

// RegisterReadyNoErrorHandler 注册就绪阶段处理器，忽略错误
func RegisterReadyNoErrorHandler(handler NoErrorHandler) {
	readyHandlers = append(readyHandlers, handler.Handle)
}

// RegisterShutdownHandler 注册关闭阶段处理器
func RegisterShutdownHandler(handler Handler) {
	shutdownHandlers = append(shutdownHandlers, handler)
}

// RegisterShutdownNoErrorHandler 注册关闭阶段处理器，忽略错误
func RegisterShutdownNoErrorHandler(handler NoErrorHandler) {
	shutdownHandlers = append(shutdownHandlers, handler.Handle)
}

// RegisterShutdownCloser 关闭阶段执行关闭
func RegisterShutdownCloser(closer io.Closer) {
	shutdownHandlers = append(shutdownHandlers, closer.Close)
}

// RegisterShutdownNoErrorCloser 关闭阶段执行关闭，忽略错误
func RegisterShutdownNoErrorCloser(closer io.Closer) {
	shutdownHandlers = append(shutdownHandlers, func() error { _ = closer.Close(); return nil })
}

// RunStartupHandlers 启动阶段处理
func RunStartupHandlers() error {
	n := len(startupHandlers) - 1
	for i := n; i >= 0; i-- {
		handler := startupHandlers[i]
		if err := handler(); err != nil {
			return err
		}
	}
	return nil
}

// RunReadyHandlers 初始化完成阶段处理
func RunReadyHandlers() error {
	n := len(readyHandlers) - 1
	for i := n; i >= 0; i-- {
		handler := readyHandlers[i]
		if err := handler(); err != nil {
			return err
		}
	}
	return nil
}

// RunShutdownHandlers 关闭阶段处理
func RunShutdownHandlers() error {
	n := len(shutdownHandlers) - 1
	for i := n; i >= 0; i-- {
		handler := shutdownHandlers[i]
		if err := handler(); err != nil {
			return err
		}
	}
	return nil
}

var (
	ErrShutdownTimeout = errors.New("shutdown timeout")
	ErrForcedShutdown  = errors.New("forced shutdown")
)

func Shutdown(timeout time.Duration, first func(), sig ...os.Signal) error {
	c := make(chan os.Signal, 1)
	if len(sig) == 0 {
		sig = []os.Signal{os.Interrupt, syscall.SIGTERM}
	}
	signal.Notify(c, sig...)
	<-c
	if first != nil {
		first()
	}
	ch := make(chan error, 1)
	go func() {
		if err := RunShutdownHandlers(); err != nil {
			ch <- err
		}
		close(ch)
	}()
	if timeout > 0 {
		to := time.NewTimer(timeout)
		select {
		case <-to.C:
			return ErrShutdownTimeout
		case err := <-ch:
			return err
		case <-c: // 再次接收到信号，则停止等待
			to.Stop()
			return ErrForcedShutdown
		}
	} else {
		select {
		case err := <-ch:
			return err
		case <-c: // 再次接收到信号，则停止等待
			return ErrForcedShutdown
		}
	}
}
