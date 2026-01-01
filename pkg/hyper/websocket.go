package hyper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hyper-tracker/pkg/utils"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const WSUrl = "wss://api.hyperliquid.xyz/ws"

type WebSocketMsgIO interface {
	IN(data []byte) error
	OUT(data []byte) error
}

type FileWebSocketMsgIO struct {
	file        *os.File
	mu          sync.Mutex
	writePrefix func(dir string)
	el          []byte
}

func NewFileWebSocketMsgIO(file *os.File, prefix string) *FileWebSocketMsgIO {
	io := &FileWebSocketMsgIO{
		file: file,
		el:   []byte("\n"),
	}
	io.setWritePrefix(prefix)
	return io
}

func (io *FileWebSocketMsgIO) setWritePrefix(prefix string) {
	var writePrefix func(dir string)
	if len(prefix) > 0 {
		writePrefix = func(dir string) {
			fmt.Fprintf(io.file, "%s %s %s:", time.Now().Format("060102150405"), prefix, dir)
		}
	} else {
		writePrefix = func(dir string) {
			fmt.Fprintf(io.file, "%s %s: ", time.Now().Format("060102150405"), dir)
		}
	}
	io.writePrefix = writePrefix
}

func (io *FileWebSocketMsgIO) IN(data []byte) error {
	io.mu.Lock()
	defer io.mu.Unlock()
	io.writePrefix("I")
	io.file.Write(data)
	io.file.Write(io.el)
	return nil
}

func (io *FileWebSocketMsgIO) OUT(data []byte) error {
	io.mu.Lock()
	defer io.mu.Unlock()
	io.writePrefix("O")
	io.file.Write(data)
	io.file.Write(io.el)
	return nil
}

type WebSocket struct {
	url        string
	httpClient *http.Client
	logger     *slog.Logger
	msgIO      WebSocketMsgIO

	conn           *utils.Mutex[*websocket.Conn]
	onConnected    func(conn *websocket.Conn) error
	onDisconnected func(err error) error
	pingRespCh     *utils.Mutex[chan struct{}]
	fillsCh        *utils.Mutex[[]chan *WsUserFills]
}

func timeoutCtx(parentCtx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(parentCtx, timeout)
}

func NewWebSocket(url string, httpClient *http.Client, logger *slog.Logger) (*WebSocket, error) {
	ws := &WebSocket{
		url:        url,
		httpClient: httpClient,
		logger:     logger,
		conn:       utils.NewMutex[*websocket.Conn](nil),
		pingRespCh: utils.NewMutex[chan struct{}](nil),
		fillsCh:    utils.NewMutex[[]chan *WsUserFills](nil),
	}
	return ws, nil
}

func DefaultWebSocket() (*WebSocket, error) {
	return NewWebSocket(WSUrl, http.DefaultClient, slog.With("service", "Hyper WS"))
}

func (ws *WebSocket) SetMsgIO(msgIO WebSocketMsgIO) {
	ws.msgIO = msgIO
}

func (ws *WebSocket) logIN(data []byte) {
	if ws.msgIO == nil {
		return
	}
	ws.msgIO.IN(data)
}

func (ws *WebSocket) logOUT(data any) {
	if ws.msgIO == nil {
		return
	}
	var content []byte
	content, ok := data.([]byte)
	if !ok {
		content, _ = json.Marshal(data)
	}
	ws.msgIO.OUT(content)
}

func (ws *WebSocket) OnConnected(fn func(conn *websocket.Conn) error) {
	ws.onConnected = fn
}

func (ws *WebSocket) OnDisconnected(fn func(err error) error) {
	ws.onDisconnected = fn
}

type WsRequest struct {
	Method       string `json:"method"`
	Subscription any    `json:"subscription,omitempty"`
}

type WsResponse struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

func (ws *WebSocket) UserFills(ctx context.Context, address string) error {
	conn := ws.conn.Get()
	if conn == nil {
		return fmt.Errorf("websocket not connected")
	}
	data := WsRequest{
		Method: "subscribe",
		Subscription: H{
			"type": "userFills",
			"user": address,
		},
	}
	ctx, cancel := timeoutCtx(ctx, 3*time.Second)
	defer cancel()
	err := wsjson.Write(ctx, conn, data)
	ws.logOUT(data)
	if err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	return nil
}

var ErrPingTimeout = errors.New("ping timeout")

func (ws *WebSocket) ping(ctx context.Context, conn *websocket.Conn) error {
	ch := make(chan struct{})
	ws.pingRespCh.Set(ch)
	ctx, cancel := timeoutCtx(ctx, 3*time.Second)
	defer cancel()
	req := WsRequest{
		Method: "ping",
	}
	err := wsjson.Write(ctx, conn, req)
	ws.logOUT(req)
	if err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		ch = ws.pingRespCh.Swap(nil)
		if ch != nil {
			close(ch)
		}
		return ErrPingTimeout
	}
}

func (ws *WebSocket) Ping() error {
	conn := ws.conn.Get()
	if conn == nil {
		return fmt.Errorf("websocket not connected")
	}
	return ws.ping(context.Background(), conn)
}

type WsUserFills struct {
	IsSnapshot bool   `json:"isSnapshot,omitempty"`
	User       string `json:"user,omitempty"`
	Fills      []Fill `json:"fills,omitempty"`
}

func (ws *WebSocket) read(ctx context.Context, conn *websocket.Conn) error {
	mt, message, err := conn.Read(ctx)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	ws.logIN(message)
	if mt == websocket.MessageText {
		var resp WsResponse
		err = json.Unmarshal(message, &resp)
		if err != nil {
			return fmt.Errorf("json unmarshal: %w", err)
		}
		switch resp.Channel {
		case "pong":
			ch := ws.pingRespCh.Swap(nil)
			if ch != nil {
				close(ch)
			}
		case "userFills":
			ws.handleUserFills(resp.Data)
		}
	} else {
		ws.logger.Debug(fmt.Sprintf("unknown message: %v %s", mt, string(message)))
	}
	return nil
}

func (ws *WebSocket) handleUserFills(data []byte) {
	fills := &WsUserFills{}
	err := json.Unmarshal(data, fills)
	if err != nil {
		ws.logger.Error("json unmarshal: " + err.Error())
		return
	}
	ws.fillsCh.Do(func(chs []chan *WsUserFills) {
		for _, ch := range chs {
			select {
			case ch <- fills:
				// 发送成功
			default:
				// channel满了，记录警告但不阻塞
				ws.logger.Warn(fmt.Sprintf("订阅者channel已满, 丢弃消息: %+v", fills))
			}
		}
	})
}

func (ws *WebSocket) readLoop(ctx context.Context, conn *websocket.Conn, errCh chan<- error) {
	for {
		err := ws.read(ctx, conn)
		if err != nil {
			ws.logger.Error("read: " + err.Error())
			errCh <- err
			return
		}
	}
}

// 定时ping保持连接，如果ping失败，则关闭连接
func (ws *WebSocket) pingLoop(ctx context.Context, conn *websocket.Conn, errCh chan<- error) {
	pingTk := time.NewTicker(55 * time.Second)
	defer pingTk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-pingTk.C:
			err := ws.ping(ctx, conn)
			if err != nil {
				ws.logger.Error("ping: " + err.Error())
				errCh <- err
				return
			}
		}
	}
}

func (ws *WebSocket) run(ctx context.Context, conn *websocket.Conn) <-chan error {
	errCh := make(chan error, 1)
	go ws.readLoop(ctx, conn, errCh)
	go ws.pingLoop(ctx, conn, errCh)
	return errCh
}

// SubscribeFills 创建并返回一个新的fills订阅channel（适用于多订阅者场景）
func (ws *WebSocket) SubscribeFills() <-chan *WsUserFills {
	ch := make(chan *WsUserFills, 100)
	ws.fillsCh.Deal(func(chs []chan *WsUserFills) []chan *WsUserFills {
		return append(chs, ch)
	})
	return ch
}

// SubscribeFillsTo 订阅fills到外部提供的channel（适用于单订阅者高性能场景）
// 注意：外部channel的生命周期需要由调用方管理
func (ws *WebSocket) SubscribeFillsTo(ch chan *WsUserFills) {
	ws.fillsCh.Deal(func(chs []chan *WsUserFills) []chan *WsUserFills {
		return append(chs, ch)
	})
}

func (ws *WebSocket) UnsubscribeFills(ch chan *WsUserFills) {
	ws.fillsCh.Deal(func(chs []chan *WsUserFills) []chan *WsUserFills {
		for i, c := range chs {
			if c == ch {
				chs = append(chs[:i], chs[i+1:]...)
				break
			}
		}
		return chs
	})
	close(ch)
}

func (ws *WebSocket) Start(ctx context.Context) {
	opts := &websocket.DialOptions{
		HTTPClient: ws.httpClient,
	}
	closeConn := func() {
		ws.conn.Deal(func(conn *websocket.Conn) *websocket.Conn {
			if conn != nil {
				conn.Close(websocket.StatusNormalClosure, "normal closure")
			}
			return nil
		})
	}
	defer func() {
		closeConn()
	}()
	for {
		select {
		case <-ctx.Done():
			ws.logger.Info("websocket exit")
			return
		default:
			conn, _, err := websocket.Dial(ctx, ws.url, opts)
			if err != nil {
				ws.logger.Error("websocket dial: " + err.Error())
				time.Sleep(1 * time.Second)
				continue
			}
			ws.conn.Set(conn)
			errCh := ws.run(ctx, conn)
			if ws.onConnected != nil {
				err = ws.onConnected(conn)
				if err != nil {
					ws.logger.Error("onConnected: " + err.Error())
					closeConn()
					continue
				}
			}
			err = <-errCh
			if ws.onDisconnected != nil {
				err = ws.onDisconnected(err)
			}
			if err == nil { // 没有发生错误，则退出循环
				return
			}
			closeConn()
		}
	}
}
