package hyper

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestWebSocket(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	ws, err := DefaultWebSocket()
	if err != nil {
		t.Fatalf("new web socket: %v", err)
	}
	ws.onConnected = func(conn *websocket.Conn) error {
		ws.UserFills(context.Background(), "0x85abe7cf51cd374bdcc4a02fd2baceea6a3914f8")
		return nil
	}
	go ws.Start(context.Background())
	time.Sleep(60 * time.Second)
}
