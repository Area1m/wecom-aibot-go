package wecomaibot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// panickyLogger 在 Warn 时 panic，用来把 panic 注入到会话 goroutine（handleFrame）里。
// 验证 runSessionSafely 能兜住：转成错误上报并按普通断线重连，而不是让客户端永久假死。
type panickyLogger struct{ silentTestLogger }

func (panickyLogger) Warn(string, ...any) { panic("logger warn panic (test)") }

func TestSessionPanicIsContainedAndReconnects(t *testing.T) {
	var subscribes int32
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var frame WsFrameRaw
			if err := json.Unmarshal(data, &frame); err != nil {
				return
			}
			if !strings.HasPrefix(frame.Headers.ReqID, WsCmdSubscribe+"_") {
				continue
			}
			_ = conn.WriteJSON(WsFrameRaw{Headers: WsHeaders{ReqID: frame.Headers.ReqID}})
			if atomic.AddInt32(&subscribes, 1) == 1 {
				// 让 handleFrame 走到 logger.Warn 并 panic（真实场景是 SDK 自身 bug）
				_ = conn.WriteJSON(WsFrameRaw{
					Headers: WsHeaders{ReqID: WsCmdHeartbeat + "_1"},
					ErrCode: 1,
					ErrMsg:  "boom",
				})
			}
		}
	}))
	defer srv.Close()

	bot, err := NewClient(Config{
		BotID:                "bot",
		Secret:               "secret",
		WSURL:                "ws" + strings.TrimPrefix(srv.URL, "http"),
		ReconnectIntervalMS:  50,
		MaxReconnectAttempts: -1,
		HeartbeatIntervalMS:  30,
		Logger:               panickyLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}

	var errMu sync.Mutex
	var errs []error
	bot.OnError(func(_ context.Context, err error) {
		errMu.Lock()
		defer errMu.Unlock()
		errs = append(errs, err)
	})

	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer bot.Disconnect()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&subscribes) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := atomic.LoadInt32(&subscribes); got < 2 {
		t.Fatalf("panic 后应重连（而不是永久假死）: subscribes=%d", got)
	}

	errMu.Lock()
	defer errMu.Unlock()
	var found bool
	for _, e := range errs {
		if strings.Contains(e.Error(), "panic") {
			found = true
		}
	}
	if !found {
		t.Errorf("应通过 OnError 上报 panic，实际收到: %v", errs)
	}
	if !bot.IsConnected() {
		t.Error("重连后应恢复连接")
	}
}
