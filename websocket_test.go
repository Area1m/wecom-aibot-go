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

// 无法归类的帧（req_id 不匹配任何前缀，或 req_id 为空）应与官方一致：透传给消息处理器。
func TestHandleFrameUnknownPassthrough(t *testing.T) {
	w := newWSConnection(Config{}, silentTestLogger{})
	got := make(chan WsFrameRaw, 2)
	w.onMessage = func(f WsFrameRaw) { got <- f }

	w.handleFrame(context.Background(), WsFrameRaw{Headers: WsHeaders{ReqID: "unknown_1"}})
	w.handleFrame(context.Background(), WsFrameRaw{}) // reqID == ""

	for i := 0; i < 2; i++ {
		select {
		case <-got:
		case <-time.After(time.Second):
			t.Fatalf("未知帧应透传 (i=%d)", i)
		}
	}
}

// 心跳 ACK（req_id 前缀 ping_）应被静默丢弃，不透传给消息处理器。
func TestHandleFrameHeartbeatAckSilent(t *testing.T) {
	w := newWSConnection(Config{}, silentTestLogger{})
	called := make(chan struct{}, 1)
	w.onMessage = func(WsFrameRaw) { called <- struct{}{} }

	w.handleFrame(context.Background(), WsFrameRaw{Headers: WsHeaders{ReqID: WsCmdHeartbeat + "_1"}})

	select {
	case <-called:
		t.Error("心跳 ACK 不应透传给消息处理器")
	case <-time.After(50 * time.Millisecond):
	}
}

// 服务端发非法 JSON 帧：应上报「解析消息失败」但保持连接不断。
func TestMalformedFrameReportsErrorButKeepsConnection(t *testing.T) {
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
			var f WsFrameRaw
			if err := json.Unmarshal(data, &f); err != nil {
				return
			}
			if strings.HasPrefix(f.Headers.ReqID, WsCmdSubscribe+"_") {
				_ = conn.WriteJSON(WsFrameRaw{Headers: WsHeaders{ReqID: f.Headers.ReqID}})
				_ = conn.WriteMessage(websocket.TextMessage, []byte("not-json"))
				continue
			}
		}
	}))
	defer srv.Close()

	bot, err := NewClient(Config{
		BotID: "bot", Secret: "secret",
		WSURL:               "ws" + strings.TrimPrefix(srv.URL, "http"),
		HeartbeatIntervalMS: 60000, Logger: silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	parseErrs := make(chan error, 1)
	bot.OnError(func(_ context.Context, e error) {
		if strings.Contains(e.Error(), "解析消息失败") {
			parseErrs <- e
		}
	})
	authed := make(chan struct{}, 1)
	bot.OnAuthenticated(func(context.Context) { authed <- struct{}{} })
	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer bot.Disconnect()

	select {
	case <-authed:
	case <-time.After(3 * time.Second):
		t.Fatal("未认证")
	}

	select {
	case e := <-parseErrs:
		if !strings.Contains(e.Error(), "解析消息失败") {
			t.Errorf("错误应含「解析消息失败」, got %v", e)
		}
	case <-time.After(time.Second):
		t.Error("非法 JSON 应触发解析错误 OnError")
	}
	if !bot.IsConnected() {
		t.Error("解析错误不应断开连接")
	}
}
