package wecomaibot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// 认证被拒绝（subscribe ACK 带 errcode != 0）时，客户端必须主动断开并重连，
// 而不是停在「已连接但未认证」的假死态。
func TestAuthFailureTriggersReconnect(t *testing.T) {
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
			if atomic.AddInt32(&subscribes, 1) == 1 {
				// 第一次认证被拒绝
				_ = conn.WriteJSON(WsFrameRaw{Headers: WsHeaders{ReqID: frame.Headers.ReqID}, ErrCode: 1, ErrMsg: "bad secret"})
				continue
			}
			// 后续认证成功
			_ = conn.WriteJSON(WsFrameRaw{Headers: WsHeaders{ReqID: frame.Headers.ReqID}})
		}
	}))
	defer srv.Close()

	bot, err := NewClient(Config{
		BotID:                "bot",
		Secret:               "secret",
		WSURL:                "ws" + strings.TrimPrefix(srv.URL, "http"),
		ReconnectIntervalMS:  50,
		MaxReconnectAttempts: 5,
		HeartbeatIntervalMS:  200,
		Logger:               silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}

	var authErrs int32
	bot.OnError(func(_ context.Context, err error) {
		if strings.Contains(err.Error(), "认证失败") {
			atomic.AddInt32(&authErrs, 1)
		}
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
		t.Fatalf("认证失败后应重连并重新认证: subscribes=%d", got)
	}
	if got := atomic.LoadInt32(&authErrs); got == 0 {
		t.Error("应通过 OnError 上报认证失败")
	}
	if !bot.IsConnected() {
		t.Error("认证成功后应恢复连接")
	}
}

// 认证持续失败时，重连计数必须递增并最终命中 MaxReconnectAttempts 后放弃；
// 而不是在每次拨号成功后清零、无限重试。
func TestAuthFailureRespectsMaxReconnect(t *testing.T) {
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
			atomic.AddInt32(&subscribes, 1)
			_ = conn.WriteJSON(WsFrameRaw{Headers: WsHeaders{ReqID: frame.Headers.ReqID}, ErrCode: 1, ErrMsg: "bad secret"})
		}
	}))
	defer srv.Close()

	const maxAttempts = 3
	bot, err := NewClient(Config{
		BotID:                "bot",
		Secret:               "secret",
		WSURL:                "ws" + strings.TrimPrefix(srv.URL, "http"),
		ReconnectIntervalMS:  50,
		MaxReconnectAttempts: maxAttempts,
		HeartbeatIntervalMS:  200,
		Logger:               silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}

	var gaveUp int32
	bot.OnError(func(_ context.Context, err error) {
		if strings.Contains(err.Error(), "超过最大重连次数") {
			atomic.AddInt32(&gaveUp, 1)
		}
	})

	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer bot.Disconnect()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&gaveUp) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&gaveUp) == 0 {
		t.Fatalf("认证失败应在 %d 次重连后放弃: subscribes=%d", maxAttempts, atomic.LoadInt32(&subscribes))
	}

	// 放弃后不应继续重连：初始 1 次 + maxAttempts 次重连，共 maxAttempts+1 次认证。
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt32(&subscribes); got != int32(maxAttempts+1) {
		t.Errorf("认证次数应停在 %d，实际 %d", maxAttempts+1, got)
	}
}

// 重连耗尽（give-up）后不能永久假死：再次 Connect() 应能真正重启并重新开始认证，
// 而不是因 started 未复位而被静默忽略。
func TestReconnectAfterGiveUp(t *testing.T) {
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
			atomic.AddInt32(&subscribes, 1)
			_ = conn.WriteJSON(WsFrameRaw{Headers: WsHeaders{ReqID: frame.Headers.ReqID}, ErrCode: 1, ErrMsg: "bad secret"})
		}
	}))
	defer srv.Close()

	const maxAttempts = 2
	bot, err := NewClient(Config{
		BotID:                "bot",
		Secret:               "secret",
		WSURL:                "ws" + strings.TrimPrefix(srv.URL, "http"),
		ReconnectIntervalMS:  50,
		MaxReconnectAttempts: maxAttempts,
		HeartbeatIntervalMS:  200,
		Logger:               silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	defer bot.Disconnect()

	var gaveUp int32
	bot.OnError(func(_ context.Context, err error) {
		if strings.Contains(err.Error(), "超过最大重连次数") {
			atomic.AddInt32(&gaveUp, 1)
		}
	})

	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&gaveUp) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&gaveUp) == 0 {
		t.Fatalf("应在 %d 次重连后放弃", maxAttempts)
	}

	// 放弃后客户端应已复位，再次 Connect 能重新开始认证。
	before := atomic.LoadInt32(&subscribes)
	if err := bot.Connect(); err != nil {
		t.Fatalf("再次 Connect 失败: %v", err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&subscribes) > before {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&subscribes); got <= before {
		t.Errorf("再次 Connect 后应重新认证: before=%d after=%d", before, got)
	}
}

// 订阅 ACK 迟迟不回（企微在 ACK 前静默掐线）时，读超时兜底应触发重连，而不是永久卡 connecting。
func TestAuthAckTimeoutTriggersReconnect(t *testing.T) {
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
			var f WsFrameRaw
			if err := json.Unmarshal(data, &f); err != nil {
				return
			}
			if strings.HasPrefix(f.Headers.ReqID, WsCmdSubscribe+"_") {
				atomic.AddInt32(&subscribes, 1)
				// 故意不回 ACK，保持连接挂着，模拟「TCP 活着但应用层不回订阅 ACK」
			}
		}
	}))
	defer srv.Close()

	bot, err := NewClient(Config{
		BotID: "bot", Secret: "secret",
		WSURL:               "ws" + strings.TrimPrefix(srv.URL, "http"),
		ReconnectIntervalMS: 50, MaxReconnectAttempts: -1,
		HeartbeatIntervalMS: 60000,
		AuthTimeoutMS:       100, // 缩短超时，避免测试拖 15s
		Logger:              silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	var reconnects int32
	bot.OnReconnecting(func(context.Context, int) { atomic.AddInt32(&reconnects, 1) })
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
		t.Fatalf("订阅 ACK 超时后应重连并重新 subscribe: subscribes=%d reconnects=%d", got, atomic.LoadInt32(&reconnects))
	}
}

// 认证成功后必须清除「等订阅 ACK」的读超时：否则健康连接会在 authTimeout 后被误断。
func TestAuthSuccessClearsReadDeadline(t *testing.T) {
	fs := newFakeWSServer(t) // 认证后静默：不回 ACK、不发消息
	bot, err := NewClient(Config{
		BotID: "bot", Secret: "secret",
		WSURL:               "ws" + strings.TrimPrefix(fs.srv.URL, "http"),
		HeartbeatIntervalMS: 60000,
		AuthTimeoutMS:       150, // 若清除失败，150ms 后就会误断
		Logger:              silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	var disconnects int32
	bot.OnDisconnected(func(context.Context, string) { atomic.AddInt32(&disconnects, 1) })
	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer bot.Disconnect()

	// 等认证成功，再静默远超 authTimeout 的时间，确认未被读超时误断。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&fs.subscribes) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // > 3× authTimeout
	if got := atomic.LoadInt32(&disconnects); got != 0 {
		t.Errorf("认证成功后不应被读超时误断: disconnects=%d", got)
	}
	if !bot.IsConnected() {
		t.Error("认证成功后应保持连接")
	}
}
