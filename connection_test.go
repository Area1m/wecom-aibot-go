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

// Connect 重复调用应幂等：只起一条连接、只认证一次。
func TestConnectIsIdempotent(t *testing.T) {
	fs := newFakeWSServer(t)
	bot, err := NewClient(Config{
		BotID: "bot", Secret: "secret",
		WSURL:               "ws" + strings.TrimPrefix(fs.srv.URL, "http"),
		HeartbeatIntervalMS: 200, Logger: silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	defer bot.Disconnect()

	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	if err := bot.Connect(); err != nil {
		t.Fatalf("第二次 Connect 应幂等: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&fs.subscribes) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if got := atomic.LoadInt32(&fs.subscribes); got != 1 {
		t.Errorf("Connect 应幂等，只认证 1 次, got %d", got)
	}
}

// Disconnect 后应停止重连：认证成功后服务端断开触发重连，Disconnect 后 subscribe 不再增长。
func TestDisconnectStopsReconnect(t *testing.T) {
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
				_ = conn.WriteJSON(WsFrameRaw{Headers: WsHeaders{ReqID: f.Headers.ReqID}})
				return // 认证成功后断开，触发重连
			}
		}
	}))
	defer srv.Close()

	bot, err := NewClient(Config{
		BotID: "bot", Secret: "secret",
		WSURL:               "ws" + strings.TrimPrefix(srv.URL, "http"),
		ReconnectIntervalMS: 50, MaxReconnectAttempts: -1,
		HeartbeatIntervalMS: 60000, Logger: silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	var reconnects int32
	bot.OnReconnecting(func(context.Context, int) { atomic.AddInt32(&reconnects, 1) })
	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&reconnects) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&reconnects) == 0 {
		t.Fatal("应触发重连")
	}

	bot.Disconnect()
	before := atomic.LoadInt32(&subscribes)
	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt32(&subscribes); got != before {
		t.Errorf("Disconnect 后不应继续重连: before=%d after=%d", before, got)
	}
}

// 拨号失败（端口拒绝）应触发重连，且重连期间 IsConnected 为 false。
func TestDialFailureTriggersReconnect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	srv.Close() // 立即关闭，使拨号失败

	bot, err := NewClient(Config{
		BotID: "bot", Secret: "secret", WSURL: url,
		ReconnectIntervalMS: 50, MaxReconnectAttempts: 3,
		HeartbeatIntervalMS: 60000, Logger: silentTestLogger{},
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
		if atomic.LoadInt32(&reconnects) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&reconnects) == 0 {
		t.Fatal("拨号失败应触发重连")
	}
	if bot.IsConnected() {
		t.Error("拨号失败期间 IsConnected 应为 false")
	}
}
