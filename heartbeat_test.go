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

// silentTestLogger 让测试输出不被 SDK 日志淹没。
type silentTestLogger struct{}

func (silentTestLogger) Debug(string, ...any) {}
func (silentTestLogger) Info(string, ...any)  {}
func (silentTestLogger) Warn(string, ...any)  {}
func (silentTestLogger) Error(string, ...any) {}

// fakeWSServer 模拟 openws 服务端：回应 aibot_subscribe，并对 ping 帧回心跳 ACK
// （对齐官方文档/Node SDK：服务端会回 errcode=0 的 ACK）。
type fakeWSServer struct {
	srv *httptest.Server

	subscribes int32
	pings      int32
}

func newFakeWSServer(t *testing.T) *fakeWSServer {
	f := &fakeWSServer{}
	upgrader := websocket.Upgrader{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			switch {
			case strings.HasPrefix(frame.Headers.ReqID, WsCmdSubscribe+"_"):
				atomic.AddInt32(&f.subscribes, 1)
				_ = conn.WriteJSON(WsFrame{Headers: WsHeaders{ReqID: frame.Headers.ReqID}})
			case frame.Cmd == WsCmdHeartbeat:
				atomic.AddInt32(&f.pings, 1)
				// 回心跳 ACK（透传 req_id、errcode=0），对齐官方文档与 Node SDK。
				_ = conn.WriteJSON(WsFrame{Headers: WsHeaders{ReqID: frame.Headers.ReqID}})
			}
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func newTestClient(t *testing.T, srv *fakeWSServer, intervalMS int) (*Client, *int32, *int32) {
	t.Helper()
	bot, err := NewClient(Config{
		BotID:                "bot",
		Secret:               "secret",
		WSURL:                "ws" + strings.TrimPrefix(srv.srv.URL, "http"),
		ReconnectIntervalMS:  50,
		MaxReconnectAttempts: 5,
		HeartbeatIntervalMS:  intervalMS,
		Logger:               silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}

	var disconnects, reconnects int32
	bot.OnDisconnected(func(context.Context, string) { atomic.AddInt32(&disconnects, 1) })
	bot.OnReconnecting(func(context.Context, int) { atomic.AddInt32(&reconnects, 1) })
	return bot, &disconnects, &reconnects
}

// 服务端回心跳 ACK 时，missedPongCount 被清零，客户端应持续保活不误断。
func TestHeartbeatKeepsAckingServerAlive(t *testing.T) {
	srv := newFakeWSServer(t)
	bot, disconnects, _ := newTestClient(t, srv, 20)

	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer bot.Disconnect()

	time.Sleep(400 * time.Millisecond)

	if got := atomic.LoadInt32(&srv.pings); got < 10 {
		t.Errorf("心跳发送次数不足: got %d, want >= 10", got)
	}
	if got := atomic.LoadInt32(disconnects); got != 0 {
		t.Errorf("有 ACK 服务端下不应断开连接: disconnects=%d", got)
	}
	if !bot.IsConnected() {
		t.Error("连接应保持存活")
	}
}

// 服务端不回心跳 ACK 时，客户端应在连续 maxMissedPong 次未收到 ACK 后判定连接已死并重连。
func TestHeartbeatDetectsNonAckingServerDead(t *testing.T) {
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
			if strings.HasPrefix(frame.Headers.ReqID, WsCmdSubscribe+"_") {
				atomic.AddInt32(&subscribes, 1)
				_ = conn.WriteJSON(WsFrame{Headers: WsHeaders{ReqID: frame.Headers.ReqID}})
				// 心跳帧故意不回 ACK，模拟死连接
			}
		}
	}))
	defer srv.Close()

	bot, err := NewClient(Config{
		BotID: "bot", Secret: "secret",
		WSURL:               "ws" + strings.TrimPrefix(srv.URL, "http"),
		ReconnectIntervalMS: 50, MaxReconnectAttempts: -1,
		HeartbeatIntervalMS: 50, Logger: silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer bot.Disconnect()

	// 连续 2 次未收到 ACK 后应主动断开并重连（subscribe 至少 2 次）。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&subscribes) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&subscribes); got < 2 {
		t.Errorf("心跳无 ACK 应判定连接已死并重连: subscribes=%d", got)
	}
}

// 服务端断开连接后，客户端应进入正常重连（心跳不再参与死连接判定，重连路径必须照常工作）。
func TestDisconnectTriggersReconnect(t *testing.T) {
	var subscribes int32
	var closeOnce sync.Once
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
			if strings.HasPrefix(frame.Headers.ReqID, WsCmdSubscribe+"_") {
				if atomic.AddInt32(&subscribes, 1) == 1 {
					// 认证成功后立刻把连接掐掉，模拟服务端被动断开。
					closeOnce.Do(func() { _ = conn.Close() })
					return
				}
				_ = conn.WriteJSON(WsFrame{Headers: WsHeaders{ReqID: frame.Headers.ReqID}})
			}
		}
	}))
	defer srv.Close()

	bot, err := NewClient(Config{
		BotID:                "bot",
		Secret:               "secret",
		WSURL:                "ws" + strings.TrimPrefix(srv.URL, "http"),
		ReconnectIntervalMS:  50,
		MaxReconnectAttempts: 5,
		HeartbeatIntervalMS:  20,
		Logger:               silentTestLogger{},
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
		t.Errorf("断线后应重新认证: subscribes=%d, reconnects=%d", got, atomic.LoadInt32(&reconnects))
	}
}

// 心跳写失败时应主动把 conn 置空（锁住注释承诺的「写失败 → 断开 → 触发重连」路径）。
func TestSendHeartbeatWriteFailureClosesConn(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := upgrader.Upgrade(w, r, nil)
		_ = conn.Close()
	}))
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("拨号失败: %v", err)
	}
	_ = conn.Close() // 主动关闭，使后续写必然失败

	w := newWSConnection(Config{}, silentTestLogger{})
	w.setConn(conn)
	w.sendHeartbeat()
	if w.isConnected() {
		t.Error("心跳写失败后 conn 应被置空")
	}
}

// LastReceivedAt 应在收到服务端数据（认证 ACK / 心跳 ACK）后更新，用于观测半开连接僵尸窗口。
func TestLastReceivedAtUpdates(t *testing.T) {
	srv := newFakeWSServer(t)
	bot, _, _ := newTestClient(t, srv, 50)

	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer bot.Disconnect()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !bot.LastReceivedAt().IsZero() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("认证成功后 LastReceivedAt 应已更新（非零）")
}
