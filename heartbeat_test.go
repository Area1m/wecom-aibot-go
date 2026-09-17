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

// fakeWSServer 模拟 openws 服务端：回应 aibot_subscribe，但对客户端 ping 帧
// 不做任何响应（与 WeCom 服务端实测行为一致：既不回 pong，也不回空 cmd ACK）。
type fakeWSServer struct {
	srv *httptest.Server

	subscribes int32
	pings      int32

	serverPing bool

	pongMu     sync.Mutex
	pongReqIDs []string
}

func newFakeWSServer(t *testing.T) *fakeWSServer {
	f := &fakeWSServer{serverPing: false}
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
				if f.serverPing {
					_ = conn.WriteJSON(WsFrame{
						Cmd:     WsCmdHeartbeat,
						Headers: WsHeaders{ReqID: "server_ping_1"},
					})
				}
			case frame.Cmd == WsCmdPong:
				f.pongMu.Lock()
				f.pongReqIDs = append(f.pongReqIDs, frame.Headers.ReqID)
				f.pongMu.Unlock()
			case frame.Cmd == WsCmdHeartbeat:
				// 关键：服务端不回 ACK，见 fakeWSServer 注释。
				atomic.AddInt32(&f.pings, 1)
			}
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeWSServer) pongs() []string {
	f.pongMu.Lock()
	defer f.pongMu.Unlock()
	return append([]string(nil), f.pongReqIDs...)
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

// 服务端对 ping 静默（WeCom 实测行为）时，客户端必须继续保活，不能自己掐断连接。
// 修复前：missedPongCount 每 2 个心跳周期就 Close 一次，本用例会失败。
func TestHeartbeatKeepsSilentServerAlive(t *testing.T) {
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
		t.Errorf("静默服务端下不应断开连接: disconnects=%d", got)
	}
	if !bot.IsConnected() {
		t.Error("连接应保持存活")
	}
}

// 服务端主动发 ping 时应回 pong（防御性处理，当前服务端不发）。
func TestHeartbeatRepliesPongToServerPing(t *testing.T) {
	srv := newFakeWSServer(t)
	srv.serverPing = true
	bot, _, _ := newTestClient(t, srv, 20)

	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer bot.Disconnect()

	// 等服务端 ping 被回 pong，同时等至少一次心跳（心跳周期 20ms，晚于认证帧）。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(srv.pongs()) > 0 && atomic.LoadInt32(&srv.pings) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	pongs := srv.pongs()
	if len(pongs) != 1 || pongs[0] != "server_ping_1" {
		t.Errorf("应回带原 req_id 的 pong: got %v", pongs)
	}
	if got := atomic.LoadInt32(&srv.pings); got == 0 {
		t.Error("客户端应仍在发送心跳")
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
