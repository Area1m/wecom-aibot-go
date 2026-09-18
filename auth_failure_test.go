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
