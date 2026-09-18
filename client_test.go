package wecomaibot

import (
	"errors"
	"testing"
	"time"
)

func TestNewClientRequiresCredentials(t *testing.T) {
	if _, err := NewClient(Config{Secret: "s", Logger: silentTestLogger{}}); err == nil {
		t.Error("缺少 BotID 应返回错误")
	}
	if _, err := NewClient(Config{BotID: "b", Logger: silentTestLogger{}}); err == nil {
		t.Error("缺少 Secret 应返回错误")
	}
}

func TestNewClientFillsDefaults(t *testing.T) {
	bot, err := NewClient(Config{BotID: "b", Secret: "s"})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	if bot.cfg.HeartbeatIntervalMS != DefaultHeartbeatInterval {
		t.Errorf("HeartbeatIntervalMS 默认值错误: %d", bot.cfg.HeartbeatIntervalMS)
	}
	if bot.cfg.ReconnectIntervalMS != DefaultReconnectInterval {
		t.Errorf("ReconnectIntervalMS 默认值错误: %d", bot.cfg.ReconnectIntervalMS)
	}
	if bot.cfg.MaxReconnectAttempts != DefaultMaxReconnect {
		t.Errorf("MaxReconnectAttempts 默认值错误: %d", bot.cfg.MaxReconnectAttempts)
	}
	if bot.cfg.RequestTimeoutMS != DefaultRequestTimeout {
		t.Errorf("RequestTimeoutMS 默认值错误: %d", bot.cfg.RequestTimeoutMS)
	}
	if bot.cfg.TCPUserTimeoutMS != DefaultTCPUserTimeout {
		t.Errorf("TCPUserTimeoutMS 默认值错误: %d", bot.cfg.TCPUserTimeoutMS)
	}
	if bot.cfg.AuthTimeoutMS != DefaultAuthTimeout {
		t.Errorf("AuthTimeoutMS 默认值错误: %d", bot.cfg.AuthTimeoutMS)
	}
	if bot.cfg.WSURL != DefaultWSURL {
		t.Errorf("WSURL 默认值错误: %s", bot.cfg.WSURL)
	}
	if bot.cfg.Logger == nil {
		t.Error("Logger 应有默认实现")
	}
}

// 未连接时的回复必须返回 ErrNotConnected（而不是靠字符串匹配），调用方可据此等重连后重试。
func TestReplyWhenNotConnected(t *testing.T) {
	bot, err := NewClient(Config{BotID: "b", Secret: "s", Logger: silentTestLogger{}})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}

	msg := TextMessage{BaseMessage: BaseMessage{ReqID: "req_1"}}
	_, err = bot.ReplyStreamByID(msg, GenerateReqID("stream"), "hi", true, nil, nil)
	if !errors.Is(err, ErrNotConnected) {
		t.Errorf("应返回 ErrNotConnected: got %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = bot.ReplyStreamByID(msg, GenerateReqID("stream"), "hi again", true, nil, nil)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("未连接时回复应立即失败，而不是阻塞")
	}
}

func TestIsConnectedIsFalseBeforeConnect(t *testing.T) {
	bot, err := NewClient(Config{BotID: "b", Secret: "s", Logger: silentTestLogger{}})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	if bot.IsConnected() {
		t.Error("未 Connect 前 IsConnected 应为 false")
	}
	bot.Disconnect() // 未启动时是空操作，不应 panic
}
