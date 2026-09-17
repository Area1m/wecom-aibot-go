package wecomaibot

import (
	"testing"
	"time"
)

func TestNextReconnectDelay(t *testing.T) {
	w := newWSConnection(Config{ReconnectIntervalMS: 1000}, silentTestLogger{})

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second}, // 兜底：attempt 归一化为 1
		{1, time.Second}, // 1s
		{2, 2 * time.Second},
		{5, 16 * time.Second},
		{6, 30 * time.Second},   // 32s 超过上限，取 30s
		{31, 30 * time.Second},  // 仍在安全位移范围内
		{32, 30 * time.Second},  // 超过 31 直接取上限
		{63, 30 * time.Second},  // 修复前这里溢出成 0s（忙重连）
		{100, 30 * time.Second}, // MaxReconnectAttempts=-1 时会走到这里
		{1000, 30 * time.Second},
	}
	for _, c := range cases {
		if got := w.nextReconnectDelay(c.attempt); got != c.want {
			t.Errorf("nextReconnectDelay(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

// 配置里重连间隔非法（<=0）时用默认值，不能返回 0 间隔。
func TestNextReconnectDelayFallsBackToDefault(t *testing.T) {
	w := newWSConnection(Config{ReconnectIntervalMS: 0}, silentTestLogger{})
	if got := w.nextReconnectDelay(1); got != time.Duration(DefaultReconnectInterval)*time.Millisecond {
		t.Errorf("非法配置应回落到默认间隔: got %v", got)
	}
}
