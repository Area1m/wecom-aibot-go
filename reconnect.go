package wecomaibot

import "time"

// nextReconnectDelay 返回第 attempt 次重连前的等待时长：base * 2^(attempt-1)，上限 30s。
func (w *wsConnection) nextReconnectDelay(attempt int) time.Duration {
	base := time.Duration(w.cfg.ReconnectIntervalMS) * time.Millisecond
	if base <= 0 {
		base = time.Duration(DefaultReconnectInterval) * time.Millisecond
	}
	maxDelay := 30 * time.Second

	if attempt < 1 {
		attempt = 1
	}
	// MaxReconnectAttempts = -1（无限重连）时 attempt 会一直涨，1<<(attempt-1) 在
	// attempt > 63 时溢出成 0/负数，会让退避失效变成 0 间隔忙重连，故超过 31 直接给上限。
	if attempt > 31 {
		return maxDelay
	}

	delay := base * time.Duration(1<<(attempt-1))
	if delay <= 0 || delay > maxDelay {
		return maxDelay
	}
	return delay
}
