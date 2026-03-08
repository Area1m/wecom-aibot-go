package wecomaibot

import "time"

func (w *wsConnection) nextReconnectDelay(attempt int) time.Duration {
	base := time.Duration(w.cfg.ReconnectIntervalMS) * time.Millisecond
	if base <= 0 {
		base = time.Duration(DefaultReconnectInterval) * time.Millisecond
	}
	maxDelay := 30 * time.Second

	delay := base * time.Duration(1<<(attempt-1))
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}
