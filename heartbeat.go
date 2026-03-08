package wecomaibot

import (
	"context"
	"sync/atomic"
	"time"
)

func (w *wsConnection) startHeartbeat(parent context.Context) {
	w.stopHeartbeat()

	ctx, cancel := context.WithCancel(parent)
	w.heartbeatCancelMu.Lock()
	w.heartbeatCancel = cancel
	w.heartbeatCancelMu.Unlock()

	go func() {
		ticker := time.NewTicker(time.Duration(w.cfg.HeartbeatIntervalMS) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.sendHeartbeat()
			}
		}
	}()
}

func (w *wsConnection) stopHeartbeat() {
	w.heartbeatCancelMu.Lock()
	defer w.heartbeatCancelMu.Unlock()
	if w.heartbeatCancel != nil {
		w.heartbeatCancel()
		w.heartbeatCancel = nil
	}
}

func (w *wsConnection) sendHeartbeat() {
	missed := atomic.LoadInt32(&w.missedPongCount)
	if missed >= w.maxMissedPong {
		w.logger.Warn("连续 %d 次未收到心跳 ACK，主动断开连接", missed)
		conn := w.getConn()
		if conn != nil {
			_ = conn.Close()
		}
		return
	}

	atomic.AddInt32(&w.missedPongCount, 1)
	frame := WsFrame{
		Cmd: WsCmdHeartbeat,
		Headers: WsHeaders{
			ReqID: GenerateReqID(WsCmdHeartbeat),
		},
	}
	if err := w.sendRawFrame(frame); err != nil {
		w.logger.Error("发送心跳失败: %v", err)
	}
}
