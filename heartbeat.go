package wecomaibot

import (
	"context"
	"time"
)

func (w *wsConnection) startHeartbeat(parent context.Context) {
	w.stopHeartbeat()
	// 新一轮心跳：清零未收到 ACK 的连续计数。
	w.missedPongCount.Store(0)

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

// maxMissedPong 是判定连接已死所需的「连续未收到心跳 ACK」次数（对齐官方 Node SDK）。
const maxMissedPong = 2

// sendHeartbeat 按 HeartbeatIntervalMS 周期发一个 ping 帧做保活，并以「连续 maxMissedPong
// 次未收到心跳 ACK」作为死连接判据（对齐官方 Node SDK）：服务端会回 errcode=0 的 ACK
// （req_id 前缀 ping_），收到即清零计数；连续 2 次未收到则判定连接已死，主动断开触发重连。
//
// 此外连接失效还能由写失败（ping 写不出去）或服务端被动断开来发现，两者都会走重连；
// 半开连接（TCP 已死但无 FIN/RST）仍由 TCP_USER_TIMEOUT 兜底。
func (w *wsConnection) sendHeartbeat() {
	if w.missedPongCount.Load() >= maxMissedPong {
		w.logger.Warn("连续 %d 次心跳未收到 ACK，判定连接已死，主动断开以触发重连", maxMissedPong)
		if conn := w.getConn(); conn != nil {
			_ = conn.Close()
			w.setConn(nil)
		}
		return
	}
	w.missedPongCount.Add(1)

	frame := WsFrame{
		Cmd: WsCmdHeartbeat,
		Headers: WsHeaders{
			ReqID: GenerateReqID(WsCmdHeartbeat),
		},
	}
	if err := w.sendRawFrame(frame); err != nil {
		w.logger.Warn("发送心跳失败，主动断开以触发重连: %v", err)
		if conn := w.getConn(); conn != nil {
			_ = conn.Close()
			// 立刻置空，让 IsConnected() 如实反映状态（写失败没有「读循环报错」那么快）。
			w.setConn(nil)
		}
	}
}
