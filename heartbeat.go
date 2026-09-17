package wecomaibot

import (
	"context"
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

// sendHeartbeat 按 HeartbeatIntervalMS 周期发一个 ping 帧做保活。
//
// 注意：WeCom 服务端收到客户端 ping 帧后不做任何响应——既不回 pong，也不回空 cmd
// 的 ACK，与 req_id 形式无关（逐帧抓包实测）。因此不能把「没收到心跳 ACK」当作连接
// 已死：一旦这么做，长时间静默的机器人每 2 个心跳周期就会被自己掐断一次，形成
// 「断开→重连→再断开」的死循环（默认 30s 间隔时约 90s 一循环）。
//
// 连接是否已死改由以下两条路径发现，两者都会让读循环返回错误并进入正常重连：
//   - 写失败：ping 写不出去说明连接不可用，这里主动 Close 提前触发重连；
//   - 服务端被动断开：连接被服务端关闭时读循环会立刻返回错误。
func (w *wsConnection) sendHeartbeat() {
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
