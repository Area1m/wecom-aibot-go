package wecomaibot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type wsConnection struct {
	cfg    Config
	logger Logger

	connMu sync.RWMutex
	conn   *websocket.Conn

	writeMu sync.Mutex

	started     atomic.Bool
	manualClose atomic.Bool

	reconnectAttempts int
	reconnectMu       sync.Mutex

	heartbeatCancelMu sync.Mutex
	heartbeatCancel   context.CancelFunc

	replyQueueMu sync.Mutex
	replyQueues  map[string]*replyQueue
	pendingAcks  map[string]chan ackResult

	replyAckTimeout time.Duration

	onConnected     func()
	onAuthenticated func()
	onDisconnected  func(reason string)
	onReconnecting  func(attempt int)
	onError         func(err error)
	onMessage       func(frame WsFrameRaw)
}

// replyQueue 串行化同一 req_id 的回复：mu 保证前一条回复（含 ACK 等待）完成后
// 才发下一条；refs 是引用计数，由 replyQueueMu 保护，归零时从 map 删除，避免
// 为每个消息 ID 永久保留条目。
type replyQueue struct {
	reqID string
	mu    sync.Mutex
	refs  int
}

type ackResult struct {
	frame WsFrameRaw
	err   error
}

func newWSConnection(cfg Config, logger Logger) *wsConnection {
	return &wsConnection{
		cfg:             cfg,
		logger:          logger,
		replyQueues:     make(map[string]*replyQueue),
		pendingAcks:     make(map[string]chan ackResult),
		replyAckTimeout: 5 * time.Second,
	}
}

func (w *wsConnection) start(ctx context.Context) {
	if !w.started.CompareAndSwap(false, true) {
		return
	}
	w.manualClose.Store(false)
	// 每次重新启动都清零重连计数：否则上一轮重连耗尽后，Disconnect + Connect
	// 重启时 reconnectAttempts 仍停在 max，拨号失败一次就立刻再次放弃。
	w.resetReconnectAttempts()
	go w.run(ctx)
}

func (w *wsConnection) run(ctx context.Context) {
	first := true
	for {
		if ctx.Err() != nil || w.manualClose.Load() {
			return
		}

		if !first {
			attempt, ok := w.registerReconnectAttempt()
			if !ok {
				w.emitError(errors.New("超过最大重连次数"))
				return
			}
			delay := w.nextReconnectDelay(attempt)
			w.logger.Info("%dms 后开始重连，第 %d 次", delay.Milliseconds(), attempt)
			w.emitReconnecting(attempt)

			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		first = false

		reason, err := w.runSessionSafely(ctx)
		if reason != "" {
			w.emitDisconnected(reason)
		}
		if err != nil && !w.manualClose.Load() {
			w.emitError(err)
		}
	}
}

// runSessionSafely 给会话 goroutine 兜 panic：SDK 自己的 goroutine 里一旦 panic
// 会把整个客户端打死（run 退出但 started 仍为 true，用户连 Connect 都救不回来，
// 表现为永久假死、不报错、不重连）。这里把 panic 转成错误，交给外层按普通断线走重连。
func (w *wsConnection) runSessionSafely(ctx context.Context) (reason string, err error) {
	defer func() {
		rec := recover()
		if rec == nil {
			return
		}
		err = fmt.Errorf("会话处理 panic: %v", rec)
		reason = err.Error()
		w.logger.Error("%v\n%s", err, debug.Stack())
		w.stopHeartbeat()
		w.notifyPendingAckError(err)
		if conn := w.getConn(); conn != nil {
			_ = conn.Close()
			w.setConn(nil)
		}
	}()
	return w.runSession(ctx)
}

func (w *wsConnection) runSession(ctx context.Context) (string, error) {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		NetDialContext:   w.dial,
	}
	conn, _, err := dialer.DialContext(ctx, w.cfg.WSURL, nil)
	if err != nil {
		return "", fmt.Errorf("建立 WebSocket 连接失败: %w", err)
	}

	w.setConn(conn)
	w.emitConnected()

	if err := w.sendAuth(); err != nil {
		_ = conn.Close()
		w.setConn(nil)
		return "发送认证帧失败", err
	}

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			w.stopHeartbeat()
			w.notifyPendingAckError(fmt.Errorf("连接已关闭: %w", err))
			_ = conn.Close()
			w.setConn(nil)
			if w.manualClose.Load() {
				return "手动断开连接", nil
			}
			return fmt.Sprintf("连接断开: %v", err), nil
		}

		var frame WsFrameRaw
		if err := json.Unmarshal(data, &frame); err != nil {
			w.emitError(fmt.Errorf("解析消息失败: %w", err))
			continue
		}
		w.handleFrame(ctx, frame)
	}
}

func (w *wsConnection) handleFrame(ctx context.Context, frame WsFrameRaw) {
	// 服务端保活 ping → 回 pong；服务端 pong → 忽略（防御性处理，当前服务端不发）。
	switch frame.Cmd {
	case WsCmdHeartbeat:
		_ = w.sendRawFrame(WsFrame{Cmd: WsCmdPong, Headers: WsHeaders{ReqID: frame.Headers.ReqID}})
		return
	case WsCmdPong:
		return
	}

	if frame.Cmd == WsCmdCallback || frame.Cmd == WsCmdEventCallback {
		w.emitMessage(frame)
		return
	}

	reqID := frame.Headers.ReqID
	if reqID == "" {
		return
	}

	if w.resolveAck(reqID, frame) {
		return
	}

	if strings.HasPrefix(reqID, WsCmdSubscribe+"_") {
		if frame.ErrCode != 0 {
			w.emitError(fmt.Errorf("认证失败: errcode=%d errmsg=%s", frame.ErrCode, frame.ErrMsg))
			// 认证被拒绝时主动关闭连接，让读循环返回并走正常重连；否则会停在
			// 「已连接但未认证」的假死态——无心跳、无消息，IsConnected 仍为 true。
			if conn := w.getConn(); conn != nil {
				_ = conn.Close()
				w.setConn(nil)
			}
			return
		}
		// 认证成功才算一次稳定连接，此时才清零重连失败计数。不能放在拨号成功处：
		// 那样「拨号成功但认证失败」每次都会清零，绕过 MaxReconnectAttempts 无限重试。
		w.resetReconnectAttempts()
		w.startHeartbeat(ctx)
		w.emitAuthenticated()
		return
	}

	// 心跳 ACK：服务端回显 ping 的 req_id，但当前服务端并不会回（见 sendHeartbeat）。
	// 这里只按 ACK 静默处理、不透传给消息处理器，避免把 ACK 当成消息。
	if strings.HasPrefix(reqID, WsCmdHeartbeat+"_") || frame.Cmd == "" {
		if frame.ErrCode != 0 {
			w.logger.Warn("心跳 ACK 异常: reqid=%s errcode=%d errmsg=%s", reqID, frame.ErrCode, frame.ErrMsg)
		}
		return
	}

	w.emitMessage(frame)
}

func (w *wsConnection) disconnect() {
	w.manualClose.Store(true)
	w.started.Store(false)
	w.stopHeartbeat()
	w.notifyPendingAckError(fmt.Errorf("连接已手动关闭: %w", ErrNotConnected))
	conn := w.getConn()
	if conn != nil {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "manual disconnect"), time.Now().Add(time.Second))
		_ = conn.Close()
	}
	w.setConn(nil)
}

func (w *wsConnection) isConnected() bool {
	conn := w.getConn()
	return conn != nil
}

func (w *wsConnection) sendReply(reqID string, body any, cmd string) (WsFrameRaw, error) {
	if reqID == "" {
		return WsFrameRaw{}, errors.New("reqID 不能为空")
	}
	if cmd == "" {
		cmd = WsCmdResponse
	}

	// 同一 req_id 的回复串行执行：acquire 加引用计数（保证清理安全），mu 保证
	// 前一条回复的 ACK 等待完成前不发送下一条。发送+等待 ACK 在调用方 goroutine
	// 内完成（reply 类 API 只从用户 handler 调用，不会阻塞读循环）。
	q := w.acquireReplyQueue(reqID)
	defer w.releaseReplyQueue(q)

	q.mu.Lock()
	defer q.mu.Unlock()

	return w.sendAndWaitAck(reqID, WsFrame{
		Cmd: cmd,
		Headers: WsHeaders{
			ReqID: reqID,
		},
		Body: body,
	})
}

func (w *wsConnection) acquireReplyQueue(reqID string) *replyQueue {
	w.replyQueueMu.Lock()
	defer w.replyQueueMu.Unlock()

	q, ok := w.replyQueues[reqID]
	if !ok {
		q = &replyQueue{reqID: reqID}
		w.replyQueues[reqID] = q
	}
	q.refs++
	return q
}

func (w *wsConnection) releaseReplyQueue(q *replyQueue) {
	w.replyQueueMu.Lock()
	defer w.replyQueueMu.Unlock()

	// refs 归零说明没有调用方再持有该队列，可安全删除。acquire/release 都持
	// replyQueueMu，refs>0 期间队列必在 map 中，因此这里 delete 不会误删别的条目。
	if q.refs--; q.refs == 0 {
		delete(w.replyQueues, q.reqID)
	}
}

func (w *wsConnection) sendAndWaitAck(reqID string, frame WsFrame) (WsFrameRaw, error) {
	ackCh := make(chan ackResult, 1)
	w.replyQueueMu.Lock()
	w.pendingAcks[reqID] = ackCh
	w.replyQueueMu.Unlock()

	if err := w.sendRawFrame(frame); err != nil {
		w.replyQueueMu.Lock()
		delete(w.pendingAcks, reqID)
		w.replyQueueMu.Unlock()
		return WsFrameRaw{}, err
	}

	timer := time.NewTimer(w.replyAckTimeout)
	defer timer.Stop()

	select {
	case ack := <-ackCh:
		w.replyQueueMu.Lock()
		delete(w.pendingAcks, reqID)
		w.replyQueueMu.Unlock()
		if ack.err != nil {
			return WsFrameRaw{}, ack.err
		}
		if ack.frame.ErrCode != 0 {
			return ack.frame, fmt.Errorf("回复 ACK 错误: errcode=%d errmsg=%s", ack.frame.ErrCode, ack.frame.ErrMsg)
		}
		return ack.frame, nil
	case <-timer.C:
		w.replyQueueMu.Lock()
		delete(w.pendingAcks, reqID)
		w.replyQueueMu.Unlock()
		return WsFrameRaw{}, fmt.Errorf("等待回复 ACK 超时: reqID=%s", reqID)
	}
}

func (w *wsConnection) resolveAck(reqID string, frame WsFrameRaw) bool {
	w.replyQueueMu.Lock()
	ackCh, ok := w.pendingAcks[reqID]
	w.replyQueueMu.Unlock()
	if !ok {
		return false
	}

	select {
	case ackCh <- ackResult{frame: frame}:
	default:
	}
	return true
}

func (w *wsConnection) notifyPendingAckError(err error) {
	w.replyQueueMu.Lock()
	defer w.replyQueueMu.Unlock()
	for reqID, ackCh := range w.pendingAcks {
		select {
		case ackCh <- ackResult{err: fmt.Errorf("reqID=%s: %w", reqID, err)}:
		default:
		}
		delete(w.pendingAcks, reqID)
	}
}

func (w *wsConnection) sendRawFrame(frame WsFrame) error {
	conn := w.getConn()
	if conn == nil {
		return ErrNotConnected
	}

	payload, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("序列化发送帧失败: %w", err)
	}

	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		return fmt.Errorf("发送 WebSocket 帧失败: %w", err)
	}
	return nil
}

func (w *wsConnection) setConn(conn *websocket.Conn) {
	w.connMu.Lock()
	defer w.connMu.Unlock()
	w.conn = conn
}

func (w *wsConnection) getConn() *websocket.Conn {
	w.connMu.RLock()
	defer w.connMu.RUnlock()
	return w.conn
}

func (w *wsConnection) registerReconnectAttempt() (int, bool) {
	w.reconnectMu.Lock()
	defer w.reconnectMu.Unlock()

	max := w.cfg.MaxReconnectAttempts
	if max != -1 && w.reconnectAttempts >= max {
		return 0, false
	}
	w.reconnectAttempts++
	return w.reconnectAttempts, true
}

func (w *wsConnection) resetReconnectAttempts() {
	w.reconnectMu.Lock()
	defer w.reconnectMu.Unlock()
	w.reconnectAttempts = 0
}

func (w *wsConnection) emitConnected() {
	if w.onConnected != nil {
		go w.onConnected()
	}
}

func (w *wsConnection) emitAuthenticated() {
	if w.onAuthenticated != nil {
		go w.onAuthenticated()
	}
}

func (w *wsConnection) emitDisconnected(reason string) {
	if w.onDisconnected != nil {
		go w.onDisconnected(reason)
	}
}

func (w *wsConnection) emitReconnecting(attempt int) {
	if w.onReconnecting != nil {
		go w.onReconnecting(attempt)
	}
}

func (w *wsConnection) emitError(err error) {
	if err == nil {
		return
	}
	if w.onError != nil {
		go w.onError(err)
	}
}

func (w *wsConnection) emitMessage(frame WsFrameRaw) {
	if w.onMessage != nil {
		go w.onMessage(frame)
	}
}
