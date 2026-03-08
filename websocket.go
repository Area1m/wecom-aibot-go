package wecomaibot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	missedPongCount   int32
	maxMissedPong     int32

	replyQueueMu sync.Mutex
	replyQueues  map[string]*replyQueue
	pendingAcks  map[string]chan ackResult

	replyAckTimeout  time.Duration
	maxReplyQueueLen int

	onConnected     func()
	onAuthenticated func()
	onDisconnected  func(reason string)
	onReconnecting  func(attempt int)
	onError         func(err error)
	onMessage       func(frame WsFrameRaw)
}

type replyQueue struct {
	reqID string
	ch    chan replyTask
}

type replyTask struct {
	frame  WsFrame
	result chan replyResult
}

type replyResult struct {
	frame WsFrameRaw
	err   error
}

type ackResult struct {
	frame WsFrameRaw
	err   error
}

func newWSConnection(cfg Config, logger Logger) *wsConnection {
	return &wsConnection{
		cfg:              cfg,
		logger:           logger,
		maxMissedPong:    2,
		replyQueues:      make(map[string]*replyQueue),
		pendingAcks:      make(map[string]chan ackResult),
		replyAckTimeout:  5 * time.Second,
		maxReplyQueueLen: 100,
	}
}

func (w *wsConnection) start(ctx context.Context) {
	if !w.started.CompareAndSwap(false, true) {
		return
	}
	w.manualClose.Store(false)
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

		reason, err := w.runSession(ctx)
		if reason != "" {
			w.emitDisconnected(reason)
		}
		if err != nil && !w.manualClose.Load() {
			w.emitError(err)
		}
	}
}

func (w *wsConnection) runSession(ctx context.Context) (string, error) {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, w.cfg.WSURL, nil)
	if err != nil {
		return "", fmt.Errorf("建立 WebSocket 连接失败: %w", err)
	}

	w.setConn(conn)
	w.resetReconnectAttempts()
	atomic.StoreInt32(&w.missedPongCount, 0)
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
			return
		}
		w.startHeartbeat(ctx)
		w.emitAuthenticated()
		return
	}

	if strings.HasPrefix(reqID, WsCmdHeartbeat+"_") {
		if frame.ErrCode == 0 {
			atomic.StoreInt32(&w.missedPongCount, 0)
			return
		}
		w.logger.Warn("心跳 ACK 异常: errcode=%d errmsg=%s", frame.ErrCode, frame.ErrMsg)
		return
	}

	w.emitMessage(frame)
}

func (w *wsConnection) disconnect() {
	w.manualClose.Store(true)
	w.started.Store(false)
	w.stopHeartbeat()
	w.notifyPendingAckError(errors.New("连接已手动关闭"))
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

	queue := w.getOrCreateReplyQueue(reqID)
	task := replyTask{
		frame: WsFrame{
			Cmd: cmd,
			Headers: WsHeaders{
				ReqID: reqID,
			},
			Body: body,
		},
		result: make(chan replyResult, 1),
	}

	select {
	case queue.ch <- task:
	default:
		return WsFrameRaw{}, fmt.Errorf("reqID=%s 的回复队列已满", reqID)
	}

	res := <-task.result
	return res.frame, res.err
}

func (w *wsConnection) getOrCreateReplyQueue(reqID string) *replyQueue {
	w.replyQueueMu.Lock()
	defer w.replyQueueMu.Unlock()

	if q, ok := w.replyQueues[reqID]; ok {
		return q
	}
	q := &replyQueue{
		reqID: reqID,
		ch:    make(chan replyTask, w.maxReplyQueueLen),
	}
	w.replyQueues[reqID] = q
	go w.processReplyQueue(q)
	return q
}

func (w *wsConnection) processReplyQueue(queue *replyQueue) {
	idleTimer := time.NewTimer(2 * time.Minute)
	defer idleTimer.Stop()

	for {
		select {
		case task := <-queue.ch:
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(2 * time.Minute)
			frame, err := w.sendAndWaitAck(queue.reqID, task.frame)
			task.result <- replyResult{frame: frame, err: err}
		case <-idleTimer.C:
			w.replyQueueMu.Lock()
			delete(w.replyQueues, queue.reqID)
			w.replyQueueMu.Unlock()
			return
		}
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
		return errors.New("WebSocket 未连接")
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
