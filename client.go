package wecomaibot

import (
	"context"
	"errors"
	"sync"
)

// ErrNotConnected 表示 WebSocket 当前不可用——尚未连接、连接已断开或已手动关闭。
// 此时回复/发送会立刻失败，可用 errors.Is 判断（例如等重连后再重试）：
//
//	if errors.Is(err, wecomaibot.ErrNotConnected) { /* 稍后重试 */ }
var ErrNotConnected = errors.New("WebSocket 连接不可用")

// ErrReconnectExhausted 表示重连次数耗尽（超过 MaxReconnectAttempts），客户端彻底停止。
// 可用 errors.Is 判断，对齐官方 Node SDK 的 WSReconnectExhaustedError。
var ErrReconnectExhausted = errors.New("超过最大重连次数")

// Client 是企业微信智能机器人客户端：负责连接、认证、重连、消息分发与回复发送。
// 用 NewClient 创建，注册好 OnXxx 回调后调用 Connect 开始工作。
type Client struct {
	cfg        Config
	logger     Logger
	ws         *wsConnection
	apiClient  *APIClient
	dispatcher *dispatcher

	ctx    context.Context
	cancel context.CancelFunc

	startedMu sync.Mutex
	started   bool
}

// NewClient 创建客户端并填充默认配置（心跳间隔、重连间隔、请求超时、默认日志器等）。
// BotID 或 Secret 为空时返回错误。
func NewClient(cfg Config) (*Client, error) {
	merged, err := fillDefaultConfig(cfg)
	if err != nil {
		return nil, err
	}
	logger := merged.Logger
	if logger == nil {
		logger = NewDefaultLogger("AiBotSDK-Go")
		merged.Logger = logger
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		cfg:        merged,
		logger:     logger,
		dispatcher: newDispatcher(logger),
		ctx:        ctx,
		cancel:     cancel,
	}

	c.apiClient = newAPIClient(logger, merged.RequestTimeoutMS, merged.MaxDownloadBytes)
	c.ws = newWSConnection(merged, logger)
	c.bindWSCallbacks()

	return c, nil
}

func fillDefaultConfig(cfg Config) (Config, error) {
	if cfg.BotID == "" {
		return Config{}, errors.New("BotID 不能为空")
	}
	if cfg.Secret == "" {
		return Config{}, errors.New("Secret 不能为空")
	}
	if cfg.ReconnectIntervalMS <= 0 {
		cfg.ReconnectIntervalMS = DefaultReconnectInterval
	}
	if cfg.MaxReconnectAttempts == 0 {
		cfg.MaxReconnectAttempts = DefaultMaxReconnect
	}
	if cfg.HeartbeatIntervalMS <= 0 {
		cfg.HeartbeatIntervalMS = DefaultHeartbeatInterval
	}
	if cfg.RequestTimeoutMS <= 0 {
		cfg.RequestTimeoutMS = DefaultRequestTimeout
	}
	if cfg.TCPUserTimeoutMS <= 0 {
		cfg.TCPUserTimeoutMS = DefaultTCPUserTimeout
	}
	if cfg.AuthTimeoutMS <= 0 {
		cfg.AuthTimeoutMS = DefaultAuthTimeout
	}
	if cfg.MaxDownloadBytes <= 0 {
		cfg.MaxDownloadBytes = DefaultMaxDownloadBytes
	}
	if cfg.WSURL == "" {
		cfg.WSURL = DefaultWSURL
	}
	return cfg, nil
}

func (c *Client) bindWSCallbacks() {
	c.ws.onConnected = func() {
		c.dispatcher.emitConnected(c.getCtx())
	}
	c.ws.onAuthenticated = func() {
		c.dispatcher.emitAuthenticated(c.getCtx())
	}
	c.ws.onDisconnected = func(reason string) {
		c.dispatcher.emitDisconnected(c.getCtx(), reason)
	}
	c.ws.onReconnecting = func(attempt int) {
		c.dispatcher.emitReconnecting(c.getCtx(), attempt)
	}
	c.ws.onError = func(err error) {
		c.dispatcher.emitError(c.getCtx(), err)
	}
	c.ws.onMessage = func(frame WsFrameRaw) {
		c.dispatcher.dispatchFrame(c.getCtx(), frame)
	}
	c.ws.onStopped = func() {
		// 重连耗尽后复位 started，让后续 Connect() 能真正重启而非静默空操作。
		c.startedMu.Lock()
		c.started = false
		c.startedMu.Unlock()
	}
}

// getCtx 返回当前上下文。c.ctx 会被 Connect（Disconnect 后重启）改写，而这些回调
// 在 ws 的 goroutine 里异步读取，必须用同一把锁保护，避免数据竞态。
func (c *Client) getCtx() context.Context {
	c.startedMu.Lock()
	defer c.startedMu.Unlock()
	return c.ctx
}

// Connect 开始连接（异步）：建连、认证都在后台 goroutine 里进行，认证成功触发
// OnAuthenticated，中途失败会按配置自动重连并触发 OnReconnecting/OnError。
// 重复调用是幂等的，已在运行时直接返回 nil。
func (c *Client) Connect() error {
	c.startedMu.Lock()
	defer c.startedMu.Unlock()

	if c.started {
		return nil
	}
	if c.ctx == nil || c.ctx.Err() != nil {
		c.ctx, c.cancel = context.WithCancel(context.Background())
	}
	c.started = true
	c.ws.start(c.ctx)
	return nil
}

// Disconnect 手动断开并停止重连（不会触发重连）。已在断开状态时是空操作。
func (c *Client) Disconnect() {
	c.startedMu.Lock()
	defer c.startedMu.Unlock()

	if !c.started {
		return
	}
	c.started = false
	if c.cancel != nil {
		c.cancel()
	}
	c.ws.disconnect()
}

// IsConnected 返回当前 WebSocket 是否处于已连接状态（重连等待期间为 false）。
func (c *Client) IsConnected() bool {
	return c.ws.isConnected()
}

// API 返回底层 HTTP 能力客户端（目前用于文件下载等非 WebSocket 请求）。
func (c *Client) API() *APIClient {
	return c.apiClient
}

// OnConnected 注册连接建立（尚未认证）回调，可注册多个，均在独立 goroutine 中执行。
func (c *Client) OnConnected(handler func(context.Context)) {
	c.dispatcher.addConnectedHandler(handler)
}

// OnAuthenticated 注册认证成功回调——收到 aibot_subscribe 的成功 ACK 后触发。
func (c *Client) OnAuthenticated(handler func(context.Context)) {
	c.dispatcher.addAuthenticatedHandler(handler)
}

// OnDisconnected 注册断开回调，reason 为断开原因（服务端断开、心跳写失败、手动断开等）。
func (c *Client) OnDisconnected(handler func(context.Context, string)) {
	c.dispatcher.addDisconnectedHandler(handler)
}

// OnReconnecting 注册重连回调，attempt 为第几次重连尝试。
func (c *Client) OnReconnecting(handler func(context.Context, int)) {
	c.dispatcher.addReconnectingHandler(handler)
}

// OnError 注册错误回调（建连失败、认证失败、超过最大重连次数等）。
func (c *Client) OnError(handler func(context.Context, error)) {
	c.dispatcher.addErrorHandler(handler)
}

// OnMessage 注册所有消息的通用回调（先于按类型回调触发）。
func (c *Client) OnMessage(handler func(context.Context, BaseMessage)) {
	c.dispatcher.addMessageHandler(handler)
}

// OnText 注册文本消息回调。
func (c *Client) OnText(handler func(context.Context, TextMessage)) {
	c.dispatcher.addTextHandler(handler)
}

// OnImage 注册图片消息回调。
func (c *Client) OnImage(handler func(context.Context, ImageMessage)) {
	c.dispatcher.addImageHandler(handler)
}

// OnMixed 注册图文混排消息回调。
func (c *Client) OnMixed(handler func(context.Context, MixedMessage)) {
	c.dispatcher.addMixedHandler(handler)
}

// OnVoice 注册语音消息回调。
func (c *Client) OnVoice(handler func(context.Context, VoiceMessage)) {
	c.dispatcher.addVoiceHandler(handler)
}

// OnFile 注册文件消息回调。
func (c *Client) OnFile(handler func(context.Context, FileMessage)) {
	c.dispatcher.addFileHandler(handler)
}

// OnVideo 注册视频消息回调（仅单聊）。
func (c *Client) OnVideo(handler func(context.Context, VideoMessage)) {
	c.dispatcher.addVideoHandler(handler)
}

// OnEvent 注册所有事件回调（先于按事件类型回调触发）。
func (c *Client) OnEvent(handler func(context.Context, EventMessage)) {
	c.dispatcher.addEventHandler(handler)
}

// OnEnterChat 注册进入会话事件回调（EventTypeEnterChat）。
func (c *Client) OnEnterChat(handler func(context.Context, EventMessage)) {
	c.dispatcher.addEnterChatHandler(handler)
}

// OnTemplateCardEvent 注册模板卡片事件回调（EventTypeTemplateCardEvent）。
func (c *Client) OnTemplateCardEvent(handler func(context.Context, EventMessage)) {
	c.dispatcher.addTemplateCardEventHandler(handler)
}

// OnFeedbackEvent 注册用户反馈事件回调（EventTypeFeedbackEvent）。
func (c *Client) OnFeedbackEvent(handler func(context.Context, EventMessage)) {
	c.dispatcher.addFeedbackEventHandler(handler)
}

// OnDisconnectedEvent 注册连接断开事件回调（EventTypeDisconnectedEvent）：当同一 BotID 被
// 新连接顶掉时，服务端会先发该事件再主动断开旧连接。可用它排查「多连接互踢」。
func (c *Client) OnDisconnectedEvent(handler func(context.Context, EventMessage)) {
	c.dispatcher.addDisconnectedEventHandler(handler)
}

func (c *Client) replyByReqID(reqID string, body any, cmd string) (WsFrameRaw, error) {
	if reqID == "" {
		return WsFrameRaw{}, errors.New("缺少 reqID")
	}
	return c.ws.sendReply(reqID, body, cmd)
}

// HasPendingAck 返回指定 req_id 是否还有正在等待回执的回复（对齐官方 Node SDK，供流式场景避免积压）。
func (c *Client) HasPendingAck(reqID string) bool {
	return c.ws.hasPendingAck(reqID)
}
