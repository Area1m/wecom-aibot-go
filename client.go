package wecomaibot

import (
	"context"
	"errors"
	"sync"
)

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

func NewClient(cfg Config) *Client {
	merged := fillDefaultConfig(cfg)
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

	c.apiClient = newAPIClient(logger, merged.RequestTimeoutMS)
	c.ws = newWSConnection(merged, logger)
	c.bindWSCallbacks()

	return c
}

func fillDefaultConfig(cfg Config) Config {
	if cfg.BotID == "" {
		panic("BotID 不能为空")
	}
	if cfg.Secret == "" {
		panic("Secret 不能为空")
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
	if cfg.WSURL == "" {
		cfg.WSURL = DefaultWSURL
	}
	return cfg
}

func (c *Client) bindWSCallbacks() {
	c.ws.onConnected = func() {
		c.dispatcher.emitConnected(c.ctx)
	}
	c.ws.onAuthenticated = func() {
		c.dispatcher.emitAuthenticated(c.ctx)
	}
	c.ws.onDisconnected = func(reason string) {
		c.dispatcher.emitDisconnected(c.ctx, reason)
	}
	c.ws.onReconnecting = func(attempt int) {
		c.dispatcher.emitReconnecting(c.ctx, attempt)
	}
	c.ws.onError = func(err error) {
		c.dispatcher.emitError(c.ctx, err)
	}
	c.ws.onMessage = func(frame WsFrameRaw) {
		c.dispatcher.dispatchFrame(c.ctx, frame)
	}
}

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

func (c *Client) IsConnected() bool {
	return c.ws.isConnected()
}

func (c *Client) API() *APIClient {
	return c.apiClient
}

func (c *Client) OnConnected(handler func(context.Context)) {
	c.dispatcher.addConnectedHandler(handler)
}

func (c *Client) OnAuthenticated(handler func(context.Context)) {
	c.dispatcher.addAuthenticatedHandler(handler)
}

func (c *Client) OnDisconnected(handler func(context.Context, string)) {
	c.dispatcher.addDisconnectedHandler(handler)
}

func (c *Client) OnReconnecting(handler func(context.Context, int)) {
	c.dispatcher.addReconnectingHandler(handler)
}

func (c *Client) OnError(handler func(context.Context, error)) {
	c.dispatcher.addErrorHandler(handler)
}

func (c *Client) OnMessage(handler func(context.Context, BaseMessage)) {
	c.dispatcher.addMessageHandler(handler)
}

func (c *Client) OnText(handler func(context.Context, TextMessage)) {
	c.dispatcher.addTextHandler(handler)
}

func (c *Client) OnImage(handler func(context.Context, ImageMessage)) {
	c.dispatcher.addImageHandler(handler)
}

func (c *Client) OnMixed(handler func(context.Context, MixedMessage)) {
	c.dispatcher.addMixedHandler(handler)
}

func (c *Client) OnVoice(handler func(context.Context, VoiceMessage)) {
	c.dispatcher.addVoiceHandler(handler)
}

func (c *Client) OnFile(handler func(context.Context, FileMessage)) {
	c.dispatcher.addFileHandler(handler)
}

func (c *Client) OnEvent(handler func(context.Context, EventMessage)) {
	c.dispatcher.addEventHandler(handler)
}

func (c *Client) OnEnterChat(handler func(context.Context, EventMessage)) {
	c.dispatcher.addEnterChatHandler(handler)
}

func (c *Client) OnTemplateCardEvent(handler func(context.Context, EventMessage)) {
	c.dispatcher.addTemplateCardEventHandler(handler)
}

func (c *Client) OnFeedbackEvent(handler func(context.Context, EventMessage)) {
	c.dispatcher.addFeedbackEventHandler(handler)
}

func (c *Client) replyByReqID(reqID string, body any, cmd string) (WsFrameRaw, error) {
	if reqID == "" {
		return WsFrameRaw{}, errors.New("缺少 reqID")
	}
	return c.ws.sendReply(reqID, body, cmd)
}
