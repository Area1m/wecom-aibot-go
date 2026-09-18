package wecomaibot

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type dispatcher struct {
	logger Logger

	mu sync.RWMutex

	onConnected     []func(context.Context)
	onAuthenticated []func(context.Context)
	onDisconnected  []func(context.Context, string)
	onReconnecting  []func(context.Context, int)
	onError         []func(context.Context, error)

	onMessage []func(context.Context, BaseMessage)
	onText    []func(context.Context, TextMessage)
	onImage   []func(context.Context, ImageMessage)
	onMixed   []func(context.Context, MixedMessage)
	onVoice   []func(context.Context, VoiceMessage)
	onFile    []func(context.Context, FileMessage)
	onEvent   []func(context.Context, EventMessage)

	onEnterChat         []func(context.Context, EventMessage)
	onTemplateCardEvent []func(context.Context, EventMessage)
	onFeedbackEvent     []func(context.Context, EventMessage)
}

func newDispatcher(logger Logger) *dispatcher {
	return &dispatcher{logger: logger}
}

func (d *dispatcher) addConnectedHandler(handler func(context.Context)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onConnected = append(d.onConnected, handler)
}

func (d *dispatcher) addAuthenticatedHandler(handler func(context.Context)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onAuthenticated = append(d.onAuthenticated, handler)
}

func (d *dispatcher) addDisconnectedHandler(handler func(context.Context, string)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onDisconnected = append(d.onDisconnected, handler)
}

func (d *dispatcher) addReconnectingHandler(handler func(context.Context, int)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onReconnecting = append(d.onReconnecting, handler)
}

func (d *dispatcher) addErrorHandler(handler func(context.Context, error)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onError = append(d.onError, handler)
}

func (d *dispatcher) addMessageHandler(handler func(context.Context, BaseMessage)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onMessage = append(d.onMessage, handler)
}

func (d *dispatcher) addTextHandler(handler func(context.Context, TextMessage)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onText = append(d.onText, handler)
}

func (d *dispatcher) addImageHandler(handler func(context.Context, ImageMessage)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onImage = append(d.onImage, handler)
}

func (d *dispatcher) addMixedHandler(handler func(context.Context, MixedMessage)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onMixed = append(d.onMixed, handler)
}

func (d *dispatcher) addVoiceHandler(handler func(context.Context, VoiceMessage)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onVoice = append(d.onVoice, handler)
}

func (d *dispatcher) addFileHandler(handler func(context.Context, FileMessage)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onFile = append(d.onFile, handler)
}

func (d *dispatcher) addEventHandler(handler func(context.Context, EventMessage)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onEvent = append(d.onEvent, handler)
}

func (d *dispatcher) addEnterChatHandler(handler func(context.Context, EventMessage)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onEnterChat = append(d.onEnterChat, handler)
}

func (d *dispatcher) addTemplateCardEventHandler(handler func(context.Context, EventMessage)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onTemplateCardEvent = append(d.onTemplateCardEvent, handler)
}

func (d *dispatcher) addFeedbackEventHandler(handler func(context.Context, EventMessage)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onFeedbackEvent = append(d.onFeedbackEvent, handler)
}

func (d *dispatcher) emitConnected(ctx context.Context) {
	d.mu.RLock()
	handlers := append([]func(context.Context){}, d.onConnected...)
	d.mu.RUnlock()
	for _, handler := range handlers {
		go handler(ctx)
	}
}

func (d *dispatcher) emitAuthenticated(ctx context.Context) {
	d.mu.RLock()
	handlers := append([]func(context.Context){}, d.onAuthenticated...)
	d.mu.RUnlock()
	for _, handler := range handlers {
		go handler(ctx)
	}
}

func (d *dispatcher) emitDisconnected(ctx context.Context, reason string) {
	d.mu.RLock()
	handlers := append([]func(context.Context, string){}, d.onDisconnected...)
	d.mu.RUnlock()
	for _, handler := range handlers {
		go handler(ctx, reason)
	}
}

func (d *dispatcher) emitReconnecting(ctx context.Context, attempt int) {
	d.mu.RLock()
	handlers := append([]func(context.Context, int){}, d.onReconnecting...)
	d.mu.RUnlock()
	for _, handler := range handlers {
		go handler(ctx, attempt)
	}
}

func (d *dispatcher) emitError(ctx context.Context, err error) {
	d.mu.RLock()
	handlers := append([]func(context.Context, error){}, d.onError...)
	d.mu.RUnlock()
	for _, handler := range handlers {
		go handler(ctx, err)
	}
}

func (d *dispatcher) dispatchFrame(ctx context.Context, frame WsFrameRaw) {
	switch frame.Cmd {
	case WsCmdCallback:
		d.dispatchMessage(ctx, frame)
	case WsCmdEventCallback:
		d.dispatchEvent(ctx, frame)
	default:
		d.logger.Debug("收到未分发 cmd: %s", frame.Cmd)
	}
}

func (d *dispatcher) dispatchMessage(ctx context.Context, frame WsFrameRaw) {
	var base BaseMessage
	if err := json.Unmarshal(frame.Body, &base); err != nil {
		d.emitError(ctx, fmt.Errorf("解析消息失败: %w", err))
		return
	}
	base.ReqID = frame.Headers.ReqID

	d.mu.RLock()
	genericHandlers := append([]func(context.Context, BaseMessage){}, d.onMessage...)
	textHandlers := append([]func(context.Context, TextMessage){}, d.onText...)
	imageHandlers := append([]func(context.Context, ImageMessage){}, d.onImage...)
	mixedHandlers := append([]func(context.Context, MixedMessage){}, d.onMixed...)
	voiceHandlers := append([]func(context.Context, VoiceMessage){}, d.onVoice...)
	fileHandlers := append([]func(context.Context, FileMessage){}, d.onFile...)
	d.mu.RUnlock()

	for _, handler := range genericHandlers {
		go handler(ctx, base)
	}

	switch base.MsgType {
	case MessageTypeText:
		var msg TextMessage
		if err := json.Unmarshal(frame.Body, &msg); err != nil {
			d.emitError(ctx, fmt.Errorf("解析文本消息失败: %w", err))
			return
		}
		msg.ReqID = frame.Headers.ReqID
		for _, handler := range textHandlers {
			go handler(ctx, msg)
		}
	case MessageTypeImage:
		var msg ImageMessage
		if err := json.Unmarshal(frame.Body, &msg); err != nil {
			d.emitError(ctx, fmt.Errorf("解析图片消息失败: %w", err))
			return
		}
		msg.ReqID = frame.Headers.ReqID
		for _, handler := range imageHandlers {
			go handler(ctx, msg)
		}
	case MessageTypeMixed:
		var msg MixedMessage
		if err := json.Unmarshal(frame.Body, &msg); err != nil {
			d.emitError(ctx, fmt.Errorf("解析图文混排消息失败: %w", err))
			return
		}
		msg.ReqID = frame.Headers.ReqID
		for _, handler := range mixedHandlers {
			go handler(ctx, msg)
		}
	case MessageTypeVoice:
		var msg VoiceMessage
		if err := json.Unmarshal(frame.Body, &msg); err != nil {
			d.emitError(ctx, fmt.Errorf("解析语音消息失败: %w", err))
			return
		}
		msg.ReqID = frame.Headers.ReqID
		for _, handler := range voiceHandlers {
			go handler(ctx, msg)
		}
	case MessageTypeFile:
		var msg FileMessage
		if err := json.Unmarshal(frame.Body, &msg); err != nil {
			d.emitError(ctx, fmt.Errorf("解析文件消息失败: %w", err))
			return
		}
		msg.ReqID = frame.Headers.ReqID
		for _, handler := range fileHandlers {
			go handler(ctx, msg)
		}
	default:
		d.logger.Debug("收到未处理消息类型: %s", base.MsgType)
	}
}

func (d *dispatcher) dispatchEvent(ctx context.Context, frame WsFrameRaw) {
	var eventMsg EventMessage
	if err := json.Unmarshal(frame.Body, &eventMsg); err != nil {
		d.emitError(ctx, fmt.Errorf("解析事件失败: %w", err))
		return
	}
	eventMsg.ReqID = frame.Headers.ReqID

	d.mu.RLock()
	eventHandlers := append([]func(context.Context, EventMessage){}, d.onEvent...)
	enterHandlers := append([]func(context.Context, EventMessage){}, d.onEnterChat...)
	cardHandlers := append([]func(context.Context, EventMessage){}, d.onTemplateCardEvent...)
	feedbackHandlers := append([]func(context.Context, EventMessage){}, d.onFeedbackEvent...)
	d.mu.RUnlock()

	for _, handler := range eventHandlers {
		go handler(ctx, eventMsg)
	}

	switch eventMsg.Event.EventType {
	case EventTypeEnterChat:
		for _, handler := range enterHandlers {
			go handler(ctx, eventMsg)
		}
	case EventTypeTemplateCardEvent:
		for _, handler := range cardHandlers {
			go handler(ctx, eventMsg)
		}
	case EventTypeFeedbackEvent:
		for _, handler := range feedbackHandlers {
			go handler(ctx, eventMsg)
		}
	default:
		d.logger.Debug("收到未处理事件类型: %s", eventMsg.Event.EventType)
	}
}
