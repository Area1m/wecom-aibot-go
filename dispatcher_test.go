package wecomaibot

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func mustBody(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func eventFrame(reqID, eventType string) WsFrameRaw {
	return WsFrameRaw{
		Cmd:     WsCmdEventCallback,
		Headers: WsHeaders{ReqID: reqID},
		Body: mustBody(map[string]any{
			"msgid": "e1", "create_time": 1, "aibotid": "bot",
			"msgtype": "event",
			"event":   map[string]any{"eventtype": eventType, "event_key": "k", "task_id": "t"},
		}),
	}
}

func recvTimeout[T any](t *testing.T, ch <-chan T, name string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(time.Second):
		t.Fatalf("%s 未触发", name)
		var zero T
		return zero
	}
}

func assertNoRecv[T any](t *testing.T, ch <-chan T, name string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("%s 不应触发, 收到 %v", name, v)
	case <-time.After(50 * time.Millisecond):
	}
}

// 事件分发：三种 eventtype 都应触发通用 event 回调 + 对应专用回调，并带上 ReqID。
func TestDispatchEventBranches(t *testing.T) {
	cases := []struct {
		name  string
		event string
		add   func(*dispatcher, func(context.Context, EventMessage))
	}{
		{"enter_chat", EventTypeEnterChat, func(d *dispatcher, h func(context.Context, EventMessage)) { d.addEnterChatHandler(h) }},
		{"template_card_event", EventTypeTemplateCardEvent, func(d *dispatcher, h func(context.Context, EventMessage)) { d.addTemplateCardEventHandler(h) }},
		{"feedback_event", EventTypeFeedbackEvent, func(d *dispatcher, h func(context.Context, EventMessage)) { d.addFeedbackEventHandler(h) }},
		{"disconnected_event", EventTypeDisconnectedEvent, func(d *dispatcher, h func(context.Context, EventMessage)) { d.addDisconnectedEventHandler(h) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := newDispatcher(silentTestLogger{})
			generic := make(chan EventMessage, 1)
			specific := make(chan EventMessage, 1)
			d.addEventHandler(func(_ context.Context, m EventMessage) { generic <- m })
			c.add(d, func(_ context.Context, m EventMessage) { specific <- m })

			d.dispatchEvent(context.Background(), eventFrame("req_1", c.event))

			g := recvTimeout(t, generic, "通用 event 回调")
			s := recvTimeout(t, specific, "专用事件回调")
			if g.ReqID != "req_1" || g.Event.EventType != c.event {
				t.Errorf("通用回调收到错误消息: %+v", g)
			}
			if s.ReqID != "req_1" || s.Event.EventType != c.event {
				t.Errorf("专用回调收到错误消息: %+v", s)
			}
		})
	}
}

// 未知 eventtype：通用 event 回调仍应触发，专用回调不触发。
func TestDispatchEventUnknownType(t *testing.T) {
	d := newDispatcher(silentTestLogger{})
	generic := make(chan EventMessage, 1)
	d.addEventHandler(func(_ context.Context, m EventMessage) { generic <- m })

	d.dispatchEvent(context.Background(), eventFrame("req_1", "some_unknown_event"))

	m := recvTimeout(t, generic, "通用 event 回调")
	if m.Event.EventType != "some_unknown_event" {
		t.Errorf("未知 eventtype 应原样透传: %+v", m)
	}
}

// 事件 body 非法 JSON：应通过 OnError 上报「解析事件失败」。
func TestDispatchEventParseError(t *testing.T) {
	d := newDispatcher(silentTestLogger{})
	errs := make(chan error, 1)
	d.addErrorHandler(func(_ context.Context, e error) { errs <- e })

	d.dispatchEvent(context.Background(), WsFrameRaw{Cmd: WsCmdEventCallback, Body: []byte("not-json")})

	e := recvTimeout(t, errs, "解析事件失败 OnError")
	if !strings.Contains(e.Error(), "解析事件失败") {
		t.Errorf("错误应含「解析事件失败」, got %v", e)
	}
}

// 消息分发：image/mixed/voice/file 四种类型各自触发专用回调并带上 ReqID 与字段。
func TestDispatchMessageTypes(t *testing.T) {
	d := newDispatcher(silentTestLogger{})
	image := make(chan ImageMessage, 1)
	mixed := make(chan MixedMessage, 1)
	voice := make(chan VoiceMessage, 1)
	file := make(chan FileMessage, 1)
	video := make(chan VideoMessage, 1)
	d.addImageHandler(func(_ context.Context, m ImageMessage) { image <- m })
	d.addMixedHandler(func(_ context.Context, m MixedMessage) { mixed <- m })
	d.addVoiceHandler(func(_ context.Context, m VoiceMessage) { voice <- m })
	d.addFileHandler(func(_ context.Context, m FileMessage) { file <- m })
	d.addVideoHandler(func(_ context.Context, m VideoMessage) { video <- m })

	msgFrame := func(body map[string]any) WsFrameRaw {
		return WsFrameRaw{Cmd: WsCmdCallback, Headers: WsHeaders{ReqID: "req_m"}, Body: mustBody(body)}
	}

	d.dispatchMessage(context.Background(), msgFrame(map[string]any{
		"msgid": "1", "aibotid": "bot", "chattype": "single", "from": map[string]any{"userid": "u"},
		"msgtype": "image", "image": map[string]any{"url": "http://x/a.png", "aeskey": "k1"},
	}))
	d.dispatchMessage(context.Background(), msgFrame(map[string]any{
		"msgid": "2", "aibotid": "bot", "chattype": "single", "from": map[string]any{"userid": "u"},
		"msgtype": "mixed", "mixed": map[string]any{"msg_item": []any{}},
	}))
	d.dispatchMessage(context.Background(), msgFrame(map[string]any{
		"msgid": "3", "aibotid": "bot", "chattype": "single", "from": map[string]any{"userid": "u"},
		"msgtype": "voice", "voice": map[string]any{"content": "hi"},
	}))
	d.dispatchMessage(context.Background(), msgFrame(map[string]any{
		"msgid": "4", "aibotid": "bot", "chattype": "single", "from": map[string]any{"userid": "u"},
		"msgtype": "file", "file": map[string]any{"url": "http://x/f.bin", "aeskey": "k2"},
	}))
	d.dispatchMessage(context.Background(), msgFrame(map[string]any{
		"msgid": "5", "aibotid": "bot", "chattype": "single", "from": map[string]any{"userid": "u"},
		"msgtype": "video", "video": map[string]any{"url": "http://x/v.mp4", "aeskey": "k3"},
	}))

	img := recvTimeout(t, image, "图片回调")
	if img.ReqID != "req_m" || img.Image.URL != "http://x/a.png" || img.Image.AESKey != "k1" {
		t.Errorf("图片消息错误: %+v", img)
	}
	mx := recvTimeout(t, mixed, "图文混排回调")
	if mx.ReqID != "req_m" || mx.Mixed.MsgItem == nil {
		t.Errorf("图文混排消息错误: %+v", mx)
	}
	vo := recvTimeout(t, voice, "语音回调")
	if vo.ReqID != "req_m" || vo.Voice.Content != "hi" {
		t.Errorf("语音消息错误: %+v", vo)
	}
	fl := recvTimeout(t, file, "文件回调")
	if fl.ReqID != "req_m" || fl.File.URL != "http://x/f.bin" || fl.File.AESKey != "k2" {
		t.Errorf("文件消息错误: %+v", fl)
	}
	vd := recvTimeout(t, video, "视频回调")
	if vd.ReqID != "req_m" || vd.Video.URL != "http://x/v.mp4" || vd.Video.AESKey != "k3" {
		t.Errorf("视频消息错误: %+v", vd)
	}
}

// 类型化反序列化失败（msgtype=text 但 text 字段类型不符）：应上报错误且不触发 text 回调。
func TestDispatchMessageTypedUnmarshalError(t *testing.T) {
	d := newDispatcher(silentTestLogger{})
	errs := make(chan error, 1)
	text := make(chan TextMessage, 1)
	d.addErrorHandler(func(_ context.Context, e error) { errs <- e })
	d.addTextHandler(func(_ context.Context, m TextMessage) { text <- m })

	body := mustBody(map[string]any{
		"msgid": "1", "aibotid": "bot", "chattype": "single", "from": map[string]any{"userid": "u"},
		"msgtype": "text", "text": "not-an-object",
	})
	d.dispatchMessage(context.Background(), WsFrameRaw{Cmd: WsCmdCallback, Headers: WsHeaders{ReqID: "r"}, Body: body})

	e := recvTimeout(t, errs, "类型化反序列化失败 OnError")
	if !strings.Contains(e.Error(), "解析文本消息失败") {
		t.Errorf("错误应含「解析文本消息失败」, got %v", e)
	}
	assertNoRecv(t, text, "text 回调")
}

// 消息 body 非法 JSON：应通过 OnError 上报「解析消息失败」。
func TestDispatchMessageBaseParseError(t *testing.T) {
	d := newDispatcher(silentTestLogger{})
	errs := make(chan error, 1)
	d.addErrorHandler(func(_ context.Context, e error) { errs <- e })

	d.dispatchMessage(context.Background(), WsFrameRaw{Cmd: WsCmdCallback, Body: []byte("not-json")})

	e := recvTimeout(t, errs, "解析消息失败 OnError")
	if !strings.Contains(e.Error(), "解析消息失败") {
		t.Errorf("错误应含「解析消息失败」, got %v", e)
	}
}

// 未知 cmd 的帧：dispatchFrame 走 default 分支，只记日志不 panic、不分发。
func TestDispatchFrameUnknownCmd(t *testing.T) {
	d := newDispatcher(silentTestLogger{})
	d.dispatchFrame(context.Background(), WsFrameRaw{Cmd: "some_unknown_cmd"})
	// 仅断言不 panic；无回调可断言
}
