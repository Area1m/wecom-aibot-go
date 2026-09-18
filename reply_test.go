package wecomaibot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// callbackServer 在认证成功后主动推一条文本消息回调，并对回复帧回 ACK，
// 用来端到端验证「收到消息 → 回调 → 回复」这条主链路。
type callbackServer struct {
	srv *httptest.Server

	mu   sync.Mutex
	recv []WsFrameRaw
}

func newCallbackServer(t *testing.T) *callbackServer {
	t.Helper()
	s := &callbackServer{}
	upgrader := websocket.Upgrader{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var frame WsFrameRaw
			if err := json.Unmarshal(data, &frame); err != nil {
				return
			}
			s.record(frame)

			if strings.HasPrefix(frame.Headers.ReqID, WsCmdSubscribe+"_") {
				_ = conn.WriteJSON(WsFrameRaw{Headers: WsHeaders{ReqID: frame.Headers.ReqID}})
				_ = conn.WriteJSON(map[string]any{
					"cmd":     WsCmdCallback,
					"headers": map[string]string{"req_id": "msg_req_1"},
					"body": map[string]any{
						"msgid":    "m1",
						"aibotid":  "bot",
						"chattype": "single",
						"from":     map[string]string{"userid": "u1"},
						"msgtype":  MessageTypeText,
						"text":     map[string]string{"content": "hi"},
					},
				})
				continue
			}
			// 回复/心跳帧一律回 ACK
			_ = conn.WriteJSON(WsFrameRaw{Headers: WsHeaders{ReqID: frame.Headers.ReqID}})
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *callbackServer) record(frame WsFrameRaw) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recv = append(s.recv, frame)
}

func (s *callbackServer) find(reqID string) (WsFrameRaw, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.recv {
		if f.Headers.ReqID == reqID {
			return f, true
		}
	}
	return WsFrameRaw{}, false
}

// findByCmd 返回收到的第一帧指定 cmd 的帧（用于主动发送/欢迎语/卡片更新等独立 req_id 的场景）。
func (s *callbackServer) findByCmd(cmd string) (WsFrameRaw, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.recv {
		if f.Cmd == cmd {
			return f, true
		}
	}
	return WsFrameRaw{}, false
}

func (s *callbackServer) url() string {
	return "ws" + strings.TrimPrefix(s.srv.URL, "http")
}

func TestReplyStreamEndToEnd(t *testing.T) {
	srv := newCallbackServer(t)
	bot, err := NewClient(Config{
		BotID:               "bot",
		Secret:              "secret",
		WSURL:               srv.url(),
		HeartbeatIntervalMS: 200,
		Logger:              silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}

	received := make(chan TextMessage, 1)
	bot.OnText(func(_ context.Context, msg TextMessage) { received <- msg })

	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer bot.Disconnect()

	var msg TextMessage
	select {
	case msg = <-received:
	case <-time.After(3 * time.Second):
		t.Fatal("未收到文本消息回调")
	}
	if msg.Text.Content != "hi" || msg.MsgID != "m1" {
		t.Fatalf("回调内容错误: %+v", msg)
	}
	if msg.RequestID() != "msg_req_1" {
		t.Fatalf("回调 req_id 错误: %s", msg.RequestID())
	}

	// 流式回复：同一 streamID 两帧，回复帧的 req_id 必须是原消息的 req_id
	streamID := GenerateReqID("stream")
	if _, err := bot.ReplyStreamByID(msg, streamID, "思考中...", false, nil, nil); err != nil {
		t.Fatalf("流式首包失败: %v", err)
	}
	if _, err := bot.ReplyStreamByID(msg, streamID, "你好", true, nil, nil); err != nil {
		t.Fatalf("流式结束包失败: %v", err)
	}

	frame, ok := srv.find("msg_req_1")
	if !ok {
		t.Fatal("服务端未收到回复帧")
	}
	if frame.Cmd != WsCmdResponse {
		t.Errorf("回复帧 cmd 错误: %s", frame.Cmd)
	}

	var body StreamReplyBody
	if err := json.Unmarshal(frame.Body, &body); err != nil {
		t.Fatalf("解析回复体失败: %v", err)
	}
	if body.MsgType != "stream" || body.Stream.ID != streamID {
		t.Errorf("回复体错误: %+v", body)
	}

	// 主动发送走独立的 req_id，不依赖收到的消息
	if _, err := bot.SendMessage("chat_1", map[string]any{"msgtype": "markdown", "markdown": map[string]any{"content": "**hi**"}}); err != nil {
		t.Fatalf("主动发送失败: %v", err)
	}
}

// 并发回复同一消息必须串行执行且不互相覆盖 pendingAcks：队列互斥锁保证同一 req_id
// 同时只有一个 sendAndWaitAck 在途，否则两个协程会互相覆盖 pendingAcks[req_id] 的
// 等待通道、导致 ACK 串扰。
func TestConcurrentRepliesToSameMessage(t *testing.T) {
	srv := newCallbackServer(t)
	bot, err := NewClient(Config{
		BotID:               "bot",
		Secret:              "secret",
		WSURL:               srv.url(),
		HeartbeatIntervalMS: 200,
		Logger:              silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}

	received := make(chan TextMessage, 1)
	bot.OnText(func(_ context.Context, msg TextMessage) { received <- msg })

	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer bot.Disconnect()

	var msg TextMessage
	select {
	case msg = <-received:
	case <-time.After(3 * time.Second):
		t.Fatal("未收到文本消息回调")
	}

	const n = 10
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 同一 req_id、不同 streamID，只有最后一个 finish=true
			_, err := bot.ReplyStreamByID(msg, GenerateReqID("stream"), "chunk", i == n-1, nil, nil)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("并发回复失败: %v", err)
		}
	}
}

// replyQueue 用引用计数做清理：并发 acquire/release 后 map 必须清空（refs 归零即删），
// 既不能残留条目（内存泄漏），也不能误删仍被持有的队列（破坏串行）。
func TestReplyQueueRefcountCleanup(t *testing.T) {
	w := newWSConnection(Config{}, silentTestLogger{})

	const goroutines = 50
	const distinct = 10 // 故意碰撞，让部分 req_id 出现 refs>1 的并发持有
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reqID := "req_" + string(rune('a'+i%distinct))
			q := w.acquireReplyQueue(reqID)
			q.mu.Lock()
			q.mu.Unlock()
			w.releaseReplyQueue(q)
		}(i)
	}
	wg.Wait()

	w.replyQueueMu.Lock()
	n := len(w.replyQueues)
	w.replyQueueMu.Unlock()
	if n != 0 {
		t.Errorf("所有队列释放后 replyQueues 应为空，实际 %d 条", n)
	}
}

// connectAndAwaitText 连接回调服务端并等待其推送的文本回调（同时确保已认证）。
func connectAndAwaitText(t *testing.T, srv *callbackServer) (*Client, TextMessage) {
	t.Helper()
	bot, err := NewClient(Config{
		BotID:               "bot",
		Secret:              "secret",
		WSURL:               srv.url(),
		HeartbeatIntervalMS: 200,
		Logger:              silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	received := make(chan TextMessage, 1)
	bot.OnText(func(_ context.Context, msg TextMessage) { received <- msg })
	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	t.Cleanup(bot.Disconnect)

	select {
	case msg := <-received:
		return bot, msg
	case <-time.After(3 * time.Second):
		t.Fatal("未收到文本消息回调")
		return nil, TextMessage{}
	}
}

// 主动发送帧体：body 应为扁平 {chatid, msgtype, markdown}，而非 {chatid, msg} 包装。
func TestSendMessageFrameShape(t *testing.T) {
	srv := newCallbackServer(t)
	bot, _ := connectAndAwaitText(t, srv)

	if _, err := bot.SendMessage("chat_1", map[string]any{
		"msgtype":  "markdown",
		"markdown": map[string]any{"content": "**hi**"},
	}); err != nil {
		t.Fatalf("SendMessage 失败: %v", err)
	}

	frame, ok := srv.findByCmd(WsCmdSendMsg)
	if !ok {
		t.Fatal("未收到 aibot_send_msg 帧")
	}
	if !strings.HasPrefix(frame.Headers.ReqID, WsCmdSendMsg+"_") {
		t.Errorf("req_id 前缀错误: %s", frame.Headers.ReqID)
	}
	var body map[string]any
	if err := json.Unmarshal(frame.Body, &body); err != nil {
		t.Fatalf("解析 body 失败: %v", err)
	}
	if body["chatid"] != "chat_1" {
		t.Errorf("chatid 错误: %v", body["chatid"])
	}
	if body["msgtype"] != "markdown" {
		t.Errorf("msgtype 错误: %v", body["msgtype"])
	}
	if _, hasMsg := body["msg"]; hasMsg {
		t.Error("body 不应含 msg 包装键")
	}
	md, _ := body["markdown"].(map[string]any)
	if md == nil || md["content"] != "**hi**" {
		t.Errorf("markdown 内容错误: %v", body["markdown"])
	}
}

func TestSendMessageEmptyChatID(t *testing.T) {
	bot, err := NewClient(Config{BotID: "b", Secret: "s", Logger: silentTestLogger{}})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	if _, err := bot.SendMessage("", map[string]any{"msgtype": "markdown"}); err == nil {
		t.Error("空 chatID 应报错")
	}
}

// 欢迎语回复应使用 aibot_respond_welcome_msg 命令字并透传原 req_id。
func TestReplyWelcomeCmd(t *testing.T) {
	srv := newCallbackServer(t)
	bot, msg := connectAndAwaitText(t, srv)

	if _, err := bot.ReplyWelcome(msg, map[string]any{"msgtype": "text", "text": map[string]any{"content": "hi"}}); err != nil {
		t.Fatalf("ReplyWelcome 失败: %v", err)
	}
	frame, ok := srv.findByCmd(WsCmdResponseWelcome)
	if !ok {
		t.Fatal("未收到 aibot_respond_welcome_msg 帧")
	}
	if frame.Headers.ReqID != msg.RequestID() {
		t.Errorf("欢迎语应透传原 req_id: got %s, want %s", frame.Headers.ReqID, msg.RequestID())
	}
}

// 模板卡片回复的 feedback 应合并进 card 对象内，且不污染调用方传入的 card。
func TestReplyTemplateCardFeedback(t *testing.T) {
	srv := newCallbackServer(t)
	bot, msg := connectAndAwaitText(t, srv)

	card := TemplateCard{"card_type": "text_notice", "main_title": map[string]any{"title": "t"}}
	if _, err := bot.ReplyTemplateCard(msg, card, &ReplyFeedback{ID: "fb1"}); err != nil {
		t.Fatalf("ReplyTemplateCard 失败: %v", err)
	}
	if _, polluted := card["feedback"]; polluted {
		t.Error("传入的 card 不应被修改")
	}

	frame, ok := srv.findByCmd(WsCmdResponse)
	if !ok {
		t.Fatal("未收到回复帧")
	}
	var body TemplateCardReplyBody
	if err := json.Unmarshal(frame.Body, &body); err != nil {
		t.Fatalf("解析 body 失败: %v", err)
	}
	if body.MsgType != "template_card" {
		t.Errorf("msgtype 错误: %s", body.MsgType)
	}
	fb, _ := body.TemplateCard["feedback"].(map[string]any)
	if fb == nil || fb["id"] != "fb1" {
		t.Errorf("feedback 应合并进 card 内: %v", body.TemplateCard)
	}
}

// 流式+卡片组合回复：msgtype 应为 stream_with_template_card 且含 template_card。
func TestReplyStreamWithCardBody(t *testing.T) {
	srv := newCallbackServer(t)
	bot, msg := connectAndAwaitText(t, srv)

	card := TemplateCard{"card_type": "news_notice"}
	if _, err := bot.ReplyStreamWithCard(msg, "s1", "txt", false, &StreamWithCardOptions{TemplateCard: card}); err != nil {
		t.Fatalf("ReplyStreamWithCard 失败: %v", err)
	}
	frame, ok := srv.findByCmd(WsCmdResponse)
	if !ok {
		t.Fatal("未收到回复帧")
	}
	var body map[string]any
	if err := json.Unmarshal(frame.Body, &body); err != nil {
		t.Fatalf("解析 body 失败: %v", err)
	}
	if body["msgtype"] != "stream_with_template_card" {
		t.Errorf("msgtype 错误: %v", body["msgtype"])
	}
	if body["template_card"] == nil {
		t.Error("应含 template_card")
	}
}

// 卡片更新：cmd 应为 aibot_respond_update_msg，body 含 response_type/template_card/userids。
func TestUpdateTemplateCardBody(t *testing.T) {
	srv := newCallbackServer(t)
	bot, msg := connectAndAwaitText(t, srv)

	card := TemplateCard{"card_type": "text_notice"}
	if _, err := bot.UpdateTemplateCard(msg, card, []string{"u1"}); err != nil {
		t.Fatalf("UpdateTemplateCard 失败: %v", err)
	}
	frame, ok := srv.findByCmd(WsCmdResponseUpdate)
	if !ok {
		t.Fatal("未收到 aibot_respond_update_msg 帧")
	}
	var body map[string]any
	if err := json.Unmarshal(frame.Body, &body); err != nil {
		t.Fatalf("解析 body 失败: %v", err)
	}
	if body["response_type"] != "update_template_card" {
		t.Errorf("response_type 错误: %v", body["response_type"])
	}
	if body["template_card"] == nil {
		t.Error("应含 template_card")
	}
	ids, _ := body["userids"].([]any)
	if len(ids) != 1 || ids[0] != "u1" {
		t.Errorf("userids 错误: %v", body["userids"])
	}
}

// 去 omitempty 后，中间帧（finish=false）也必须显式序列化出 finish 与 content。
func TestReplyStreamByIDFinishFalseSerialized(t *testing.T) {
	srv := newCallbackServer(t)
	bot, msg := connectAndAwaitText(t, srv)

	if _, err := bot.ReplyStreamByID(msg, "s1", "chunk", false, nil, nil); err != nil {
		t.Fatalf("ReplyStreamByID 失败: %v", err)
	}
	frame, ok := srv.findByCmd(WsCmdResponse)
	if !ok {
		t.Fatal("未收到回复帧")
	}
	var raw map[string]any
	if err := json.Unmarshal(frame.Body, &raw); err != nil {
		t.Fatalf("解析 body 失败: %v", err)
	}
	stream, _ := raw["stream"].(map[string]any)
	if stream == nil {
		t.Fatal("缺少 stream")
	}
	if stream["finish"] != false {
		t.Errorf("中间帧 finish 应显式序列化为 false, got %v", stream["finish"])
	}
	if stream["content"] != "chunk" {
		t.Errorf("content 错误: %v", stream["content"])
	}
}

// 服务端对回复帧回 errcode!=0 的 ACK：Reply 应返回「回复 ACK 错误」。
func TestReplyAckErrorCode(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var f WsFrameRaw
			if err := json.Unmarshal(data, &f); err != nil {
				return
			}
			if strings.HasPrefix(f.Headers.ReqID, WsCmdSubscribe+"_") {
				_ = conn.WriteJSON(WsFrameRaw{Headers: WsHeaders{ReqID: f.Headers.ReqID}})
				continue
			}
			_ = conn.WriteJSON(WsFrameRaw{Headers: WsHeaders{ReqID: f.Headers.ReqID}, ErrCode: 1, ErrMsg: "boom"})
		}
	}))
	defer srv.Close()

	bot, err := NewClient(Config{
		BotID: "bot", Secret: "secret",
		WSURL:               "ws" + strings.TrimPrefix(srv.URL, "http"),
		HeartbeatIntervalMS: 60000, Logger: silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	authed := make(chan struct{}, 1)
	bot.OnAuthenticated(func(context.Context) { authed <- struct{}{} })
	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer bot.Disconnect()

	select {
	case <-authed:
	case <-time.After(3 * time.Second):
		t.Fatal("未认证")
	}

	msg := TextMessage{BaseMessage: BaseMessage{ReqID: "req_x"}}
	_, err = bot.Reply(msg, map[string]any{"msgtype": "text", "text": map[string]any{"content": "hi"}}, "")
	if err == nil || !strings.Contains(err.Error(), "回复 ACK 错误") {
		t.Errorf("应返回回复 ACK 错误, got %v", err)
	}
}

// 服务端不回 ACK 时，Reply 应在 replyAckTimeout 后返回超时错误。
func TestReplyAckTimeout(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var f WsFrameRaw
			if err := json.Unmarshal(data, &f); err != nil {
				return
			}
			if strings.HasPrefix(f.Headers.ReqID, WsCmdSubscribe+"_") {
				_ = conn.WriteJSON(WsFrameRaw{Headers: WsHeaders{ReqID: f.Headers.ReqID}})
			}
			// 其它帧不回 ACK
		}
	}))
	defer srv.Close()

	bot, err := NewClient(Config{
		BotID: "bot", Secret: "secret",
		WSURL:               "ws" + strings.TrimPrefix(srv.URL, "http"),
		HeartbeatIntervalMS: 60000, Logger: silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	authed := make(chan struct{}, 1)
	bot.OnAuthenticated(func(context.Context) { authed <- struct{}{} })
	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer bot.Disconnect()

	select {
	case <-authed:
	case <-time.After(3 * time.Second):
		t.Fatal("未认证")
	}

	bot.ws.replyAckTimeout = 50 * time.Millisecond
	msg := TextMessage{BaseMessage: BaseMessage{ReqID: "req_x"}}
	_, err = bot.Reply(msg, map[string]any{"msgtype": "text", "text": map[string]any{"content": "hi"}}, "")
	if err == nil || !strings.Contains(err.Error(), "等待回复 ACK 超时") {
		t.Errorf("应返回等待回复 ACK 超时, got %v", err)
	}
}

// 回复在途等待 ACK 时服务端断开：Reply 应通过 notifyPendingAckError 返回错误。
func TestPendingReplyFailsOnDisconnect(t *testing.T) {
	upgrader := websocket.Upgrader{}
	received := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var f WsFrameRaw
			if err := json.Unmarshal(data, &f); err != nil {
				return
			}
			if strings.HasPrefix(f.Headers.ReqID, WsCmdSubscribe+"_") {
				_ = conn.WriteJSON(WsFrameRaw{Headers: WsHeaders{ReqID: f.Headers.ReqID}})
				continue
			}
			// 收到回复帧后立即断开
			select {
			case received <- struct{}{}:
			default:
			}
			_ = conn.Close()
			return
		}
	}))
	defer srv.Close()

	bot, err := NewClient(Config{
		BotID: "bot", Secret: "secret",
		WSURL:               "ws" + strings.TrimPrefix(srv.URL, "http"),
		HeartbeatIntervalMS: 60000, Logger: silentTestLogger{},
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	authed := make(chan struct{}, 1)
	bot.OnAuthenticated(func(context.Context) { authed <- struct{}{} })
	if err := bot.Connect(); err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer bot.Disconnect()

	select {
	case <-authed:
	case <-time.After(3 * time.Second):
		t.Fatal("未认证")
	}

	msg := TextMessage{BaseMessage: BaseMessage{ReqID: "req_x"}}
	replyErr := make(chan error, 1)
	go func() {
		_, e := bot.Reply(msg, map[string]any{"msgtype": "text", "text": map[string]any{"content": "hi"}}, "")
		replyErr <- e
	}()

	select {
	case <-received:
	case <-time.After(3 * time.Second):
		t.Fatal("服务端未收到回复帧")
	}

	select {
	case e := <-replyErr:
		if e == nil {
			t.Error("断连时 Reply 应返回错误")
		}
	case <-time.After(3 * time.Second):
		t.Error("断连后 Reply 应在超时前返回错误")
	}
}
