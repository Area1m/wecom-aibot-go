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
	if _, err := bot.SendMarkdown("chat_1", "**hi**"); err != nil {
		t.Fatalf("主动发送失败: %v", err)
	}
}
