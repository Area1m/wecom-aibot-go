package wecomaibot

import "encoding/json"

// 默认值：Config 里未显式设置的字段会按这些值填充（见 NewClient）。
const (
	DefaultWSURL             = "wss://openws.work.weixin.qq.com"
	DefaultReconnectInterval = 1000
	DefaultMaxReconnect      = 10
	DefaultHeartbeatInterval = 30000
	DefaultRequestTimeout    = 10000
	DefaultMaxDownloadBytes  = 100 << 20 // 100 MiB
)

// WebSocket 协议命令字。
const (
	WsCmdSubscribe       = "aibot_subscribe"           // 认证
	WsCmdHeartbeat       = "ping"                      // 心跳（服务端不回 ACK）
	WsCmdPong            = "pong"                      // 心跳应答（仅服务端主动 ping 时回包）
	WsCmdResponse        = "aibot_respond_msg"         // 回复消息/事件
	WsCmdResponseWelcome = "aibot_respond_welcome_msg" // 欢迎语回复
	WsCmdResponseUpdate  = "aibot_respond_update_msg"  // 更新模板卡片
	WsCmdSendMsg         = "aibot_send_msg"            // 主动发送
	WsCmdCallback        = "aibot_msg_callback"        // 消息回调
	WsCmdEventCallback   = "aibot_event_callback"      // 事件回调
)

// 消息类型（BaseMessage.MsgType）。
const (
	MessageTypeText  = "text"
	MessageTypeImage = "image"
	MessageTypeMixed = "mixed"
	MessageTypeVoice = "voice"
	MessageTypeFile  = "file"
	MessageTypeEvent = "event"
)

// 事件类型（EventContent.EventType）。
const (
	EventTypeEnterChat         = "enter_chat"
	EventTypeTemplateCardEvent = "template_card_event"
	EventTypeFeedbackEvent     = "feedback_event"
)

// Config 是客户端配置。BotID / Secret 必填，其余字段留零值即用默认值。
type Config struct {
	BotID  string // 机器人 ID（必填）
	Secret string // 机器人 Secret（必填）

	ReconnectIntervalMS  int    // 重连基础间隔（毫秒），默认 1000，按 2 倍指数退避、上限 30s
	MaxReconnectAttempts int    // 最大重连次数，默认 10；-1 表示无限重连
	HeartbeatIntervalMS  int    // 心跳（ping）间隔（毫秒），默认 30000
	RequestTimeoutMS     int    // HTTP 请求超时（毫秒），默认 10000，用于文件下载
	MaxDownloadBytes     int    // 单次文件下载上限（字节），默认 100 MiB，防止异常大响应打爆内存
	WSURL                string // WebSocket 地址，默认 wss://openws.work.weixin.qq.com
	Logger               Logger // 日志实现，默认 DefaultLogger
}

// WsHeaders 是所有 WebSocket 帧的头部，req_id 用于把请求与响应/ACK 关联起来。
type WsHeaders struct {
	ReqID string `json:"req_id"`
}

// WsFrame 是发送用的 WebSocket 帧。
type WsFrame struct {
	Cmd     string    `json:"cmd,omitempty"`
	Headers WsHeaders `json:"headers"`
	Body    any       `json:"body,omitempty"`
	ErrCode int       `json:"errcode,omitempty"`
	ErrMsg  string    `json:"errmsg,omitempty"`
}

// WsFrameRaw 是接收用的 WebSocket 帧，Body 保持原始 JSON 由上层按 cmd 解析。
type WsFrameRaw struct {
	Cmd     string          `json:"cmd,omitempty"`
	Headers WsHeaders       `json:"headers"`
	Body    json.RawMessage `json:"body,omitempty"`
	ErrCode int             `json:"errcode,omitempty"`
	ErrMsg  string          `json:"errmsg,omitempty"`
}

// RequestCarrier 是所有消息/事件类型都实现的最小接口，回复类 API 需要它来取 req_id。
type RequestCarrier interface {
	RequestID() string
}

// MessageFrom 是消息发送者。
type MessageFrom struct {
	UserID string `json:"userid"`
}

// EventFrom 是事件触发者。
type EventFrom struct {
	UserID string `json:"userid"`
	CorpID string `json:"corpid,omitempty"`
}

// TextContent 是文本内容。
type TextContent struct {
	Content string `json:"content"`
}

// ImageContent 是图片内容，AESKey 非空时需解密后使用。
type ImageContent struct {
	URL    string `json:"url"`
	AESKey string `json:"aeskey,omitempty"`
}

// VoiceContent 是语音内容（已转写文本）。
type VoiceContent struct {
	Content string `json:"content"`
}

// FileContent 是文件内容，AESKey 非空时需解密后使用。
type FileContent struct {
	URL    string `json:"url"`
	AESKey string `json:"aeskey,omitempty"`
}

// MixedMsgItem 是图文混排中的一项。
type MixedMsgItem struct {
	MsgType string        `json:"msgtype"`
	Text    *TextContent  `json:"text,omitempty"`
	Image   *ImageContent `json:"image,omitempty"`
}

// MixedContent 是图文混排内容。
type MixedContent struct {
	MsgItem []MixedMsgItem `json:"msg_item"`
}

// QuoteContent 是被引用的消息内容。
type QuoteContent struct {
	MsgType string        `json:"msgtype"`
	Text    *TextContent  `json:"text,omitempty"`
	Image   *ImageContent `json:"image,omitempty"`
	Mixed   *MixedContent `json:"mixed,omitempty"`
	Voice   *VoiceContent `json:"voice,omitempty"`
	File    *FileContent  `json:"file,omitempty"`
}

// BaseMessage 是所有消息的公共字段；ReqID 用于回复，不参与 JSON 编解码。
type BaseMessage struct {
	ReqID       string        `json:"-"`
	MsgID       string        `json:"msgid"`
	AIBotID     string        `json:"aibotid"`
	ChatID      string        `json:"chatid,omitempty"`
	ChatType    string        `json:"chattype"`
	From        MessageFrom   `json:"from"`
	CreateTime  int64         `json:"create_time,omitempty"`
	ResponseURL string        `json:"response_url,omitempty"`
	MsgType     string        `json:"msgtype"`
	Quote       *QuoteContent `json:"quote,omitempty"`
}

// RequestID 返回回复该消息时要带的 req_id。
func (m BaseMessage) RequestID() string {
	return m.ReqID
}

// TextMessage 是文本消息。
type TextMessage struct {
	BaseMessage
	Text TextContent `json:"text"`
}

// ImageMessage 是图片消息。
type ImageMessage struct {
	BaseMessage
	Image ImageContent `json:"image"`
}

// MixedMessage 是图文混排消息。
type MixedMessage struct {
	BaseMessage
	Mixed MixedContent `json:"mixed"`
}

// VoiceMessage 是语音消息。
type VoiceMessage struct {
	BaseMessage
	Voice VoiceContent `json:"voice"`
}

// FileMessage 是文件消息。
type FileMessage struct {
	BaseMessage
	File FileContent `json:"file"`
}

// EventContent 是事件内容。
type EventContent struct {
	EventType string `json:"eventtype"`
	EventKey  string `json:"event_key,omitempty"`
	TaskID    string `json:"task_id,omitempty"`
}

// EventMessage 是事件消息（进入会话、模板卡片、用户反馈等），ReqID 用于回复。
type EventMessage struct {
	ReqID      string       `json:"-"`
	MsgID      string       `json:"msgid"`
	CreateTime int64        `json:"create_time"`
	AIBotID    string       `json:"aibotid"`
	ChatID     string       `json:"chatid,omitempty"`
	ChatType   string       `json:"chattype,omitempty"`
	From       EventFrom    `json:"from"`
	MsgType    string       `json:"msgtype"`
	Event      EventContent `json:"event"`
}

// RequestID 返回回复该事件时要带的 req_id。
func (m EventMessage) RequestID() string {
	return m.ReqID
}

// ReplyMsgItem 是流式回复里附带的图片项（base64 内容 + md5）。
type ReplyMsgItem struct {
	MsgType string `json:"msgtype"`
	Image   struct {
		Base64 string `json:"base64"`
		MD5    string `json:"md5"`
	} `json:"image"`
}

// ReplyFeedback 是回复附带的反馈信息。
type ReplyFeedback struct {
	ID string `json:"id"`
}

// StreamPayload 是流式回复的内容体：同一个 ID 多次发送，最后以 Finish=true 结束。
type StreamPayload struct {
	ID       string         `json:"id"`
	Finish   bool           `json:"finish,omitempty"`
	Content  string         `json:"content,omitempty"`
	MsgItem  []ReplyMsgItem `json:"msg_item,omitempty"`
	Feedback *ReplyFeedback `json:"feedback,omitempty"`
}

// StreamReplyBody 是流式回复的消息体。
type StreamReplyBody struct {
	MsgType string        `json:"msgtype"`
	Stream  StreamPayload `json:"stream"`
}

// WelcomeTextReplyBody 是文本欢迎语的消息体。
type WelcomeTextReplyBody struct {
	MsgType string `json:"msgtype"`
	Text    struct {
		Content string `json:"content"`
	} `json:"text"`
}

// WelcomeTemplateCardReplyBody 是模板卡片欢迎语的消息体。
type WelcomeTemplateCardReplyBody struct {
	MsgType      string       `json:"msgtype"`
	TemplateCard TemplateCard `json:"template_card"`
}

// TemplateCard 是模板卡片的原始 JSON 结构，字段随卡片类型而变，由调用方按企微文档填写。
type TemplateCard map[string]any

// TemplateCardReplyBody 是模板卡片回复的消息体。
type TemplateCardReplyBody struct {
	MsgType      string       `json:"msgtype"`
	TemplateCard TemplateCard `json:"template_card"`
}

// StreamWithTemplateCardReplyBody 是流式回复 + 模板卡片的消息体。
type StreamWithTemplateCardReplyBody struct {
	MsgType      string        `json:"msgtype"`
	Stream       StreamPayload `json:"stream"`
	TemplateCard TemplateCard  `json:"template_card,omitempty"`
}

// UpdateTemplateCardBody 是更新模板卡片的消息体。
type UpdateTemplateCardBody struct {
	ResponseType string       `json:"response_type"`
	UserIDs      []string     `json:"userids,omitempty"`
	TemplateCard TemplateCard `json:"template_card"`
}

// SendMarkdownMsgBody 是主动发送 Markdown 的消息体。
type SendMarkdownMsgBody struct {
	MsgType  string `json:"msgtype"`
	Markdown struct {
		Content string `json:"content"`
	} `json:"markdown"`
}

// SendTemplateCardMsgBody 是主动发送模板卡片的消息体。
type SendTemplateCardMsgBody struct {
	MsgType      string       `json:"msgtype"`
	TemplateCard TemplateCard `json:"template_card"`
}

// DownloadedFile 是下载（并已解密）的文件内容与文件名。
type DownloadedFile struct {
	Buffer   []byte
	Filename string
}
