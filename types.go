package wecomaibot

import "encoding/json"

const (
	DefaultWSURL             = "wss://openws.work.weixin.qq.com"
	DefaultReconnectInterval = 1000
	DefaultMaxReconnect      = 10
	DefaultHeartbeatInterval = 30000
	DefaultRequestTimeout    = 10000
)

const (
	WsCmdSubscribe       = "aibot_subscribe"
	WsCmdHeartbeat       = "ping"
	WsCmdResponse        = "aibot_respond_msg"
	WsCmdResponseWelcome = "aibot_respond_welcome_msg"
	WsCmdResponseUpdate  = "aibot_respond_update_msg"
	WsCmdSendMsg         = "aibot_send_msg"
	WsCmdCallback        = "aibot_msg_callback"
	WsCmdEventCallback   = "aibot_event_callback"
)

const (
	MessageTypeText  = "text"
	MessageTypeImage = "image"
	MessageTypeMixed = "mixed"
	MessageTypeVoice = "voice"
	MessageTypeFile  = "file"
	MessageTypeEvent = "event"
)

const (
	EventTypeEnterChat         = "enter_chat"
	EventTypeTemplateCardEvent = "template_card_event"
	EventTypeFeedbackEvent     = "feedback_event"
)

type Config struct {
	BotID                string
	Secret               string
	ReconnectIntervalMS  int
	MaxReconnectAttempts int
	HeartbeatIntervalMS  int
	RequestTimeoutMS     int
	WSURL                string
	Logger               Logger
}

type WsHeaders struct {
	ReqID string `json:"req_id"`
}

type WsFrame struct {
	Cmd     string    `json:"cmd,omitempty"`
	Headers WsHeaders `json:"headers"`
	Body    any       `json:"body,omitempty"`
	ErrCode int       `json:"errcode,omitempty"`
	ErrMsg  string    `json:"errmsg,omitempty"`
}

type WsFrameRaw struct {
	Cmd     string          `json:"cmd,omitempty"`
	Headers WsHeaders       `json:"headers"`
	Body    json.RawMessage `json:"body,omitempty"`
	ErrCode int             `json:"errcode,omitempty"`
	ErrMsg  string          `json:"errmsg,omitempty"`
}

type RequestCarrier interface {
	RequestID() string
}

type MessageFrom struct {
	UserID string `json:"userid"`
}

type EventFrom struct {
	UserID string `json:"userid"`
	CorpID string `json:"corpid,omitempty"`
}

type TextContent struct {
	Content string `json:"content"`
}

type ImageContent struct {
	URL    string `json:"url"`
	AESKey string `json:"aeskey,omitempty"`
}

type VoiceContent struct {
	Content string `json:"content"`
}

type FileContent struct {
	URL    string `json:"url"`
	AESKey string `json:"aeskey,omitempty"`
}

type MixedMsgItem struct {
	MsgType string        `json:"msgtype"`
	Text    *TextContent  `json:"text,omitempty"`
	Image   *ImageContent `json:"image,omitempty"`
}

type MixedContent struct {
	MsgItem []MixedMsgItem `json:"msg_item"`
}

type QuoteContent struct {
	MsgType string        `json:"msgtype"`
	Text    *TextContent  `json:"text,omitempty"`
	Image   *ImageContent `json:"image,omitempty"`
	Mixed   *MixedContent `json:"mixed,omitempty"`
	Voice   *VoiceContent `json:"voice,omitempty"`
	File    *FileContent  `json:"file,omitempty"`
}

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

func (m BaseMessage) RequestID() string {
	return m.ReqID
}

type TextMessage struct {
	BaseMessage
	Text TextContent `json:"text"`
}

type ImageMessage struct {
	BaseMessage
	Image ImageContent `json:"image"`
}

type MixedMessage struct {
	BaseMessage
	Mixed MixedContent `json:"mixed"`
}

type VoiceMessage struct {
	BaseMessage
	Voice VoiceContent `json:"voice"`
}

type FileMessage struct {
	BaseMessage
	File FileContent `json:"file"`
}

type EventContent struct {
	EventType string `json:"eventtype"`
	EventKey  string `json:"event_key,omitempty"`
	TaskID    string `json:"task_id,omitempty"`
}

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

func (m EventMessage) RequestID() string {
	return m.ReqID
}

type ReplyMsgItem struct {
	MsgType string `json:"msgtype"`
	Image   struct {
		Base64 string `json:"base64"`
		MD5    string `json:"md5"`
	} `json:"image"`
}

type ReplyFeedback struct {
	ID string `json:"id"`
}

type StreamPayload struct {
	ID       string         `json:"id"`
	Finish   bool           `json:"finish,omitempty"`
	Content  string         `json:"content,omitempty"`
	MsgItem  []ReplyMsgItem `json:"msg_item,omitempty"`
	Feedback *ReplyFeedback `json:"feedback,omitempty"`
}

type StreamReplyBody struct {
	MsgType string        `json:"msgtype"`
	Stream  StreamPayload `json:"stream"`
}

type WelcomeTextReplyBody struct {
	MsgType string `json:"msgtype"`
	Text    struct {
		Content string `json:"content"`
	} `json:"text"`
}

type WelcomeTemplateCardReplyBody struct {
	MsgType      string       `json:"msgtype"`
	TemplateCard TemplateCard `json:"template_card"`
}

type TemplateCard map[string]any

type TemplateCardReplyBody struct {
	MsgType      string       `json:"msgtype"`
	TemplateCard TemplateCard `json:"template_card"`
}

type StreamWithTemplateCardReplyBody struct {
	MsgType      string        `json:"msgtype"`
	Stream       StreamPayload `json:"stream"`
	TemplateCard TemplateCard  `json:"template_card,omitempty"`
}

type UpdateTemplateCardBody struct {
	ResponseType string       `json:"response_type"`
	UserIDs      []string     `json:"userids,omitempty"`
	TemplateCard TemplateCard `json:"template_card"`
}

type SendMarkdownMsgBody struct {
	MsgType  string `json:"msgtype"`
	Markdown struct {
		Content string `json:"content"`
	} `json:"markdown"`
}

type SendTemplateCardMsgBody struct {
	MsgType      string       `json:"msgtype"`
	TemplateCard TemplateCard `json:"template_card"`
}

type DownloadedFile struct {
	Buffer   []byte
	Filename string
}
