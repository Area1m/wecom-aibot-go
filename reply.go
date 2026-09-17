package wecomaibot

import "fmt"

// StreamWithCardOptions 是 ReplyStreamWithCard 的可选参数：卡片、流式反馈与卡片反馈。
type StreamWithCardOptions struct {
	MsgItem        []ReplyMsgItem
	StreamFeedback *ReplyFeedback
	TemplateCard   TemplateCard
	CardFeedback   *ReplyFeedback
}

// Reply 通用回复：把 body 作为回复内容发给 req 对应的消息/事件，cmd 为空时用
// WsCmdResponse。发完会等待服务端 ACK（5s 超时），同一 req_id 的回复串行执行。
func (c *Client) Reply(req RequestCarrier, body any, cmd string) (WsFrameRaw, error) {
	return c.replyByReqID(req.RequestID(), body, cmd)
}

// ReplyStream 一次性流式回复：自动生成 streamID，直接发出结束帧（finish=true）。
func (c *Client) ReplyStream(req RequestCarrier, content string) (WsFrameRaw, error) {
	streamID := GenerateReqID("stream")
	return c.ReplyStreamByID(req, streamID, content, true, nil, nil)
}

// ReplyStreamByID 完整流式回复控制：同一个 req 多次调用、复用同一个 streamID，
// 先 finish=false 发中间内容，结束帧用 finish=true。msgItem 仅在结束帧生效。
// feedback 非空时用于流式反馈。
func (c *Client) ReplyStreamByID(req RequestCarrier, streamID string, content string, finish bool, msgItem []ReplyMsgItem, feedback *ReplyFeedback) (WsFrameRaw, error) {
	payload := StreamPayload{
		ID:      streamID,
		Finish:  finish,
		Content: content,
	}
	if finish && len(msgItem) > 0 {
		payload.MsgItem = msgItem
	}
	if feedback != nil {
		payload.Feedback = feedback
	}
	body := StreamReplyBody{
		MsgType: "stream",
		Stream:  payload,
	}
	return c.Reply(req, body, WsCmdResponse)
}

// ReplyWelcomeText 回复进入会话的欢迎语（纯文本）。需配合 OnEnterChat 使用。
func (c *Client) ReplyWelcomeText(req RequestCarrier, content string) (WsFrameRaw, error) {
	body := WelcomeTextReplyBody{MsgType: "text"}
	body.Text.Content = content
	return c.Reply(req, body, WsCmdResponseWelcome)
}

// ReplyWelcomeTemplateCard 用模板卡片回复欢迎语。
func (c *Client) ReplyWelcomeTemplateCard(req RequestCarrier, card TemplateCard) (WsFrameRaw, error) {
	body := WelcomeTemplateCardReplyBody{
		MsgType:      "template_card",
		TemplateCard: card,
	}
	return c.Reply(req, body, WsCmdResponseWelcome)
}

// ReplyTemplateCard 回复模板卡片，feedback 非空时附带反馈信息（不会修改传入的 card）。
func (c *Client) ReplyTemplateCard(req RequestCarrier, card TemplateCard, feedback *ReplyFeedback) (WsFrameRaw, error) {
	if feedback != nil {
		card = cloneCard(card)
		card["feedback"] = feedback
	}
	body := TemplateCardReplyBody{
		MsgType:      "template_card",
		TemplateCard: card,
	}
	return c.Reply(req, body, WsCmdResponse)
}

// ReplyStreamWithCard 流式回复与模板卡片组合：opts.TemplateCard 非空时在同一帧里带卡片。
func (c *Client) ReplyStreamWithCard(req RequestCarrier, streamID string, content string, finish bool, opts *StreamWithCardOptions) (WsFrameRaw, error) {
	payload := StreamPayload{ID: streamID, Finish: finish, Content: content}
	if opts != nil {
		if finish && len(opts.MsgItem) > 0 {
			payload.MsgItem = opts.MsgItem
		}
		if opts.StreamFeedback != nil {
			payload.Feedback = opts.StreamFeedback
		}
	}

	body := StreamWithTemplateCardReplyBody{
		MsgType: "stream_with_template_card",
		Stream:  payload,
	}
	if opts != nil && opts.TemplateCard != nil {
		card := cloneCard(opts.TemplateCard)
		if opts.CardFeedback != nil {
			card["feedback"] = opts.CardFeedback
		}
		body.TemplateCard = card
	}
	return c.Reply(req, body, WsCmdResponse)
}

// UpdateTemplateCard 更新已发出的模板卡片，userIDs 为空表示更新所有可见用户。
func (c *Client) UpdateTemplateCard(req RequestCarrier, card TemplateCard, userIDs []string) (WsFrameRaw, error) {
	body := UpdateTemplateCardBody{
		ResponseType: "update_template_card",
		UserIDs:      userIDs,
		TemplateCard: card,
	}
	return c.Reply(req, body, WsCmdResponseUpdate)
}

// SendMarkdown 主动发送 Markdown 消息到指定会话（不依赖收到的消息）。
func (c *Client) SendMarkdown(chatID string, content string) (WsFrameRaw, error) {
	if chatID == "" {
		return WsFrameRaw{}, fmt.Errorf("chatID 不能为空")
	}
	body := SendMarkdownMsgBody{MsgType: "markdown"}
	body.Markdown.Content = content
	return c.sendMessage(chatID, body)
}

// SendTemplateCard 主动发送模板卡片到指定会话（不依赖收到的消息）。
func (c *Client) SendTemplateCard(chatID string, card TemplateCard) (WsFrameRaw, error) {
	if chatID == "" {
		return WsFrameRaw{}, fmt.Errorf("chatID 不能为空")
	}
	body := SendTemplateCardMsgBody{
		MsgType:      "template_card",
		TemplateCard: card,
	}
	return c.sendMessage(chatID, body)
}

func (c *Client) sendMessage(chatID string, body any) (WsFrameRaw, error) {
	reqID := GenerateReqID(WsCmdSendMsg)
	payload := map[string]any{
		"chatid": chatID,
		"msg":    body,
	}
	switch v := body.(type) {
	case SendMarkdownMsgBody:
		payload = map[string]any{
			"chatid":   chatID,
			"msgtype":  v.MsgType,
			"markdown": v.Markdown,
		}
	case SendTemplateCardMsgBody:
		payload = map[string]any{
			"chatid":        chatID,
			"msgtype":       v.MsgType,
			"template_card": v.TemplateCard,
		}
	}
	return c.replyByReqID(reqID, payload, WsCmdSendMsg)
}

// DownloadFile 下载消息里的文件/图片；aesKey 非空时用 AES-256-CBC 解密（IV 取 key 前
// 16 字节）并去除 PKCS#7 填充，为空时返回原始数据。单文件上限 100 MiB。
func (c *Client) DownloadFile(url string, aesKey string) (DownloadedFile, error) {
	downloaded, err := c.apiClient.downloadFileRaw(url)
	if err != nil {
		return DownloadedFile{}, err
	}
	if aesKey == "" {
		return downloaded, nil
	}
	buf, err := decryptFile(downloaded.Buffer, aesKey)
	if err != nil {
		return DownloadedFile{}, err
	}
	downloaded.Buffer = buf
	return downloaded, nil
}

func cloneCard(card TemplateCard) TemplateCard {
	cloned := make(TemplateCard, len(card))
	for k, v := range card {
		cloned[k] = v
	}
	return cloned
}
