package wecomaibot

import "fmt"

type StreamWithCardOptions struct {
	MsgItem        []ReplyMsgItem
	StreamFeedback *ReplyFeedback
	TemplateCard   TemplateCard
	CardFeedback   *ReplyFeedback
}

func (c *Client) Reply(req RequestCarrier, body any, cmd string) (WsFrameRaw, error) {
	return c.replyByReqID(req.RequestID(), body, cmd)
}

func (c *Client) ReplyStream(req RequestCarrier, content string) (WsFrameRaw, error) {
	streamID := GenerateReqID("stream")
	return c.ReplyStreamByID(req, streamID, content, true, nil, nil)
}

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

func (c *Client) ReplyWelcomeText(req RequestCarrier, content string) (WsFrameRaw, error) {
	body := WelcomeTextReplyBody{MsgType: "text"}
	body.Text.Content = content
	return c.Reply(req, body, WsCmdResponseWelcome)
}

func (c *Client) ReplyWelcomeTemplateCard(req RequestCarrier, card TemplateCard) (WsFrameRaw, error) {
	body := WelcomeTemplateCardReplyBody{
		MsgType:      "template_card",
		TemplateCard: card,
	}
	return c.Reply(req, body, WsCmdResponseWelcome)
}

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

func (c *Client) UpdateTemplateCard(req RequestCarrier, card TemplateCard, userIDs []string) (WsFrameRaw, error) {
	body := UpdateTemplateCardBody{
		ResponseType: "update_template_card",
		UserIDs:      userIDs,
		TemplateCard: card,
	}
	return c.Reply(req, body, WsCmdResponseUpdate)
}

func (c *Client) SendMarkdown(chatID string, content string) (WsFrameRaw, error) {
	if chatID == "" {
		return WsFrameRaw{}, fmt.Errorf("chatID 不能为空")
	}
	body := SendMarkdownMsgBody{MsgType: "markdown"}
	body.Markdown.Content = content
	return c.sendMessage(chatID, body)
}

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
