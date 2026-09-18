package wecomaibot

func (w *wsConnection) sendAuth() error {
	body := map[string]any{
		"bot_id": w.cfg.BotID,
		"secret": w.cfg.Secret,
	}
	for k, v := range w.cfg.ExtraAuthParams {
		body[k] = v
	}
	frame := WsFrame{
		Cmd:     WsCmdSubscribe,
		Headers: WsHeaders{ReqID: GenerateReqID(WsCmdSubscribe)},
		Body:    body,
	}
	if err := w.sendRawFrame(frame); err != nil {
		return err
	}
	w.logger.Info("认证帧发送成功")
	return nil
}
