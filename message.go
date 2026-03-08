package wecomaibot

import "encoding/json"

func decodeTextMessage(body json.RawMessage, reqID string) (TextMessage, error) {
	var msg TextMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		return TextMessage{}, err
	}
	msg.ReqID = reqID
	return msg, nil
}

func decodeEventMessage(body json.RawMessage, reqID string) (EventMessage, error) {
	var msg EventMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		return EventMessage{}, err
	}
	msg.ReqID = reqID
	return msg, nil
}
