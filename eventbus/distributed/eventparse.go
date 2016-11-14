package distributed

import (
	"encoding/json"

	eh "github.com/looplab/eventhorizon"
)

type EventParse interface {
	Encode(eh.Event) (string, error)
	Decode(string, eh.Event) (eh.Event, error)
}

type JsonEventParse struct {
}

func (jep *JsonEventParse) Encode(event eh.Event) (string, error) {
	msg, err := json.Marshal(event)
	if err != nil {
		return "", err
	} else {
		return string(msg), nil
	}
}

func (jep *JsonEventParse) Decode(msg string, event eh.Event) (eh.Event, error) {
	err := json.Unmarshal([]byte(msg), &event)
	if err != nil {
		return nil, err
	} else {
		return event, nil
	}
}
