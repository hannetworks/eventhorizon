package distributed

import (
	"encoding/json"

	eh "github.com/looplab/eventhorizon"
)

type CommandParse interface {
	Encode(eh.Command) (string, error)
	Decode(string, eh.Command) (eh.Command, error)
}

type JsonCommandParse struct {
}

func (jcp *JsonCommandParse) Encode(command eh.Command) (string, error) {
	msg, err := json.Marshal(command)
	if err != nil {
		return "", err
	} else {
		return string(msg), nil
	}
}

func (jcp *JsonCommandParse) Decode(msg string, command eh.Command) (eh.Command, error) {
	err := json.Unmarshal([]byte(msg), &command)
	if err != nil {
		return nil, err
	} else {
		return command, nil
	}
}
