package distributed

import (
	"strings"

	eh "github.com/looplab/eventhorizon"
)

type EventTopicStrategy interface {
	ParseTopic(string) eh.EventType
}

type EventDefaultTopicStrategy struct {
}

func (dts *EventDefaultTopicStrategy) ParseTopic(topic string) eh.EventType {
	if strings.Contains(topic, "/") {
		return eh.EventType(strings.Split(topic, "/")[2])
	} else {
		return ""
	}

}
