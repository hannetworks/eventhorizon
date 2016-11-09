package distributed

import (
	"strings"

	eh "github.com/looplab/eventhorizon"
)

type TopicStrategy interface {
	ParseTopic(string) eh.CommandType
}

type DefaultTopicStrategy struct {
}

func (dts *DefaultTopicStrategy) ParseTopic(topic string) eh.CommandType {
	if strings.Contains(topic, "/") {
		return eh.CommandType(strings.Split(topic, "/")[1])
	} else {
		return ""
	}

}
