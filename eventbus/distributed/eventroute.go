package distributed

import (
	"reflect"
	"strings"

	eh "github.com/looplab/eventhorizon"
)

type EventRoute interface {
	GetRoutingKey(eh.Event) EventRoutingKey
	GetTopicPattern() EventTopicPattern
}
type EventRoutingKey string

type EventTopicPattern string
type EventRoutingStrategy struct {
	domain string
}

// return package.structname
func (rs *EventRoutingStrategy) GetRoutingKey(event eh.Event) EventRoutingKey {
	return EventRoutingKey("event." + strings.Split(reflect.TypeOf(event).String(), "*")[1])
}

func (rs *EventRoutingStrategy) GetTopicPattern() EventTopicPattern {
	if rs.domain == "" {
		panic("the domain of RoutingStrategy must not be null")
	}
	return EventTopicPattern("event." + rs.domain + ".#")
}
